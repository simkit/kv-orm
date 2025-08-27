import { type Redis } from "ioredis";
import { z, ZodObject, ZodRawShape } from "zod";
import { randomUUID } from "node:crypto";

import {
  Hooks,
  KvOrmHooks,
  KvOrmMethodOptions,
  KvOrmOptions,
  OperatorFor,
  RequiredZodFields,
} from "../types.ts";
import { NotFoundError } from "../errors.ts";

export class KvOrm<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  private prefix: string;
  private kv: Redis;
  public readonly entitySchema: S;

  private readonly initialHooks: KvOrmHooks<S>;
  private dynamicHooks: KvOrmHooks<S> = {};

  constructor(opts: KvOrmOptions<S>) {
    this.prefix = opts.prefix;
    this.kv = opts.kv;
    this.entitySchema = opts.schema;
    this.initialHooks = opts.hooks ?? {};
  }

  private key(id: string): string {
    return `${this.prefix}:${id}`;
  }

  private async runHooks<Input, Result>(
    hooks: Array<Hooks<Input, Result> | undefined>,
    phase: "before" | "after",
    input: Input,
    result?: Result,
  ): Promise<void> {
    for (const hook of hooks) {
      if (!hook) continue;
      if (phase === "before") {
        await hook.before?.({ input });
      } else {
        await hook.after?.({ input, result: result! });
      }
    }
  }

  private async scanKeys(pattern = "*", count = 100): Promise<string[]> {
    const keys: string[] = [];
    let cursor = "0";
    do {
      const [nextCursor, batch] = await this.kv.scan(
        cursor,
        "MATCH",
        `${this.prefix}:${pattern}`,
        "COUNT",
        count,
      );
      cursor = nextCursor;
      keys.push(...batch);
    } while (cursor !== "0");
    return keys;
  }

  private normalizeEntity(
    data: z.input<S> | z.infer<S>,
    isNew = true,
  ): z.infer<S> {
    let id: string;
    if ("id" in data && data.id) {
      if (!z.uuid().safeParse(data.id).success) {
        throw new Error(`Invalid UUID provided for id: ${data.id}`);
      }
      id = data.id as string;
    } else {
      id = randomUUID();
    }

    const now = new Date().toISOString();

    return this.entitySchema.parse({
      ...data,
      id,
      createdAt: isNew ? now : data.createdAt ?? now,
      updatedAt: now,
    });
  }

  async create(
    data: z.input<S>,
    options?: KvOrmMethodOptions<z.input<S>, z.infer<S>>,
  ): Promise<z.infer<S>> {
    const hooksToRun = [
      this.initialHooks.create,
      options?.hooks,
      this.dynamicHooks.create,
    ];
    await this.runHooks(hooksToRun, "before", data);

    const entity = this.normalizeEntity(data);
    await this.kv.set(this.key(entity.id as string), JSON.stringify(entity));

    await this.runHooks(hooksToRun, "after", data, entity);

    return entity;
  }

  async createBulk(
    data: z.input<S>[],
    options?: KvOrmMethodOptions<z.input<S>[], z.infer<S>[]>,
  ): Promise<z.infer<S>[]> {
    const hooksToRun = [
      this.initialHooks.createBulk,
      options?.hooks,
      this.dynamicHooks.createBulk,
    ];
    await this.runHooks(hooksToRun, "before", data);

    const entities = data.map((d) => this.normalizeEntity(d));
    const kvPairs = entities.flatMap((entity) => [
      this.key(entity.id as string),
      JSON.stringify(entity),
    ]);

    if (kvPairs.length > 0) {
      await this.kv.mset(...kvPairs);
    }

    await this.runHooks(hooksToRun, "after", data, entities);

    return entities;
  }

  async get(
    id: string,
    options?: KvOrmMethodOptions<string, z.infer<S>>,
  ): Promise<z.infer<S>> {
    const hooksToRun = [
      this.initialHooks.get,
      options?.hooks,
      this.dynamicHooks.get,
    ];

    await this.runHooks(hooksToRun, "before", id);

    const entity = await this.maybeGet(id);
    if (!entity) throw new NotFoundError(this.prefix, id);

    await this.runHooks(hooksToRun, "after", id, entity);

    return entity;
  }

  async maybeGet(
    id: string,
    options?: KvOrmMethodOptions<string, z.infer<S> | null>,
  ): Promise<z.infer<S> | null> {
    const hooksToRun = [
      this.initialHooks.maybeGet,
      options?.hooks,
      this.dynamicHooks.maybeGet,
    ];

    await this.runHooks(hooksToRun, "before", id);

    const raw = await this.kv.get(this.key(id));
    const entity = raw ? this.entitySchema.parse(JSON.parse(raw)) : null;

    await this.runHooks(hooksToRun, "after", id, entity);

    return entity;
  }

  async getAll(
    pattern = "*",
    options?: KvOrmMethodOptions<string, z.infer<S>[]>,
  ): Promise<z.infer<S>[]> {
    const hooksToRun = [
      this.initialHooks.getAll,
      options?.hooks,
      this.dynamicHooks.getAll,
    ];

    await this.runHooks(hooksToRun, "before", pattern);

    const keys = await this.scanKeys(pattern);
    const raws = keys.length ? await this.kv.mget(...keys) : [];

    const result = raws.filter(Boolean).map((v) =>
      this.entitySchema.parse(JSON.parse(v!))
    );

    await this.runHooks(hooksToRun, "after", pattern, result);

    return result;
  }

  async findWhere<
    K extends keyof z.infer<S>,
    V extends z.infer<S>[K],
  >(
    field: K,
    operator: OperatorFor<V>,
    value: V | V[],
    options?: KvOrmMethodOptions<
      { field: K; operator: OperatorFor<V>; value: V | V[] },
      z.infer<S>[]
    >,
  ): Promise<z.infer<S>[]> {
    const initialHook = this.initialHooks.findWhere<K, V>?.();
    const dynamicHook = this.dynamicHooks.findWhere<K, V>?.();

    const hooksToRun = [
      initialHook,
      options?.hooks,
      dynamicHook,
    ];

    const input = { field, operator, value };
    await this.runHooks(hooksToRun, "before", input);

    const allEntities = await this.getAll("*");

    const results = allEntities.filter((entity) => {
      const fieldValue = entity[field] as V;

      switch (operator) {
        case "eq":
          return fieldValue === value;
        case "ne":
          return fieldValue !== value;
        case "lt":
          if (
            typeof fieldValue === "string" && typeof value === "string" ||
            typeof fieldValue === "number" && typeof value === "number" ||
            fieldValue instanceof Date && value instanceof Date
          ) {
            return fieldValue < value;
          }
          return false;
        case "lte":
          if (
            typeof fieldValue === "string" && typeof value === "string" ||
            typeof fieldValue === "number" && typeof value === "number" ||
            fieldValue instanceof Date && value instanceof Date
          ) {
            return fieldValue <= value;
          }
          return false;
        case "gt":
          if (
            typeof fieldValue === "string" && typeof value === "string" ||
            typeof fieldValue === "number" && typeof value === "number" ||
            fieldValue instanceof Date && value instanceof Date
          ) {
            return fieldValue > value;
          }
          return false;
        case "gte":
          if (
            typeof fieldValue === "string" && typeof value === "string" ||
            typeof fieldValue === "number" && typeof value === "number" ||
            fieldValue instanceof Date && value instanceof Date
          ) {
            return fieldValue >= value;
          }
          return false;
        case "like":
          return (
            typeof fieldValue === "string" &&
            typeof value === "string" &&
            fieldValue.toLowerCase().includes(value.toLowerCase())
          );
        case "in":
          return Array.isArray(value) && (value as V[]).includes(fieldValue);
        case "nin":
          return Array.isArray(value) && !(value as V[]).includes(fieldValue);
      }
    });

    await this.runHooks(hooksToRun, "after", input, results);

    return results;
  }

  async update(
    id: string,
    patch: Partial<z.input<S>>,
    options?: KvOrmMethodOptions<
      { id: string; patch: Partial<z.input<S>> },
      z.infer<S> | null
    >,
  ): Promise<z.infer<S> | null> {
    const hooksToRun = [
      this.initialHooks.update,
      options?.hooks,
      this.dynamicHooks.update,
    ];
    const input = { id, patch };

    await this.runHooks(hooksToRun, "before", input);

    const existing = await this.maybeGet(id);
    if (!existing) return null;

    const updated = this.normalizeEntity({ ...existing, ...patch }, false);

    await this.kv.set(this.key(updated.id as string), JSON.stringify(updated));

    await this.runHooks(hooksToRun, "after", input, updated);

    return updated;
  }

  async updateOrFail(
    id: string,
    patch: Partial<z.input<S>>,
    options?: KvOrmMethodOptions<
      { id: string; patch: Partial<z.input<S>> },
      z.infer<S>
    >,
  ): Promise<z.infer<S>> {
    const hooksToRun = [
      this.initialHooks.updateOrFail,
      options?.hooks,
      this.dynamicHooks.updateOrFail,
    ];
    const input = { id, patch };

    await this.runHooks(hooksToRun, "before", input);

    const existing = await this.get(id);

    const updated = this.normalizeEntity({ ...existing, ...patch }, false);

    await this.kv.set(this.key(updated.id as string), JSON.stringify(updated));

    await this.runHooks(hooksToRun, "after", input, updated);

    return updated;
  }

  async delete(
    id: string,
    options?: KvOrmMethodOptions<string, boolean>,
  ): Promise<boolean> {
    const hooksToRun = [
      this.initialHooks.delete,
      options?.hooks,
      this.dynamicHooks.delete,
    ];

    await this.runHooks(hooksToRun, "before", id);

    const result = (await this.kv.del(this.key(id))) === 1;

    await this.runHooks(hooksToRun, "after", id, result);

    return result;
  }

  async deleteAll(
    pattern = "*",
    options?: KvOrmMethodOptions<string, number>,
  ): Promise<number> {
    const hooksToRun = [
      this.initialHooks.deleteAll,
      options?.hooks,
      this.dynamicHooks.deleteAll,
    ];

    await this.runHooks(hooksToRun, "before", pattern);

    const keys = await this.scanKeys(pattern);
    const result = keys.length ? await this.kv.del(...keys) : 0;

    await this.runHooks(hooksToRun, "after", pattern, result);

    return result;
  }

  addHooks(hooks: KvOrmHooks<S>) {
    Object.assign(this.dynamicHooks, hooks);
  }
}
