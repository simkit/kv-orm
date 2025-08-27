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

  private indexedFields: { [K in keyof z.infer<S>]?: "set" | "zset" };

  constructor(opts: KvOrmOptions<S>) {
    this.prefix = opts.prefix;
    this.kv = opts.kv;
    this.entitySchema = opts.schema;
    this.initialHooks = opts.hooks ?? {};
    this.indexedFields = opts.indexedFields ?? {};
  }

  private key(id: string): string {
    return `${this.prefix}:${id}`;
  }

  private setIndexKey<K extends keyof z.infer<S>>(
    field: K,
    value: z.infer<S>[K],
  ): string {
    return `${this.prefix}:idx:set:${String(field)}:${value}`;
  }

  private zsetIndexKey(field: keyof z.infer<S>): string {
    return `${this.prefix}:idx:zset:${String(field)}`;
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

  private updateIndexes(
    multi: ReturnType<Redis["multi"]>,
    entity: z.infer<S>,
    isUpdate: boolean = false,
    oldEntity?: z.infer<S>,
  ): void {
    for (const field in this.indexedFields) {
      const fieldKey = field as keyof z.infer<S>;
      const indexType = this.indexedFields[fieldKey];
      const value = entity[fieldKey];
      const oldValue = isUpdate ? oldEntity?.[fieldKey] : undefined;
      const id = entity.id as string;

      if (indexType === "set") {
        if (isUpdate && oldValue !== value) {
          if (oldValue !== undefined && oldValue !== null) {
            multi.srem(this.setIndexKey(fieldKey, oldValue), id);
          }
        }
        if (value !== undefined && value !== null) {
          multi.sadd(this.setIndexKey(fieldKey, value), id);
        }
      } else if (
        indexType === "zset" && (isUpdate ? oldValue !== value : true)
      ) {
        const score = (value instanceof Date)
          ? value.getTime()
          : value as number;
        if (
          score !== undefined && score !== null && typeof score === "number"
        ) {
          multi.zadd(this.zsetIndexKey(fieldKey), score, id);
        }
      }
    }
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

    // Start a Redis transaction to ensure atomicity
    const multi = this.kv.multi();
    multi.set(this.key(entity.id as string), JSON.stringify(entity));
    this.updateIndexes(multi, entity);
    await multi.exec();

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

    const multi = this.kv.multi();
    for (const entity of entities) {
      multi.set(this.key(entity.id as string), JSON.stringify(entity));
      this.updateIndexes(multi, entity);
    }
    await multi.exec();

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

  private async findWhereIndexed<
    K extends keyof z.infer<S>,
    V extends z.infer<S>[K],
  >(
    field: K,
    operator: OperatorFor<V>,
    value: V | V[],
  ): Promise<string[]> {
    const indexType = this.indexedFields[field];
    let ids: string[] = [];

    if (indexType === "set") {
      switch (operator) {
        case "eq":
          ids = await this.kv.smembers(
            this.setIndexKey(field, value as z.infer<S>[K]),
          );
          break;
        case "in":
          if (Array.isArray(value)) {
            const keys = value.map((val) =>
              this.setIndexKey(field, val as z.infer<S>[K])
            );
            const tempKey = `${this.prefix}:temp:${randomUUID()}`;
            const results = await this.kv.multi()
              .sunionstore(tempKey, ...keys)
              .expire(tempKey, 30)
              .exec();

            if (results && results[0] && results[0][1] !== null) {
              ids = await this.kv.smembers(tempKey);
            }
          }
          break;
        case "ne": {
          const allIds = await this.scanKeys().then((keys) =>
            keys.map((k) => k.replace(`${this.prefix}:`, ""))
          );
          const badIds = await this.kv.smembers(
            this.setIndexKey(field, value as z.infer<S>[K]),
          );
          ids = allIds.filter((id) => !badIds.includes(id));
          break;
        }
        case "nin": {
          const allIds = await this.scanKeys().then((keys) =>
            keys.map((k) => k.replace(`${this.prefix}:`, ""))
          );
          const badIds = Array.isArray(value)
            ? await this.kv.sunion(
              ...value.map((v) => this.setIndexKey(field, v as z.infer<S>[K])),
            )
            : [];
          ids = allIds.filter((id) => !badIds.includes(id));
          break;
        }
      }
    } else if (indexType === "zset") {
      const val = (value instanceof Date) ? value.getTime() : value as number;
      const start = "-inf";
      const end = "+inf";

      switch (operator) {
        case "lt":
          ids = await this.kv.zrangebyscore(
            this.zsetIndexKey(field),
            start,
            `(${val}`,
          );
          break;
        case "lte":
          ids = await this.kv.zrangebyscore(
            this.zsetIndexKey(field),
            start,
            `${val}`,
          );
          break;
        case "gt":
          ids = await this.kv.zrangebyscore(
            this.zsetIndexKey(field),
            `(${val}`,
            end,
          );
          break;
        case "gte":
          ids = await this.kv.zrangebyscore(
            this.zsetIndexKey(field),
            `${val}`,
            end,
          );
          break;
      }
    }

    return ids;
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

    let ids: string[] = [];
    const indexType = this.indexedFields[field];

    if (indexType) {
      const supportedBySet = ["eq", "ne", "in", "nin"].includes(operator);
      const supportedByZset = ["lt", "lte", "gt", "gte", "in"].includes(
        operator,
      );

      if (
        (indexType === "set" && supportedBySet) ||
        (indexType === "zset" && supportedByZset)
      ) {
        ids = await this.findWhereIndexed(field, operator, value);
      }
    }

    if (!ids.length && (!indexType || ["like"].includes(operator))) {
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

    const raws = ids.length
      ? await this.kv.mget(...ids.map((id) => this.key(id)))
      : [];
    const result = raws.filter(Boolean).map((v) =>
      this.entitySchema.parse(JSON.parse(v!))
    );

    await this.runHooks(hooksToRun, "after", input, result);
    return result;
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

    await this.kv.watch(this.key(id));
    const existing = await this.maybeGet(id);

    if (!existing) {
      await this.kv.unwatch();
      return null;
    }

    const updated = this.normalizeEntity({ ...existing, ...patch }, false);

    const multi = this.kv.multi();
    multi.set(this.key(updated.id as string), JSON.stringify(updated));
    this.updateIndexes(multi, updated, true, existing);

    const results = await multi.exec();

    if (results === null) {
      return null;
    }

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

    await this.kv.watch(this.key(id));
    const existing = await this.get(id);

    const updated = this.normalizeEntity({ ...existing, ...patch }, false);

    const multi = this.kv.multi();
    multi.set(this.key(updated.id as string), JSON.stringify(updated));
    this.updateIndexes(multi, updated, true, existing);

    const results = await multi.exec();

    if (results === null) {
      throw new Error(
        `Optimistic locking failed for key: ${
          this.key(id)
        }. Please retry the operation.`,
      );
    }

    await this.runHooks(hooksToRun, "after", input, updated);

    return updated;
  }

  private deleteIndexes(
    multi: ReturnType<Redis["multi"]>,
    entity: z.infer<S>,
  ): void {
    for (const field in this.indexedFields) {
      const fieldKey = field as keyof z.infer<S>;
      const indexType = this.indexedFields[fieldKey];
      const value = entity[fieldKey];
      const id = entity.id as string;

      if (indexType === "set") {
        if (value !== undefined && value !== null) {
          multi.srem(
            this.setIndexKey(fieldKey, value as z.infer<S>[keyof z.infer<S>]),
            id,
          );
        }
      } else if (indexType === "zset") {
        multi.zrem(this.zsetIndexKey(fieldKey), id);
      }
    }
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
    const entity = await this.maybeGet(id);
    if (!entity) return false;

    const multi = this.kv.multi();
    multi.del(this.key(id));
    this.deleteIndexes(multi, entity);
    const results = await multi.exec();

    return results !== null && results[0] !== null && results[0][1] === 1;
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
    if (!keys.length) {
      return 0;
    }

    const raws = await this.kv.mget(...keys);
    const entities = raws.filter(Boolean).map((v) =>
      this.entitySchema.parse(JSON.parse(v!))
    );

    const multi = this.kv.multi();
    multi.del(...keys);
    for (const entity of entities) {
      this.deleteIndexes(multi, entity);
    }
    const results = await multi.exec();

    if (results === null || !results[0] || results[0][1] === null) {
      return 0;
    }
    const deletedCount = results[0][1] as number;

    await this.runHooks(hooksToRun, "after", pattern, deletedCount);

    return deletedCount;
  }

  addHooks(hooks: KvOrmHooks<S>) {
    Object.assign(this.dynamicHooks, hooks);
  }
}
