import { type Redis } from "ioredis";
import { z, ZodObject, ZodRawShape } from "zod";
import { randomUUID } from "node:crypto";

import {
  KvOrmContext,
  KvOrmHooks,
  KvOrmMethodOptions,
  KvOrmOptions,
  OperatorFor,
  RequiredZodFields,
} from "../types.ts";

import { create, createBulk } from "./methods/create.ts";
import { get, getAll, maybeGet } from "./methods/get.ts";
import { update, updateOrFail } from "./methods/update.ts";
import { deleteAll, deleteEntity } from "./methods/delete.ts";
import { findWhere } from "./methods/find.ts";

export class KvOrm<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  public prefix: string;
  public kv: Redis;
  public readonly entitySchema: S;
  public readonly initialHooks: KvOrmHooks<S>;
  public dynamicHooks: KvOrmHooks<S> = {};

  public indexedFields: { [K in keyof z.infer<S>]?: "set" | "zset" };

  private readonly context: KvOrmContext<S>;

  constructor(opts: KvOrmOptions<S>) {
    this.prefix = opts.prefix;
    this.kv = opts.kv;
    this.entitySchema = opts.schema;
    this.initialHooks = opts.hooks ?? {};
    this.indexedFields = opts.indexedFields ?? {};

    // for internal methods
    this.context = {
      prefix: this.prefix,
      kv: this.kv,
      entitySchema: this.entitySchema,
      initialHooks: this.initialHooks,
      dynamicHooks: this.dynamicHooks,
      indexedFields: this.indexedFields,
      runHooks: this.runHooks.bind(this),
      key: this.key.bind(this),
      setIndexKey: this.setIndexKey.bind(this),
      zsetIndexKey: this.zsetIndexKey.bind(this),
      scanKeys: this.scanKeys.bind(this),
      updateIndexes: this.updateIndexes.bind(this),
      deleteIndexes: this.deleteIndexes.bind(this),
      normalizeEntity: this.normalizeEntity.bind(this),
      findWhereIndexed: this.findWhereIndexed.bind(this),
    };
  }

  public create = (
    data: z.input<S>,
    options?: KvOrmMethodOptions<z.input<S>, z.infer<S>>,
  ) => create(this.context, data, options);

  public createBulk = (
    data: z.input<S>[],
    options?: KvOrmMethodOptions<z.input<S>[], z.infer<S>[]>,
  ) => createBulk(this.context, data, options);

  public get = (
    id: string,
    options?: KvOrmMethodOptions<string, z.infer<S>>,
  ) => get(this.context, id, options);

  public maybeGet = (
    id: string,
    options?: KvOrmMethodOptions<string, z.infer<S> | null>,
  ) => maybeGet(this.context, id, options);

  public getAll = (
    pattern = "*",
    options?: KvOrmMethodOptions<string, z.infer<S>[]>,
  ) => getAll(this.context, pattern, options);

  public findWhere = <
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
  ) => findWhere(this.context, field, operator, value, options);

  public update = (
    id: string,
    patch: Partial<z.input<S>>,
    options?: KvOrmMethodOptions<
      { id: string; patch: Partial<z.input<S>> },
      z.infer<S> | null
    >,
  ) => update(this.context, id, patch, options);

  public updateOrFail = (
    id: string,
    patch: Partial<z.input<S>>,
    options?: KvOrmMethodOptions<
      { id: string; patch: Partial<z.input<S>> },
      z.infer<S>
    >,
  ) => updateOrFail(this.context, id, patch, options);

  public delete = (
    id: string,
    options?: KvOrmMethodOptions<string, boolean>,
  ) => deleteEntity(this.context, id, options);

  public deleteAll = (
    pattern = "*",
    options?: KvOrmMethodOptions<string, number>,
  ) => deleteAll(this.context, pattern, options);

  public addHooks(hooks: KvOrmHooks<S>) {
    Object.assign(this.dynamicHooks, hooks);
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
    hooks: Array<KvOrmMethodOptions<Input, Result>["hooks"] | undefined>,
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
}
