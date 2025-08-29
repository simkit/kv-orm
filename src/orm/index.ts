import type { Redis } from "ioredis";
import { z, type ZodObject, type ZodRawShape } from "zod";
import { randomUUID } from "node:crypto";

import type {
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

  public async rebuildIndexes(): Promise<void> {
    const ids = await this.kv.smembers(this.allKey());
    const multi = this.kv.multi();

    for (const field in this.indexedFields) {
      const indexType = this.indexedFields[field as keyof z.infer<S>];
      if (indexType === "set") {
        const keys = await this.kv.keys(`${this.prefix}:idx:set:${field}:*`);
        if (keys.length) multi.del(...keys);
      } else if (indexType === "zset") {
        multi.del(this.zsetIndexKey(field as keyof z.infer<S>));
      }
    }

    for (const id of ids) {
      const raw = await this.kv.get(this.key(id));
      if (!raw) continue;
      const entity = this.entitySchema.parse(JSON.parse(raw));
      this.updateIndexes(multi, entity, false);
    }

    await multi.exec();
  }

  private key(id: string): string {
    return `${this.prefix}:${id}`;
  }

  private allKey(): string {
    return `${this.prefix}:all`;
  }

  private normalizeIndexValue(value: unknown): string {
    if (value instanceof Date) return value.toISOString();
    if (typeof value === "number" || typeof value === "boolean") {
      return String(value);
    }
    return String(value);
  }

  private setIndexKey<K extends keyof z.infer<S>>(
    field: K,
    value: z.infer<S>[K],
  ): string {
    return `${this.prefix}:idx:set:${String(field)}:${
      this.normalizeIndexValue(value)
    }`;
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
    multi.sadd(this.allKey(), entity.id as string);

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
      } else if (indexType === "zset") {
        if (typeof value === "string") {
          multi.zadd(this.zsetIndexKey(fieldKey), 0, `${value}||${id}`);
        } else {
          const score = value instanceof Date ? value.getTime() : Number(value);
          if (!isNaN(score)) {
            multi.zadd(this.zsetIndexKey(fieldKey), score, id);
          }
        }
      }
    }
  }

  private deleteIndexes(
    multi: ReturnType<Redis["multi"]>,
    entity: z.infer<S>,
  ): void {
    multi.srem(this.allKey(), entity.id as string);

    for (const field in this.indexedFields) {
      const fieldKey = field as keyof z.infer<S>;
      const indexType = this.indexedFields[fieldKey];
      const value = entity[fieldKey];
      const id = entity.id as string;

      if (indexType === "set") {
        if (value !== undefined && value !== null) {
          multi.srem(this.setIndexKey(fieldKey, value), id);
        }
      } else if (indexType === "zset") {
        if (typeof value === "string") {
          multi.zrem(this.zsetIndexKey(fieldKey), `${value}||${id}`);
        } else {
          multi.zrem(this.zsetIndexKey(fieldKey), id);
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

    const now = new Date();

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
            ids = await this.kv.sunion(...keys);
          }
          break;
        case "ne":
          ids = await this.kv.sdiff(
            this.allKey(),
            this.setIndexKey(field, value as z.infer<S>[K]),
          );
          break;
        case "nin":
          if (Array.isArray(value) && value.length > 0) {
            const badKeys = value.map((v) =>
              this.setIndexKey(field, v as z.infer<S>[K])
            );
            ids = await this.kv.sdiff(this.allKey(), ...badKeys);
          } else {
            ids = await this.kv.smembers(this.allKey());
          }
          break;
      }
    } else if (indexType === "zset") {
      if (typeof value === "string") {
        switch (operator) {
          case "lt":
            ids = await this.kv.zrangebylex(
              this.zsetIndexKey(field),
              "-",
              `(${value}`,
            );
            break;
          case "lte":
            ids = await this.kv.zrangebylex(
              this.zsetIndexKey(field),
              "-",
              `[${value}`,
            );
            break;
          case "gt":
            ids = await this.kv.zrangebylex(
              this.zsetIndexKey(field),
              `(${value}`,
              "+",
            );
            break;
          case "gte":
            ids = await this.kv.zrangebylex(
              this.zsetIndexKey(field),
              `[${value}`,
              "+",
            );
            break;
        }
        ids = ids.map((x) => x.split("||")[1]);
      } else {
        const val = value instanceof Date ? value.getTime() : Number(value);
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
    }

    return ids;
  }
}
