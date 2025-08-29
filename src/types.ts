import type { z, ZodObject, ZodRawShape } from "zod";
import type { Redis } from "ioredis";
import type { baseFields } from "./schemas.ts";

/** Required base fields for all entities */
export type RequiredZodFields = typeof baseFields;

/**
 * Operators available for a field type.
 *
 * - Numbers and Dates: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `in`, `nin`
 * - Strings: `eq`, `ne`, `like`, `in`, `nin`
 * - Other types: `eq`, `ne`, `in`, `nin`
 */
export type OperatorFor<T> = T extends number | Date
  ? "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "in" | "nin"
  : T extends string ? "eq" | "ne" | "like" | "in" | "nin"
  : "eq" | "ne" | "in" | "nin";

/** Options for constructing a KvOrm instance */
export interface KvOrmOptions<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  /** Redis key prefix for this entity type */
  prefix: string;

  /** Redis client instance */
  kv: Redis;

  /** Zod schema used for entities */
  schema: S;

  /** Optional global hooks */
  hooks?: KvOrmHooks<S>;

  /** Optional secondary index configuration */
  indexedFields?: { [K in keyof z.infer<S>]?: "set" | "zset" };
}

/** Options for individual KvOrm methods */
export interface KvOrmMethodOptions<Input, Result> {
  /** Optional hooks specific to this method call */
  hooks?: Hooks<Input, Result>;
}

/** Arguments passed to a before hook */
export type BeforeHookArgs<Input> = { input: Input };

/** Arguments passed to an after hook */
export type AfterHookArgs<Input, Result> = { input: Input; result: Result };

/** Hook definition */
export type Hooks<Input, Result> = {
  /** Called before the operation */
  before?: (args: BeforeHookArgs<Input>) => Promise<void> | void;

  /** Called after the operation */
  after?: (args: AfterHookArgs<Input, Result>) => Promise<void> | void;
};

/** Input type for findWhere queries */
export type FindWhereInput<
  S,
  K extends keyof z.infer<S>,
  V extends z.infer<S>[K],
> = {
  /** Field to query */
  field: K;

  /** Operator to use for comparison */
  operator: OperatorFor<V>;

  /** Value(s) to compare */
  value: V | V[];
};

/** Collection of hooks for a KvOrm instance */
export type KvOrmHooks<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> = {
  get?: Hooks<string, z.infer<S>>;
  maybeGet?: Hooks<string, z.infer<S> | null>;
  getAll?: Hooks<string, z.infer<S>[]>;
  findWhere?: <
    K extends keyof z.infer<S>,
    V extends z.infer<S>[K],
  >() => Hooks<FindWhereInput<S, K, V>, z.infer<S>[]>;
  create?: Hooks<z.input<S>, z.infer<S>>;
  createBulk?: Hooks<z.input<S>[], z.infer<S>[]>;
  update?: Hooks<{ id: string; patch: Partial<z.input<S>> }, z.infer<S> | null>;
  updateOrFail?: Hooks<{ id: string; patch: Partial<z.input<S>> }, z.infer<S>>;
  delete?: Hooks<string, boolean>;
  deleteAll?: Hooks<string, number>;
};

/** Internal context object passed to KvOrm methods */
export interface KvOrmContext<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  /** Redis key prefix */
  prefix: string;

  /** Redis client */
  kv: Redis;

  /** Entity validation schema */
  entitySchema: S;

  /** Global hooks registered at construction */
  initialHooks: KvOrmHooks<S>;

  /** Dynamically added runtime hooks */
  dynamicHooks: KvOrmHooks<S>;

  /** Secondary index configuration */
  indexedFields: { [K in keyof z.infer<S>]?: "set" | "zset" };

  /** Run hooks for a specific phase (`before` or `after`) */
  runHooks<Input, Result>(
    hooks: Array<KvOrmMethodOptions<Input, Result>["hooks"] | undefined>,
    phase: "before" | "after",
    input: Input,
    result?: Result,
  ): Promise<void>;

  /** Generate the Redis key for a given ID */
  key(id: string): string;

  /** Generate the Redis set index key for a field/value pair */
  setIndexKey<K extends keyof z.infer<S>>(
    field: K,
    value: z.infer<S>[K],
  ): string;

  /** Generate the Redis sorted set index key for a field */
  zsetIndexKey(field: keyof z.infer<S>): string;

  /** Scan Redis keys matching a pattern */
  scanKeys(pattern?: string, count?: number): Promise<string[]>;

  /** Update secondary indexes for an entity */
  updateIndexes(
    multi: ReturnType<Redis["multi"]>,
    entity: z.infer<S>,
    isUpdate?: boolean,
    oldEntity?: z.infer<S>,
  ): void;

  /** Delete secondary indexes for an entity */
  deleteIndexes(multi: ReturnType<Redis["multi"]>, entity: z.infer<S>): void;

  /** Normalize an entity for insertion/update */
  normalizeEntity(data: z.input<S> | z.infer<S>, isNew?: boolean): z.infer<S>;

  /** Find entity IDs using secondary indexes */
  findWhereIndexed<K extends keyof z.infer<S>, V extends z.infer<S>[K]>(
    field: K,
    operator: OperatorFor<V>,
    value: V | V[],
  ): Promise<string[]>;
}
