import z, { ZodObject, ZodRawShape } from "zod";
import type { Redis } from "ioredis";

// Required fields for entities
export type RequiredZodFields = {
  id: z.ZodDefault<z.ZodUUID>;

  updatedAt: z.ZodDefault<z.ZodCoercedDate>;
  createdAt: z.ZodDefault<z.ZodCoercedDate>;
};

// Operator Types
export type OperatorFor<T> = T extends number | Date
  ? "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "in" | "nin"
  : T extends string ? "eq" | "ne" | "like" | "in" | "nin"
  : "eq" | "ne" | "in" | "nin";

// ORM Options for constructor
export interface KvOrmOptions<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  prefix: string;
  kv: Redis;
  schema: S;
  hooks?: KvOrmHooks<S>;
  indexedFields?: { [K in keyof z.infer<S>]?: "set" | "zset" };
}

// ORM Options for individual methods
export interface KvOrmMethodOptions<Input, Result> {
  hooks?: Hooks<Input, Result>;
}

// Hook args + types
export type BeforeHookArgs<Input> = { input: Input };
export type AfterHookArgs<Input, Result> = { input: Input; result: Result };

export type Hooks<Input, Result> = {
  before?: (args: BeforeHookArgs<Input>) => Promise<void> | void;
  after?: (args: AfterHookArgs<Input, Result>) => Promise<void> | void;
};

// FindWhere input
export type FindWhereInput<
  S,
  K extends keyof z.infer<S>,
  V extends z.infer<S>[K],
> = {
  field: K;
  operator: OperatorFor<V>;
  value: V | V[];
};

// ORM Hooks
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

// ORM Context
export interface KvOrmContext<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  prefix: string;
  kv: Redis;
  entitySchema: S;
  initialHooks: KvOrmHooks<S>;
  dynamicHooks: KvOrmHooks<S>;
  indexedFields: { [K in keyof z.infer<S>]?: "set" | "zset" };

  runHooks<Input, Result>(
    hooks: Array<KvOrmMethodOptions<Input, Result>["hooks"] | undefined>,
    phase: "before" | "after",
    input: Input,
    result?: Result,
  ): Promise<void>;

  key(id: string): string;
  setIndexKey<K extends keyof z.infer<S>>(
    field: K,
    value: z.infer<S>[K],
  ): string;
  zsetIndexKey(field: keyof z.infer<S>): string;
  scanKeys(pattern?: string, count?: number): Promise<string[]>;
  updateIndexes(
    multi: ReturnType<Redis["multi"]>,
    entity: z.infer<S>,
    isUpdate?: boolean,
    oldEntity?: z.infer<S>,
  ): void;
  deleteIndexes(multi: ReturnType<Redis["multi"]>, entity: z.infer<S>): void;
  normalizeEntity(data: z.input<S> | z.infer<S>, isNew?: boolean): z.infer<S>;
  findWhereIndexed<K extends keyof z.infer<S>, V extends z.infer<S>[K]>(
    field: K,
    operator: OperatorFor<V>,
    value: V | V[],
  ): Promise<string[]>;
}
