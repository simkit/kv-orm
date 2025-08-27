import z, { ZodObject, ZodRawShape } from "zod";
import { type Redis } from "ioredis";

// Required fields for entities
export type RequiredZodFields = {
  id: z.ZodDefault<z.ZodUUID>;

  updatedAt: z.ZodDefault<z.ZodISODateTime>;
  createdAt: z.ZodDefault<z.ZodISODateTime>;
};

// ORM options for the constructor
export interface KvOrmOptions<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  prefix: string;
  kv: Redis;
  schema: S;
  hooks?: KvOrmHooks<S>;
}

// ORM options for individual methods. Now generic.
export interface KvOrmMethodOptions<Input, Result> {
  hooks?: Hooks<Input, Result>;
}

// Hook Args
export type BeforeHookArgs<Input> = { input: Input };
export type AfterHookArgs<Input, Result> = { input: Input; result: Result };

// Hooks type
export type Hooks<Input, Result> = {
  before?: (args: BeforeHookArgs<Input>) => Promise<void> | void;
  after?: (args: AfterHookArgs<Input, Result>) => Promise<void> | void;
};

// ORM Hooks
export type KvOrmHooks<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> = {
  get?: Hooks<string, z.infer<S>>;
  maybeGet?: Hooks<string, z.infer<S> | null>;
  getAll?: Hooks<string, z.infer<S>[]>;
  create?: Hooks<z.input<S>, z.infer<S>>;
  update?: Hooks<{ id: string; patch: Partial<z.input<S>> }, z.infer<S> | null>;
  updateOrFail?: Hooks<{ id: string; patch: Partial<z.input<S>> }, z.infer<S>>;
  delete?: Hooks<string, boolean>;
  deleteAll?: Hooks<string, number>;
};
