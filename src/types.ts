import z, { ZodObject, ZodRawShape } from "zod";

export type HookArgs<Input, Result> = {
  input: Input;
  result?: Result;
};

export type Hooks<Input, Result> = {
  before?: (args: HookArgs<Input, Result>) => Promise<void> | void;
  after?: (args: HookArgs<Input, Result>) => Promise<void> | void;
};

export type KvOrmHooks<S extends ZodObject<ZodRawShape>> = {
  get?: Hooks<string, z.infer<S>>;
  maybeGet?: Hooks<string, z.infer<S> | null>;
  getAll?: Hooks<string, z.infer<S>[]>;
  create?: Hooks<z.input<S>, z.infer<S>>;
  update?: Hooks<{ id: string; patch: Partial<z.input<S>> }, z.infer<S> | null>;
  updateOrFail?: Hooks<{ id: string; patch: Partial<z.input<S>> }, z.infer<S>>;
  delete?: Hooks<string, boolean>;
  deleteAll?: Hooks<string, number>;
};
