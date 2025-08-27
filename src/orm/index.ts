import { type Redis } from "ioredis";
import { z, ZodObject, ZodRawShape } from "zod";
import { HookArgs, Hooks, KvOrmHooks } from "../types.ts";
import { NotFoundError } from "../errors.ts";

// Required fields
export type RequiredZodFields = {
  id: z.ZodDefault<z.ZodUUID>;

  updatedAt: z.ZodDefault<z.ZodISODateTime>;
  createdAt: z.ZodDefault<z.ZodISODateTime>;
};

// ORM options
export interface KvOrmOptions<
  S extends ZodObject<ZodRawShape> & {
    shape: RequiredZodFields;
  },
> {
  prefix: string;
  kv: Redis;
  schema: S;
}

// Redis ORM
export class KvOrm<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  private prefix: string;
  private kv: Redis;
  public readonly entitySchema: S;
  private hooks: KvOrmHooks<S> = {};

  constructor(opts: KvOrmOptions<S>, hooks?: KvOrmHooks<S>) {
    this.prefix = opts.prefix;
    this.kv = opts.kv;
    this.entitySchema = opts.schema;
    if (hooks) this.hooks = hooks;
  }

  private key(id: string) {
    return `${this.prefix}:${id}`;
  }

  private async runHook<Input, Result>(
    hook: Hooks<Input, Result> | undefined,
    phase: "before" | "after",
    args: HookArgs<Input, Result>,
  ) {
    if (!hook) return;
    await (phase === "before" ? hook.before?.(args) : hook.after?.(args));
  }

  async get(id: string): Promise<z.infer<S>> {
    await this.runHook(this.hooks.get, "before", { input: id });

    const entity = await this.maybeGet(id);
    if (!entity) throw new NotFoundError(this.prefix, id);

    await this.runHook(this.hooks.get, "after", { input: id, result: entity });
    return entity;
  }

  async maybeGet(id: string): Promise<z.infer<S> | null> {
    await this.runHook(this.hooks.maybeGet, "before", { input: id });

    const raw = await this.kv.get(this.key(id));
    const entity = raw ? this.entitySchema.parse(JSON.parse(raw)) : null;

    await this.runHook(this.hooks.maybeGet, "after", {
      input: id,
      result: entity,
    });
    return entity;
  }

  async create(data: z.input<S>): Promise<z.infer<S>> {
    await this.runHook(this.hooks.create, "before", { input: data });

    const parsed = this.entitySchema.parse(data);
    await this.kv.set(this.key(parsed.id as string), JSON.stringify(parsed));

    await this.runHook(this.hooks.create, "after", {
      input: data,
      result: parsed,
    });
    return parsed;
  }

  async getAll(pattern = "*"): Promise<z.infer<S>[]> {
    await this.runHook(this.hooks.getAll, "before", { input: pattern });

    const keys = await this.kv.keys(`${this.prefix}:${pattern}`);
    const raws = keys.length > 0 ? await this.kv.mget(...keys) : [];
    const result = raws.filter(Boolean).map((v) =>
      this.entitySchema.parse(JSON.parse(v!))
    );

    await this.runHook(this.hooks.getAll, "after", { input: pattern, result });
    return result;
  }

  async update(
    id: string,
    patch: Partial<z.input<S>>,
  ): Promise<z.infer<S> | null> {
    await this.runHook(this.hooks.update, "before", { input: { id, patch } });

    const existing = await this.maybeGet(id);
    if (!existing) return null;

    const updated = this.entitySchema.parse({
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    });
    await this.kv.set(this.key(updated.id as string), JSON.stringify(updated));

    await this.runHook(this.hooks.update, "after", {
      input: { id, patch },
      result: updated,
    });
    return updated;
  }

  async updateOrFail(
    id: string,
    patch: Partial<z.input<S>>,
  ): Promise<z.infer<S>> {
    await this.runHook(this.hooks.updateOrFail, "before", {
      input: { id, patch },
    });

    const existing = await this.get(id);
    const updated = this.entitySchema.parse({
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    });
    await this.kv.set(this.key(updated.id as string), JSON.stringify(updated));

    await this.runHook(this.hooks.updateOrFail, "after", {
      input: { id, patch },
      result: updated,
    });
    return updated;
  }

  async delete(id: string): Promise<boolean> {
    await this.runHook(this.hooks.delete, "before", { input: id });

    const result = (await this.kv.del(this.key(id))) === 1;

    await this.runHook(this.hooks.delete, "after", { input: id, result });
    return result;
  }

  async deleteAll(pattern = "*"): Promise<number> {
    await this.runHook(this.hooks.deleteAll, "before", { input: pattern });

    const keys = await this.kv.keys(`${this.prefix}:${pattern}`);
    const result = keys.length > 0 ? await this.kv.del(...keys) : 0;

    await this.runHook(this.hooks.deleteAll, "after", {
      input: pattern,
      result,
    });
    return result;
  }

  addHooks(hooks: KvOrmHooks<S>) {
    Object.assign(this.hooks, hooks);
  }
}
