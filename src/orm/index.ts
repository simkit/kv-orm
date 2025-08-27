import { type Redis } from "ioredis";
import { z, ZodObject, ZodRawShape } from "zod";
import {
  Hooks,
  KvOrmHooks,
  KvOrmOptions,
  RequiredZodFields,
} from "../types.ts";
import { NotFoundError } from "../errors.ts";

// Redis ORM
export class KvOrm<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
> {
  private prefix: string;
  private kv: Redis;
  public readonly entitySchema: S;
  private hooks: KvOrmHooks<S>;

  constructor(opts: KvOrmOptions<S>) {
    this.prefix = opts.prefix;
    this.kv = opts.kv;
    this.entitySchema = opts.schema;
    this.hooks = opts.hooks ?? {};
  }

  private key(id: string) {
    return `${this.prefix}:${id}`;
  }

  private async runHook<Input, Result>(
    hook: Hooks<Input, Result> | undefined,
    phase: "before" | "after",
    input: Input,
    result?: Result,
  ) {
    if (!hook) return;

    if (phase === "before") {
      await hook.before?.({ input });
    } else {
      await hook.after?.({ input, result: result! });
    }
  }

  async get(id: string): Promise<z.infer<S>> {
    await this.runHook(this.hooks.get, "before", id);

    const entity = await this.maybeGet(id);
    if (!entity) throw new NotFoundError(this.prefix, id);

    await this.runHook(this.hooks.get, "after", id, entity);

    return entity;
  }

  async maybeGet(id: string): Promise<z.infer<S> | null> {
    await this.runHook(this.hooks.maybeGet, "before", id);

    const raw = await this.kv.get(this.key(id));

    const entity = raw ? this.entitySchema.parse(JSON.parse(raw)) : null;

    await this.runHook(this.hooks.maybeGet, "after", id, entity);

    return entity;
  }

  async create(data: z.input<S>): Promise<z.infer<S>> {
    await this.runHook(this.hooks.create, "before", data);

    const parsed = this.entitySchema.parse(data);

    await this.kv.set(this.key(parsed.id as string), JSON.stringify(parsed));

    await this.runHook(this.hooks.create, "after", data, parsed);

    return parsed;
  }

  async getAll(pattern = "*"): Promise<z.infer<S>[]> {
    await this.runHook(this.hooks.getAll, "before", pattern);

    const keys = await this.kv.keys(`${this.prefix}:${pattern}`);

    const raws = keys.length ? await this.kv.mget(...keys) : [];

    const result = raws.filter(Boolean).map((v) =>
      this.entitySchema.parse(JSON.parse(v!))
    );

    await this.runHook(this.hooks.getAll, "after", pattern, result);

    return result;
  }

  async update(
    id: string,
    patch: Partial<z.input<S>>,
  ): Promise<z.infer<S> | null> {
    await this.runHook(this.hooks.update, "before", { id, patch });

    const existing = await this.maybeGet(id);
    if (!existing) return null;

    const updated = this.entitySchema.parse({
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    });

    await this.kv.set(this.key(updated.id as string), JSON.stringify(updated));

    await this.runHook(this.hooks.update, "after", { id, patch }, updated);

    return updated;
  }

  async updateOrFail(
    id: string,
    patch: Partial<z.input<S>>,
  ): Promise<z.infer<S>> {
    await this.runHook(this.hooks.updateOrFail, "before", { id, patch });

    const existing = await this.get(id);

    const updated = this.entitySchema.parse({
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    });

    await this.kv.set(this.key(updated.id as string), JSON.stringify(updated));

    await this.runHook(
      this.hooks.updateOrFail,
      "after",
      { id, patch },
      updated,
    );
    return updated;
  }

  async delete(id: string): Promise<boolean> {
    await this.runHook(this.hooks.delete, "before", id);

    const result = (await this.kv.del(this.key(id))) === 1;

    await this.runHook(this.hooks.delete, "after", id, result);

    return result;
  }

  async deleteAll(pattern = "*"): Promise<number> {
    await this.runHook(this.hooks.deleteAll, "before", pattern);

    const keys = await this.kv.keys(`${this.prefix}:${pattern}`);

    const result = keys.length ? await this.kv.del(...keys) : 0;

    await this.runHook(this.hooks.deleteAll, "after", pattern, result);

    return result;
  }

  addHooks(hooks: KvOrmHooks<S>) {
    Object.assign(this.hooks, hooks);
  }
}
