import { z, ZodObject, ZodRawShape } from "zod";
import { KvClient } from "./types.ts";

/**
 * A type that represents the required base Zod types for any ORM entity.
 * This is used for type-level enforcement on the consumer's side.
 */
export type RequiredZodFields = {
  id: z.ZodDefault<z.ZodUUID>;
  updatedAt: z.ZodDefault<z.ZodISODateTime>;
  createdAt: z.ZodDefault<z.ZodISODateTime>;
};

/**
 * Options for the KvOrm constructor.
 * The schema must satisfy the strict type constraint.
 */
export interface KvOrmOptions<
  S extends ZodObject<ZodRawShape> & {
    shape: RequiredZodFields;
  },
> {
  prefix: string;
  kv: KvClient;
  schema: S;
}

/**
 * A type-safe, Zod-powered ORM for Redis.
 * It strictly requires entities to have `id`, `createdAt`, and `updatedAt` fields
 * with their specific Zod types.
 *
 * @template S The Zod schema for the entity, which must include the RequiredZodFields.
 */
export class KvOrm<
  S extends ZodObject<ZodRawShape> & {
    shape: RequiredZodFields;
  },
> {
  private prefix: string;
  private kv: KvClient;
  public readonly entitySchema: S;

  constructor(opts: KvOrmOptions<S>) {
    this.prefix = opts.prefix;
    this.kv = opts.kv;
    this.entitySchema = opts.schema;
  }

  private key(id: string): string {
    return `${this.prefix}:${id}`;
  }

  async getAll(pattern = "*"): Promise<z.infer<S>[]> {
    const keys = await this.kv.keys(`${this.prefix}:${pattern}`);
    if (keys.length === 0) return [];
    const raws = await this.kv.mget(...keys);
    return raws
      .filter(Boolean)
      .map((v) => this.entitySchema.parse(JSON.parse(v as string)));
  }

  async getById(id: string): Promise<z.infer<S> | null> {
    const raw = await this.kv.get(this.key(id));
    if (!raw) return null;
    return this.entitySchema.parse(JSON.parse(raw));
  }

  async create(data: z.input<S>): Promise<z.infer<S>> {
    const parsed = this.entitySchema.parse(data);

    await this.kv.set(this.key(parsed.id as string), JSON.stringify(parsed));

    return parsed;
  }

  async update(
    id: string,
    patch: Partial<z.input<S>>,
  ): Promise<z.infer<S> | null> {
    const existing = await this.getById(id);
    if (!existing) return null;
    const updated = this.entitySchema.parse({
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    });
    await this.kv.set(this.key(updated.id as string), JSON.stringify(updated));
    return updated;
  }

  async delete(id: string): Promise<boolean> {
    return (await this.kv.del(this.key(id))) === 1;
  }
}
