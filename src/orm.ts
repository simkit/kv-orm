import { type Redis } from "ioredis";
import { z, ZodObject, ZodRawShape } from "zod";

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
  S extends ZodObject<ZodRawShape> & {
    shape: RequiredZodFields;
  },
> {
  private prefix: string;
  private kv: Redis;
  public readonly entitySchema: S;

  constructor(opts: KvOrmOptions<S>) {
    this.prefix = opts.prefix;
    this.kv = opts.kv;
    this.entitySchema = opts.schema;
  }

  private key(id: string): string {
    return `${this.prefix}:${id}`;
  }

  async get(id: string): Promise<z.infer<S>> {
    const entity = await this.maybeGet(id);
    if (!entity) throw new NotFoundError(this.prefix, id);
    return entity;
  }

  async maybeGet(id: string): Promise<z.infer<S> | null> {
    const raw = await this.kv.get(this.key(id));
    if (!raw) return null;

    return this.entitySchema.parse(JSON.parse(raw));
  }

  async create(data: z.input<S>): Promise<z.infer<S>> {
    const parsed = this.entitySchema.parse(data);
    await this.kv.set(this.key(parsed.id as string), JSON.stringify(parsed));
    return parsed;
  }

  async getAll(pattern = "*"): Promise<z.infer<S>[]> {
    const keys = await this.kv.keys(`${this.prefix}:${pattern}`);
    if (keys.length === 0) return [];

    const raws = await this.kv.mget(...keys);
    return raws
      .filter(Boolean)
      .map((v) => this.entitySchema.parse(JSON.parse(v as string)));
  }

  async update(
    id: string,
    patch: Partial<z.input<S>>,
  ): Promise<z.infer<S> | null> {
    const existing = await this.maybeGet(id);
    if (!existing) return null;

    const updated = this.entitySchema.parse({
      ...existing,
      ...patch,
      updatedAt: new Date().toISOString(),
    });

    await this.kv.set(this.key(updated.id as string), JSON.stringify(updated));
    return updated;
  }

  async updateOrFail(
    id: string,
    patch: Partial<z.input<S>>,
  ): Promise<z.infer<S>> {
    const existing = await this.get(id);
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

// NotFound error
export class NotFoundError extends Error {
  constructor(entity: string, id: string) {
    super(`${entity} with id '${id}' not found`);
    this.name = "NotFoundError";
  }
}
