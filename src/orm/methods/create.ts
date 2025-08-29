import type { z, ZodObject, ZodRawShape } from "zod";
import type {
  KvOrmContext,
  KvOrmMethodOptions,
  RequiredZodFields,
} from "../../types.ts";

/**
 * Create a new entity. Generates UUID and timestamps automatically.
 */
export async function create<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  data: z.input<S>,
  options?: KvOrmMethodOptions<z.input<S>, z.infer<S>>,
): Promise<z.infer<S>> {
  const hooksToRun = [
    context.initialHooks.create,
    options?.hooks,
    context.dynamicHooks.create,
  ];
  await context.runHooks(hooksToRun, "before", data);

  const entity = context.normalizeEntity(data);

  const multi = context.kv.multi();
  multi.set(context.key(entity.id as string), JSON.stringify(entity));
  context.updateIndexes(multi, entity);
  await multi.exec();

  await context.runHooks(hooksToRun, "after", data, entity);

  return entity;
}

/**
 * Create multiple entities in one batch.
 */
export async function createBulk<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  data: z.input<S>[],
  options?: KvOrmMethodOptions<z.input<S>[], z.infer<S>[]>,
): Promise<z.infer<S>[]> {
  const hooksToRun = [
    context.initialHooks.createBulk,
    options?.hooks,
    context.dynamicHooks.createBulk,
  ];
  await context.runHooks(hooksToRun, "before", data);

  const entities = data.map((d) => context.normalizeEntity(d));

  const multi = context.kv.multi();
  for (const entity of entities) {
    multi.set(context.key(entity.id as string), JSON.stringify(entity));
    context.updateIndexes(multi, entity);
  }
  await multi.exec();

  await context.runHooks(hooksToRun, "after", data, entities);

  return entities;
}
