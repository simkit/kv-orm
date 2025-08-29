import type { ZodObject, ZodRawShape } from "zod";
import type {
  KvOrmContext,
  KvOrmMethodOptions,
  RequiredZodFields,
} from "../../types.ts";
import { maybeGet } from "./get.ts";

/**
 * Delete an entity by ID.
 */
export async function deleteEntity<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  id: string,
  options?: KvOrmMethodOptions<string, boolean>,
): Promise<boolean> {
  const hooksToRun = [
    context.initialHooks.delete,
    options?.hooks,
    context.dynamicHooks.delete,
  ];

  await context.runHooks(hooksToRun, "before", id);
  const entity = await maybeGet(context, id);
  if (!entity) return false;

  const multi = context.kv.multi();
  multi.del(context.key(id));
  context.deleteIndexes(multi, entity);
  const results = await multi.exec();

  return results !== null && results[0] !== null && results[0][1] === 1;
}

/**
 * Delete all entities matching a key pattern (default "*").
 */
export async function deleteAll<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  pattern = "*",
  options?: KvOrmMethodOptions<string, number>,
): Promise<number> {
  const hooksToRun = [
    context.initialHooks.deleteAll,
    options?.hooks,
    context.dynamicHooks.deleteAll,
  ];

  await context.runHooks(hooksToRun, "before", pattern);

  const keys = await context.scanKeys(pattern);
  if (!keys.length) {
    return 0;
  }

  const raws = await context.kv.mget(...keys);
  const entities = raws.filter(Boolean).map((v) =>
    context.entitySchema.parse(JSON.parse(v!))
  );

  const multi = context.kv.multi();
  multi.del(...keys);
  for (const entity of entities) {
    context.deleteIndexes(multi, entity);
  }
  const results = await multi.exec();

  if (results === null || !results[0] || results[0][1] === null) {
    return 0;
  }
  const deletedCount = results[0][1] as number;

  await context.runHooks(hooksToRun, "after", pattern, deletedCount);

  return deletedCount;
}
