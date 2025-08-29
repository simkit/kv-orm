import type { z, ZodObject, ZodRawShape } from "zod";
import type {
  KvOrmContext,
  KvOrmMethodOptions,
  RequiredZodFields,
} from "../../types.ts";
import { get, maybeGet } from "./get.ts";

/**
 * Make a partial update to an entity. Returns null if not found.
 */
export async function update<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  id: string,
  patch: Partial<z.input<S>>,
  options?: KvOrmMethodOptions<
    { id: string; patch: Partial<z.input<S>> },
    z.infer<S> | null
  >,
): Promise<z.infer<S> | null> {
  const hooksToRun = [
    context.initialHooks.update,
    options?.hooks,
    context.dynamicHooks.update,
  ];
  const input = { id, patch };

  await context.runHooks(hooksToRun, "before", input);

  await context.kv.watch(context.key(id));
  const existing = await maybeGet(context, id);

  if (!existing) {
    await context.kv.unwatch();
    return null;
  }

  const updated = context.normalizeEntity({ ...existing, ...patch }, false);

  const multi = context.kv.multi();
  multi.set(context.key(updated.id as string), JSON.stringify(updated));
  context.updateIndexes(multi, updated, true, existing);

  const results = await multi.exec();

  if (results === null) {
    return null;
  }

  await context.runHooks(hooksToRun, "after", input, updated);

  return updated;
}

/**
 * Make a partial update to an entity. Throws error if entity does not exist
 */
export async function updateOrFail<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  id: string,
  patch: Partial<z.input<S>>,
  options?: KvOrmMethodOptions<
    { id: string; patch: Partial<z.input<S>> },
    z.infer<S>
  >,
): Promise<z.infer<S>> {
  const hooksToRun = [
    context.initialHooks.updateOrFail,
    options?.hooks,
    context.dynamicHooks.updateOrFail,
  ];
  const input = { id, patch };

  await context.runHooks(hooksToRun, "before", input);

  await context.kv.watch(context.key(id));
  const existing = await get(context, id);

  const updated = context.normalizeEntity({ ...existing, ...patch }, false);

  const multi = context.kv.multi();
  multi.set(context.key(updated.id as string), JSON.stringify(updated));
  context.updateIndexes(multi, updated, true, existing);

  const results = await multi.exec();

  if (results === null) {
    throw new Error(
      `Optimistic locking failed for key: ${
        context.key(id)
      }. Please retry the operation.`,
    );
  }

  await context.runHooks(hooksToRun, "after", input, updated);

  return updated;
}
