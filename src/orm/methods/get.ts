import { z, ZodObject, ZodRawShape } from "zod";
import { NotFoundError } from "../../errors.ts";
import {
  KvOrmContext,
  KvOrmMethodOptions,
  RequiredZodFields,
} from "../../types.ts";

export async function get<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  id: string,
  options?: KvOrmMethodOptions<string, z.infer<S>>,
): Promise<z.infer<S>> {
  const hooksToRun = [
    context.initialHooks.get,
    options?.hooks,
    context.dynamicHooks.get,
  ];

  await context.runHooks(hooksToRun, "before", id);

  const entity = await maybeGet(context, id);
  if (!entity) throw new NotFoundError(context.prefix, id);

  await context.runHooks(hooksToRun, "after", id, entity);

  return entity;
}

export async function maybeGet<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  id: string,
  options?: KvOrmMethodOptions<string, z.infer<S> | null>,
): Promise<z.infer<S> | null> {
  const hooksToRun = [
    context.initialHooks.maybeGet,
    options?.hooks,
    context.dynamicHooks.maybeGet,
  ];

  await context.runHooks(hooksToRun, "before", id);

  const raw = await context.kv.get(context.key(id));
  const entity = raw ? context.entitySchema.parse(JSON.parse(raw)) : null;

  await context.runHooks(hooksToRun, "after", id, entity);

  return entity;
}

export async function getAll<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
>(
  context: KvOrmContext<S>,
  pattern = "*",
  options?: KvOrmMethodOptions<string, z.infer<S>[]>,
): Promise<z.infer<S>[]> {
  const hooksToRun = [
    context.initialHooks.getAll,
    options?.hooks,
    context.dynamicHooks.getAll,
  ];

  await context.runHooks(hooksToRun, "before", pattern);

  const keys = await context.scanKeys(pattern);
  const raws = keys.length ? await context.kv.mget(...keys) : [];

  const result = raws.filter(Boolean).map((v) =>
    context.entitySchema.parse(JSON.parse(v!))
  );

  await context.runHooks(hooksToRun, "after", pattern, result);

  return result;
}
