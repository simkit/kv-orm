import type { z, ZodObject, ZodRawShape } from "zod";
import type {
  KvOrmContext,
  KvOrmMethodOptions,
  OperatorFor,
  RequiredZodFields,
} from "../../types.ts";
import { getAll } from "./get.ts";

/**
 * Find entities where a field satisfies an operator and value.
 * Tips: the indexed fields will make querying faster.
 */
export async function findWhere<
  S extends ZodObject<ZodRawShape> & { shape: RequiredZodFields },
  K extends keyof z.infer<S>,
  V extends z.infer<S>[K],
>(
  context: KvOrmContext<S>,
  field: K,
  operator: OperatorFor<V>,
  value: V | V[],
  options?: KvOrmMethodOptions<
    { field: K; operator: OperatorFor<V>; value: V | V[] },
    z.infer<S>[]
  >,
): Promise<z.infer<S>[]> {
  const initialHook = context.initialHooks.findWhere?.();
  const dynamicHook = context.dynamicHooks.findWhere?.();
  const hooksToRun = [
    initialHook,
    options?.hooks,
    dynamicHook,
  ];
  const input = { field, operator, value };

  await context.runHooks(hooksToRun, "before", input);

  let ids: string[] = [];
  const indexType = context.indexedFields[field];

  if (indexType) {
    const supportedBySet = ["eq", "ne", "in", "nin"].includes(
      operator as string,
    );
    const supportedByZset = ["lt", "lte", "gt", "gte"].includes(
      operator as string,
    );

    if (
      (indexType === "set" && supportedBySet) ||
      (indexType === "zset" && supportedByZset)
    ) {
      ids = await context.findWhereIndexed(field, operator, value);
    }
  }

  if (!ids.length && (!indexType || ["like"].includes(operator as string))) {
    const allEntities = await getAll(context);
    const results = allEntities.filter((entity) => {
      const fieldValue = entity[field] as V;

      switch (operator) {
        case "eq":
          return fieldValue === value;
        case "ne":
          return fieldValue !== value;
        case "lt":
          if (
            (typeof fieldValue === "string" && typeof value === "string") ||
            (typeof fieldValue === "number" && typeof value === "number") ||
            (fieldValue instanceof Date && value instanceof Date)
          ) {
            return fieldValue < value;
          }
          return false;
        case "lte":
          if (
            (typeof fieldValue === "string" && typeof value === "string") ||
            (typeof fieldValue === "number" && typeof value === "number") ||
            (fieldValue instanceof Date && value instanceof Date)
          ) {
            return fieldValue <= value;
          }
          return false;
        case "gt":
          if (
            (typeof fieldValue === "string" && typeof value === "string") ||
            (typeof fieldValue === "number" && typeof value === "number") ||
            (fieldValue instanceof Date && value instanceof Date)
          ) {
            return fieldValue > value;
          }
          return false;
        case "gte":
          if (
            (typeof fieldValue === "string" && typeof value === "string") ||
            (typeof fieldValue === "number" && typeof value === "number") ||
            (fieldValue instanceof Date && value instanceof Date)
          ) {
            return fieldValue >= value;
          }
          return false;
        case "like":
          return (
            typeof fieldValue === "string" &&
            typeof value === "string" &&
            fieldValue.toLowerCase().includes(value.toLowerCase())
          );
        case "in":
          return Array.isArray(value) && (value as V[]).includes(fieldValue);
        case "nin":
          return Array.isArray(value) && !(value as V[]).includes(fieldValue);
      }
    });
    await context.runHooks(hooksToRun, "after", input, results);
    return results;
  }

  const keys = ids.map((id) => context.key(id));
  const rawEntities = keys.length ? await context.kv.mget(...keys) : [];

  const entities = rawEntities
    .filter((raw): raw is string => raw !== null)
    .map((raw) => context.entitySchema.parse(JSON.parse(raw)));

  await context.runHooks(hooksToRun, "after", input, entities);

  return entities;
}
