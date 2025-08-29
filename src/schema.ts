import { z, type ZodRawShape } from "zod";
import crypto from "node:crypto";

/**
 * Fields required by all KvOrm schemas.
 *
 * - id: UUID (auto-generated if not provided)
 * - createdAt: Date (set on insert, defaults to now)
 * - updatedAt: Date (set on insert/update, defaults to now)
 */
export const baseFields = {
  id: z.uuid().default(() => crypto.randomUUID()),
  createdAt: z.coerce.date().default(() => new Date()),
  updatedAt: z.coerce.date().default(() => new Date()),
};

/**
 * Helper to create an entity schema with KvOrm's required fields.
 *
 * Automatically adds:
 * - id: UUID (default: random UUID)
 * - createdAt: Date (default: now)
 * - updatedAt: Date (default: now)
 *
 * Runtime enforcement: `id`, `createdAt`, and `updatedAt` **cannot be redefined**.
 * If they exist in the input shape, an error will be thrown.
 *
 * Example:
 * ```js
 * const userSchema = KvOrmSchema({
 *   email: z.string().email(),
 *   name: z.string(),
 * });
 * ```
 */
export const KvOrmSchema = <T extends ZodRawShape>(shape: T) => {
  const reservedKeys = Object.keys(baseFields);

  for (const key of reservedKeys) {
    if (key in shape) {
      throw new Error(
        `${key} is a required field and cannot be redefined in KvOrmSchema. ` +
          `Remove it from your schema or use z.object() directly.`,
      );
    }
  }

  return z.object({
    ...baseFields,
    ...shape,
  });
};
