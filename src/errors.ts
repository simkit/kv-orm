/**
 * Error thrown when an entity with a given ID is not found in Redis.
 *
 * Used by KvOrm methods like `get`, `updateOrFail`, etc.
 */
export class NotFoundError extends Error {
  /**
   * Creates a new NotFoundError instance.
   *
   * @param prefix - The Redis key prefix / entity type (e.g., "user")
   * @param id - The ID of the entity that was not found
   *
   * @example
   * throw new NotFoundError("user", "123e4567-e89b-12d3-a456-426614174000");
   */
  constructor(prefix: string, id: string) {
    super(`${prefix} with id '${id}' not found`);
    this.name = "NotFoundError";
  }
}
