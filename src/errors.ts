// NotFound error
export class NotFoundError extends Error {
  constructor(prefix: string, id: string) {
    super(`${prefix} with id '${id}' not found`);
    this.name = "NotFoundError";
  }
}
