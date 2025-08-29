# kv-orm

a **Redis-backed ORM** with Zod validation, automatic
`id`/`createdAt`/`updatedAt`, hooks, and optional secondary indexes (`set` and
`zset`).

## Installation

```bash
# Using Deno
deno add jsr:@simkit/kv-orm npm:zod

# Using pnpm
pnpm add jsr:@simkit/kv-orm zod
```

## Roadmap

- **Migration System** – Support for schema migrations and versioning of
  entities.
- **Query Pagination** – Add `offset` and `limit` parameters for `getAll` and
  `findWhere` queries.
- **Advanced Indexing** – Support compound and multi-field indexes.
- **Batch Operations** – Further optimizations for large bulk inserts/updates.
- **TypeScript Enhancements** – Explore stricter compile-time checks for
  reserved fields without runtime errors.

> Contributions and suggestions are welcome!

## Basic Usage

```ts
import { KvOrm, KvOrmSchema } from "@simkit/kv-orm";
import { z } from "zod";
import Redis from "ioredis";

// Initialize Redis
const redis = new Redis();

// Define a schema
const userSchema = KvOrmSchema({
  email: z.string().email(),
  name: z.string(),
});

// Create KvOrm instance
const usersOrm = new KvOrm({
  prefix: "user",
  kv: redis,
  schema: userSchema,
  indexedFields: { email: "set" }, // optional secondary indexes
});
```

> **Note:** `id`, `createdAt`, and `updatedAt` are automatically added and
> **cannot be redefined**.

## Methods

### `create(data, options?)`

Create a new entity.

```ts
const user = await usersOrm.create({
  email: "alice@example.com",
  name: "Alice",
});
```

- `data`: input object matching schema
- `options`: optional hooks `{ hooks?: { before?, after? } }`

### `createBulk(dataArray, options?)`

Create multiple entities at once.

```js
const newUsers = await usersOrm.createBulk([
  { email: "bob@example.com", name: "Bob" },
  { email: "carol@example.com", name: "Carol" },
]);
```

### `get(id, options?)`

Get an entity by ID.

```ts
const user = await usersOrm.get("uuid-of-user");
```

### `maybeGet(id, options?)`

Get an entity by ID, returns `null` if not found.

```ts
const user = await usersOrm.maybeGet("uuid-that-may-not-exist");
```

### `getAll(pattern?, options?)`

Get all entities, optionally filtered by key pattern.

```ts
const allUsers = await usersOrm.getAll(); // default to "*"
```

### `findWhere(field, operator, value, options?)`

Query entities by field (supports indexed fields).

```ts
const result = await usersOrm.findWhere("email", "eq", "alice@example.com");
```

- `operator` can be:

  - **numbers/dates:** `eq | ne | lt | lte | gt | gte | in | nin`
  - **strings:** `eq | ne | like | in | nin`
  - **others:** `eq | ne | in | nin`

### `update(id, patch, options?)`

Update an entity partially, returns updated entity or `null`.

```ts
const updatedUser = await usersOrm.update(user.id, { name: "Alice Smith" });
```

### `updateOrFail(id, patch, options?)`

Update an entity, throws if not found.

```ts
const updatedUser = await usersOrm.updateOrFail(user.id, {
  name: "Alice Smith",
});
```

### `delete(id, options?)`

Delete an entity by ID.

```ts
const deleted = await usersOrm.delete(user.id); // true if deleted
```

### `deleteAll(pattern?, options?)`

Delete multiple entities matching a key pattern.

```ts
const count = await usersOrm.deleteAll(); // default to "*" and returns number of deleted entities
```

### `addHooks(hooks)`

Add global or dynamic hooks for any method.

```ts
users.addHooks({
  create: {
    before: async ({ input }) => console.log("Creating", input),
    after: async ({ result }) => console.log("Created", result),
  },
});
```

### `rebuildIndexes()`

Rebuild all secondary indexes (useful after bulk operations or corruption).

```ts
await usersOrm.rebuildIndexes();
```

## Schema Helper: `KvOrmSchema`

Use `KvOrmSchema` to automatically add required fields (`id`, `createdAt`,
`updatedAt`):

```ts
import { KvOrmSchema } from "kv-orm";
import { z } from "zod";

const productSchema = KvOrmSchema({
  name: z.string(),
  price: z.number(),
});
```

> **Important:** Do **not** redefine `id`, `createdAt`, or `updatedAt`. KvOrm
> will throw a runtime error if they are included.

## Hooks

All methods support **before/after hooks** for custom logic:

```ts
const hooks = {
  create: {
    before: async ({ input }) => {/* validate, modify input */},
    after: async ({ result }) => {/* log or trigger events */},
  },
};

usersOrm.addHooks(hooks);
```

## Indexed Fields

You can define fields for **fast queries**:

- `"set"` → equality queries (`eq`, `ne`, `in`, `nin`)
- `"zset"` → range queries (`lt`, `lte`, `gt`, `gte`)

```ts
const usersOrm = new KvOrm({
  prefix: "user",
  kv: redis,
  schema: userSchema,
  indexedFields: {
    email: "set",
    createdAt: "zset",
  },
});
```

## License

MIT
