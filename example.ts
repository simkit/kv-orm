import { z } from "zod";
import { KvOrm, KvOrmSchema } from "@simkit/kv-orm";
import { Redis } from "ioredis";

// 1. Define schema with required fields automatically added
const userSchema = KvOrmSchema({
  email: z.email(),
  name: z.string(),
});

// 2. Create a Redis client
const redis = new Redis("<redis url>");

// 3. Initialize ORM
const usersOrm = new KvOrm({
  prefix: "user",
  kv: redis,
  schema: userSchema,
  indexedFields: { email: "set" }, // optional secondary indexes
});

// 4. Create a user
const alice = await usersOrm.create({
  email: "alice@example.com",
  name: "Alice",
});

// 5. Create multiple users in bulk
const users = await usersOrm.createBulk([
  { email: "bob@example.com", name: "Bob" },
  { email: "carol@example.com", name: "Carol" },
]);

// 6. Get a user by ID
const aliceFromDb = await usersOrm.get(alice.id);

// 7. Get a user by ID or null if not found
const maybeAlice = await usersOrm.maybeGet("non-existent-id");

// 8. Get all users
const allUsers = await usersOrm.getAll();

// 9. Find users by indexed field
const foundUsers = await usersOrm.findWhere("email", "eq", "bob@example.com");

// 10. Update a user partially
const updatedAlice = await usersOrm.update(alice.id, { name: "Alice Cooper" });

// 11. Update a user or throw if not found
const updatedBob = await usersOrm.updateOrFail("bob-id", { name: "Bobby" });

// 12. Delete a user
const deleted = await usersOrm.delete(alice.id);

// 13. Delete all users
const deletedCount = await usersOrm.deleteAll();

// 14. Add runtime hooks
usersOrm.addHooks({
  create: {
    before: async ({ input }) => console.log("Before create:", input),
    after: async ({ result }) => console.log("After create:", result),
  },
});

// 15. Rebuild all secondary indexes
await usersOrm.rebuildIndexes();
