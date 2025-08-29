import { z } from "zod";
import { Redis } from "ioredis";

import "@std/dotenv/load";
import { KvOrm, KvOrmSchema } from "@simkit/kv-orm";

const userSchema = KvOrmSchema({
  email: z.email(),
  name: z.string(),
});

const kv = new Redis(Deno.env.get("REDIS_URL") as string);

// The `indexedFields` option tells the ORM which fields to index
// 'set' is for exact matches (eq, ne, in, nin)
// 'zset' is for range queries on numbers or dates (lt, lte, gt, gte)
const usersOrm = new KvOrm({
  prefix: "users",
  kv,
  schema: userSchema,
  indexedFields: {
    name: "set",
    createdAt: "zset",
  },
  hooks: {
    create: {
      before: ({ input }) => console.log("[HOOK] Before create:", input),
      after: ({ result }) => console.log("[HOOK] After create:", result),
    },
    get: {
      before: ({ input }) => console.log("[HOOK] Before get:", input),
      after: ({ input, result }) =>
        console.log("[HOOK] After get:", input, "->", result),
    },
    update: {
      before: ({ input }) =>
        console.log("[HOOK] Before update:", input, "result"),
      after: ({ result }) => console.log("[HOOK] After update:", result),
    },
    delete: {
      before: ({ input }) => console.log("[HOOK] Before delete:", input),
      after: ({ result }) => console.log("[HOOK] After delete:", result),
    },
    getAll: {
      before: ({ input }) => console.log("[HOOK] Before getAll:", input),
      after: ({ result }) => console.log("[HOOK] After getAll:", result),
    },
    deleteAll: {
      before: ({ input }) => console.log("[HOOK] Before deleteAll:", input),
      after: ({ result }) =>
        console.log("[HOOK] After deleteAll, deleted keys:", result),
    },
  },
});

async function run() {
  usersOrm.addHooks({
    create: {
      before: () => {
        console.log("This is create before addHooks ");
      },
    },
  });

  console.log("Creating a new user (John)...");
  const newUser = await usersOrm.create({
    email: "john.doe@example.com",
    name: "John Doe",
  }, {
    hooks: {
      before: () => {
        console.log("create before method hook...");
      },
    },
  });
  console.log("New User created:", newUser);

  console.log("\nCreating a second user (Jane)...");
  const secondUser = await usersOrm.create({
    email: "jane.doe@example.com",
    name: "Jane Doe",
  });
  console.log("Second User created:", secondUser);

  console.log("\nRetrieving user by ID...");
  const retrievedUser = await usersOrm.get(newUser.id);
  console.log("Retrieved User:", retrievedUser);

  console.log("\nUpdating the first user's name...");
  const updatedUser = await usersOrm.updateOrFail(newUser.id, {
    name: "Johnathan Doe",
  });
  console.log("Updated User:", updatedUser);

  console.log(
    "\nSearching for users with name 'Johnathan Doe' (using index)...",
  );
  const filteredUsers = await usersOrm.findWhere("name", "eq", "Johnathan Doe");
  console.log("Found users by name:", filteredUsers);

  console.log(
    "\nSearching for users created in a specific time range (using index)...",
  );
  const recentUsers = await usersOrm.findWhere(
    "createdAt",
    "gte",
    new Date("2024-01-01T00:00:00.000Z"),
  );
  // const recentUsers = await usersOrm.findWhere(
  //   "name",
  //   "eq",
  //   "Doe",
  // );
  console.log("Found recent users:", recentUsers);

  console.log("\nRetrieving all users...");
  const allUsers = await usersOrm.getAll();
  console.log("All users in the database:", allUsers);

  console.log("\nDeleting all users...");
  const deletedCount = await usersOrm.deleteAll();
  console.log("Deleted", deletedCount, "users.");

  kv.quit();
}

run().catch(console.error);
