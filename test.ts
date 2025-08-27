// main.ts

import { z } from "zod";
import crypto from "node:crypto";
import { Redis as UpstashRedis } from "@upstash/redis";
import { KvOrm } from "./index.ts";
import { UpstashRedisAdapter } from "./adapters/upstash.ts";

const userSchema = z.object({
  id: z.uuid().default(() => crypto.randomUUID()),
  email: z.email(),
  name: z.string(),
  createdAt: z.iso.datetime().default(() => new Date().toISOString()),
  updatedAt: z.iso.datetime().default(() => new Date().toISOString()),
});

const upstashClient = new UpstashRedis({
  url: Deno.env.get("UPSTASH_REDIS_REST_URL")!,
  token: Deno.env.get("UPSTASH_REDIS_REST_TOKEN")!,
});

const upstashAdapter = new UpstashRedisAdapter(upstashClient);

const usersOrm = new KvOrm({
  prefix: "users",
  kv: upstashAdapter,
  schema: userSchema,
});

// --- Example Usage ---
async function runExample() {
  console.log("Creating a new user...");
  const newUser = await usersOrm.create({
    email: "john.doe@example.com",
    name: "John Doe",
  });
  console.log("New User created:", newUser);

  console.log("\nRetrieving user by ID...");
  const retrievedUser = await usersOrm.getById(newUser.id);

  console.log("Retrieved User:", retrievedUser);

  console.log("\nUpdating the user's name...");
  const updatedUser = await usersOrm.update(newUser.id, {
    name: "Johnathan Doe",
  });
  console.log("Updated User:", updatedUser);

  console.log("\nRetrieving all users...");
  const allUsers = await usersOrm.getAll();
  console.log("All users in the database:", allUsers);

  console.log("\nDeleting the user...");
  const isDeleted = await usersOrm.delete(newUser.id);
  console.log("User deleted:", isDeleted);
}

runExample().catch(console.error);
