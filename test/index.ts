import { z } from "zod";
import crypto from "node:crypto";
import { Redis } from "ioredis";
import { KvOrm } from "../src/orm.ts";

import "@std/dotenv/load";

const userSchema = z.object({
  id: z.uuid().default(() => crypto.randomUUID()),
  email: z.email(),
  name: z.string(),
  createdAt: z.iso.datetime().default(() => new Date().toISOString()),
  updatedAt: z.iso.datetime().default(() => new Date().toISOString()),
});

const kv = new Redis(Deno.env.get("REDIS_URL") as string);

const usersOrm = new KvOrm({
  prefix: "users",
  kv,
  schema: userSchema,
});

async function run() {
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

  // console.log("\nDeleting the user...");
  // const isDeleted = await usersOrm.delete(newUser.id);
  // console.log("User deleted:", isDeleted);

  kv.quit();
}

run().catch(console.error);
