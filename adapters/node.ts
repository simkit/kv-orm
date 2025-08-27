import { createClient } from "redis";
import { KvClient } from "../types.ts";

type NodeRedisClient = ReturnType<typeof createClient>;

export class NodeRedisAdapter implements KvClient {
  private client: NodeRedisClient;

  constructor(client: NodeRedisClient) {
    this.client = client;
  }

  // deno-lint-ignore require-await
  async get(key: string): Promise<string | null> {
    return this.client.get(key);
  }

  async set(key: string, value: string): Promise<"OK" | null> {
    const result = await this.client.set(key, value);
    return result === "OK" ? "OK" : null;
  }

  // deno-lint-ignore require-await
  async del(...keys: string[]): Promise<number> {
    return this.client.del(keys);
  }

  async keys(pattern: string): Promise<string[]> {
    const result = await this.client.keys(pattern);
    return result;
  }

  async mget(...keys: string[]): Promise<(string | null)[]> {
    const result = await this.client.mget(keys) as (string | null)[];
    return result;
  }
}
