// upstash-redis-adapter.ts

import { Redis as UpstashRedis } from "@upstash/redis";
import { KvClient } from "../types.ts";

export class UpstashRedisAdapter implements KvClient {
  private client: UpstashRedis;

  constructor(client: UpstashRedis) {
    this.client = client;
  }

  async get(key: string): Promise<string | null> {
    const value = await this.client.get<string | null>(key);
    return value;
  }

  async set(key: string, value: string): Promise<"OK" | null> {
    const result = await this.client.set(key, value);
    return result === "OK" ? "OK" : null;
  }

  // deno-lint-ignore require-await
  async del(...keys: string[]): Promise<number> {
    return this.client.del(...keys);
  }

  // deno-lint-ignore require-await
  async keys(pattern: string): Promise<string[]> {
    return this.client.keys(pattern);
  }

  async mget(...keys: string[]): Promise<(string | null)[]> {
    const result = await this.client.mget<(string | null)[]>(...keys);
    return result;
  }
}
