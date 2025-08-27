/**
 * The interface for a Redis-like client.
 * This decouples the ORM from a specific client implementation,
 * making it compatible with both upstash/redis and node-redis.
 */
export interface KvClient {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<"OK" | null>;
  del(...keys: string[]): Promise<number>;
  keys(pattern: string): Promise<string[]>;
  mget(...keys: string[]): Promise<(string | null)[]>;
}
