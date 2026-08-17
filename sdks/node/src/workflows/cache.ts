/**
 * Built-in task that a cache hit runs instead of the real step: it returns its
 * single argument (the cached value) verbatim. Handled inline by the worker, so
 * it never needs registering. The double-underscore name avoids user collisions.
 */
export const CACHE_TASK = "__flexiq_cache__";

/** Settings-KV prefix for stored workflow step results. */
const KEY_PREFIX = "wf:cache:";

/** The native settings KV surface the cache store needs. */
interface SettingsKv {
  getSetting(key: string): string | null;
  setSetting(key: string, value: string): void;
  deleteSetting(key: string): boolean;
  listSettings(): Record<string, string>;
}

/** A stored cache entry: base64 result bytes + write time (for TTL). */
interface CacheEntry {
  r: string;
  t: number;
}

/**
 * Cross-run store for cacheable workflow step results, backed by the shared
 * settings KV so an incremental re-run of a workflow reuses prior results. Keys
 * are content hashes (task + args + upstream results), so any upstream change
 * misses and re-runs the affected subtree.
 */
export class WorkflowCacheStore {
  constructor(
    private readonly kv: SettingsKv,
    private readonly now: () => number = Date.now,
  ) {}

  /** Cached result bytes for `key`, or `undefined` on miss / expiry. */
  get(key: string, ttlMs?: number): Buffer | undefined {
    const storageKey = KEY_PREFIX + key;
    const raw = this.kv.getSetting(storageKey);
    if (!raw) {
      return undefined;
    }
    // A corrupt entry must never stall workflow advancement: drop it and treat
    // the read as a miss so the step simply re-runs.
    let entry: CacheEntry;
    try {
      entry = JSON.parse(raw) as CacheEntry;
    } catch {
      this.kv.deleteSetting(storageKey);
      return undefined;
    }
    if (typeof entry?.r !== "string" || typeof entry?.t !== "number") {
      this.kv.deleteSetting(storageKey);
      return undefined;
    }
    if (ttlMs !== undefined && this.now() - entry.t > ttlMs) {
      return undefined;
    }
    return Buffer.from(entry.r, "base64");
  }

  /** Store `value` under `key`. */
  set(key: string, value: Buffer): void {
    const entry: CacheEntry = { r: value.toString("base64"), t: this.now() };
    this.kv.setSetting(KEY_PREFIX + key, JSON.stringify(entry));
  }

  /** Remove every cached workflow result. Returns the number of entries dropped. */
  clear(): number {
    let dropped = 0;
    for (const key of Object.keys(this.kv.listSettings())) {
      if (key.startsWith(KEY_PREFIX)) {
        this.kv.deleteSetting(key);
        dropped += 1;
      }
    }
    return dropped;
  }
}
