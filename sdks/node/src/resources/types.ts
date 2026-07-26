/**
 * Lifetime of a registered resource. Names and wire forms are shared across SDKs.
 *
 * `"thread"` has no Node equivalent — a worker runs its tasks on one event loop,
 * so there is no per-thread identity to key an instance on.
 */
export type ResourceScope = "worker" | "task" | "pooled" | "request";

/** Every resource scope, for runtime validation. */
export const RESOURCE_SCOPES: readonly ResourceScope[] = ["worker", "task", "pooled", "request"];

/**
 * Passed to a resource factory so it can depend on other resources. Worker- and
 * pooled-scoped factories may only depend on worker-scoped resources (their
 * instances outlive any single task); task- and request-scoped factories may
 * depend on any.
 */
export interface ResourceContext {
  /** Scope of the resource currently being built. */
  readonly scope: ResourceScope;
  /** Resolve a dependency resource by name. */
  use<T>(name: string): Promise<T>;
}

/** How to build — and optionally tear down — a resource of type `T`. */
export interface ResourceDefinition<T = unknown> {
  /** Builds the value. May be async and may depend on other resources via `ctx.use`. */
  factory: (ctx: ResourceContext) => T | Promise<T>;
  /** When the value is created and disposed. */
  scope: ResourceScope;
  /** Tear-down hook, run LIFO when the scope ends (worker stop / job finish). */
  dispose?: (value: T) => void | Promise<void>;
  /** Include in a no-argument `Queue.reloadResources()` sweep. Default false. */
  reloadable?: boolean;
  /** Pool tuning; only meaningful for `"pooled"` scope. */
  pool?: PoolOptions;
  /**
   * Returns truthy while the value is healthy. Checked on the interval below;
   * a failing (or throwing) check triggers recreation. Worker scope only.
   */
  healthCheck?: (value: T) => boolean | Promise<boolean>;
  /** Milliseconds between health checks. 0 or absent disables checking. */
  healthCheckIntervalMs?: number;
  /**
   * Consecutive failed checks tolerated while recreation also fails before the
   * resource is marked permanently unhealthy. Default 3.
   */
  maxRecreationAttempts?: number;
}

/** Tuning for the bounded checkout pool behind a `"pooled"`-scope resource. */
export interface PoolOptions {
  /** Max instances checked out concurrently. Default 4. */
  poolSize?: number;
  /** Instances built eagerly when the worker starts. Default 0 (lazy). */
  poolMin?: number;
  /** How long a checkout waits for a free slot before failing. Default 10000. */
  acquireTimeoutMs?: number;
  /** Max age of an idle instance before it is disposed and rebuilt. Default unlimited. */
  maxLifetimeMs?: number;
}

/** Point-in-time counters for one resource pool. */
export interface PoolStats {
  /** Configured capacity (`poolSize`). */
  size: number;
  /** Instances currently checked out. */
  active: number;
  /** Instances sitting idle, ready for reuse. */
  idle: number;
  /** Successful checkouts so far. */
  totalAcquisitions: number;
  /** Checkouts that timed out waiting for capacity. */
  totalTimeouts: number;
}

/** Resolves a resource by name within the current scope. @internal */
export type ResourceResolver = (name: string) => Promise<unknown>;

/** Lifecycle counters for one resource. */
export interface ResourceStat {
  /** Successful factory builds so far. */
  created: number;
  /** Disposals run so far. */
  disposed: number;
  /** Currently-live instances (`created - disposed`). */
  active: number;
}

/** Per-resource lifecycle metrics, keyed by resource name. */
export type ResourceMetrics = Record<string, ResourceStat>;
