import {
  ResourceError,
  ResourceNotFoundError,
  ResourceScopeError,
  ResourceUnavailableError,
} from "../errors";
import { createLogger } from "../utils";
import { HealthChecker } from "./health";
import { ResourcePool } from "./pool";
import type {
  ResourceContext,
  ResourceDefinition,
  ResourceMetrics,
  ResourceResolver,
  ResourceScope,
} from "./types";
import { RESOURCE_SCOPES } from "./types";

const log = createLogger("resources");

/** Shared empty ancestry for a top-level resolve. */
const EMPTY_CHAIN: ReadonlySet<string> = new Set();

/** A disposal thunk plus the resource name, for error context. */
interface Teardown {
  name: string;
  run: () => void | Promise<void>;
  /** The build this thunk disposes, so recreation can retire exactly it. */
  built?: Promise<unknown>;
}

/** Per-resource health, as advertised in the worker heartbeat. */
export type HealthState = "healthy" | "unhealthy";

/** Per-invocation task scope: caches task-scoped resources and disposes them. */
export interface TaskScope {
  /** Resolve a resource for this invocation (task cache first, then worker). */
  readonly resolver: ResourceResolver;
  /** Dispose the task-scoped resources built during this invocation, LIFO. */
  teardown(): Promise<void>;
}

/**
 * Registry + lifecycle for injectable resources. Worker-scoped values are built
 * once and shared; task-scoped values are built per invocation; pooled values
 * are checked out of a bounded pool per invocation and returned afterwards. The
 * worker wires this into task execution; tasks reach values via `useResource`
 * or declarative `inject`.
 *
 * Every resolver always returns a promise — guard and factory failures surface as
 * rejections, never synchronous throws — so a failed build is awaited and retried,
 * not cached.
 */
export class ResourceRuntime {
  private readonly defs = new Map<string, ResourceDefinition>();
  private readonly workerCache = new Map<string, Promise<unknown>>();
  /** Worker resources each worker resource used, recorded at build time for reload ordering. */
  private readonly workerDeps = new Map<string, Set<string>>();
  private readonly workerTeardown: Teardown[] = [];
  private readonly counters = new Map<string, { created: number; disposed: number }>();
  /** Checkout pools behind pooled-scope resources, built lazily per resource. */
  private readonly pools = new Map<string, ResourcePool>();
  /** Active worker leases sharing this runtime; teardown disposes only at zero. */
  private workerLeases = 0;
  /** Resources marked permanently unhealthy — resolving them rejects for good. */
  private readonly unhealthy = new Set<string>();
  /** One checker per runtime, shared by every lease — never one per worker. */
  private healthChecker: HealthChecker | undefined;

  /** Register (or replace) a resource definition. */
  register<T>(name: string, definition: ResourceDefinition<T>): void {
    if (!(RESOURCE_SCOPES as readonly string[]).includes(definition.scope)) {
      throw new ResourceError(
        `resource "${name}": unknown scope "${definition.scope}" — expected one of ${RESOURCE_SCOPES.join(", ")}`,
      );
    }
    this.defs.set(name, definition as ResourceDefinition);
    // A replacement definition starts with a clean bill of health.
    this.unhealthy.delete(name);
    // Drop any built worker instance so "replace" takes effect; the old
    // instance is still disposed by its queued teardown.
    this.workerCache.delete(name);
    this.workerDeps.delete(name);
    // Likewise retire the old pool; its idle instances are disposed best-effort.
    const stalePool = this.pools.get(name);
    if (stalePool) {
      this.pools.delete(name);
      void stalePool.shutdown();
    }
  }

  /** True when nothing is registered — lets the worker skip resource wiring. */
  get isEmpty(): boolean {
    return this.defs.size === 0;
  }

  /** Snapshot of per-resource lifecycle counters (created / disposed / active). */
  metrics(): ResourceMetrics {
    const out: ResourceMetrics = {};
    for (const [name, c] of this.counters) {
      out[name] = { created: c.created, disposed: c.disposed, active: c.created - c.disposed };
    }
    return out;
  }

  /** Get-or-create the mutable counter for a resource. */
  private counter(name: string): { created: number; disposed: number } {
    let c = this.counters.get(name);
    if (!c) {
      c = { created: 0, disposed: 0 };
      this.counters.set(name, c);
    }
    return c;
  }

  /** Build a worker-scoped resource once, memoizing the promise (dedups concurrent init). */
  private resolveWorker(name: string): Promise<unknown> {
    if (this.unhealthy.has(name)) {
      return Promise.reject(
        new ResourceUnavailableError(`Resource "${name}" is permanently unhealthy`),
      );
    }
    const cached = this.workerCache.get(name);
    if (cached) {
      return cached;
    }
    const def = this.defs.get(name);
    if (!def) {
      return Promise.reject(unregistered(name));
    }
    if (def.scope !== "worker") {
      return Promise.reject(new ResourceScopeError(name, def.scope));
    }
    // Rebuilt from scratch on every build so a changed factory can't leave a
    // dependency it no longer uses in the reload ordering.
    const deps = new Set<string>();
    this.workerDeps.set(name, deps);
    const ctx: ResourceContext = {
      scope: "worker",
      use: <T>(dep: string) => {
        deps.add(dep);
        return this.resolveWorker(dep) as Promise<T>;
      },
    };
    const counter = this.counter(name);
    counter.created += 1;
    // Register the pending promise BEFORE the factory body runs (deferred a
    // microtask) so a resource that resolves a dependency on itself reuses this
    // promise instead of recursing into resolveWorker and overflowing the stack.
    const built = Promise.resolve()
      .then(() => startFactory(def, ctx))
      .catch((error) => {
        counter.created -= 1; // a failed build is not a live resource
        this.workerCache.delete(name); // failed build is retryable, not cached
        throw error;
      });
    this.workerCache.set(name, built);
    this.trackResource(this.workerTeardown, name, def, built);
    return built;
  }

  /**
   * Resolve a dependency that must be worker-scoped. Pooled instances outlive
   * any single task, so their factories may only depend on worker singletons.
   */
  private resolveWorkerDependency(name: string, context: ResourceScope): Promise<unknown> {
    const def = this.defs.get(name);
    if (!def) {
      return Promise.reject(unregistered(name));
    }
    if (def.scope !== "worker") {
      return Promise.reject(new ResourceScopeError(name, def.scope, context));
    }
    return this.resolveWorker(name);
  }

  /** Get-or-create the checkout pool behind a pooled-scope resource. */
  private poolFor(name: string, def: ResourceDefinition): ResourcePool {
    let pool = this.pools.get(name);
    if (!pool) {
      const ctx: ResourceContext = {
        scope: "pooled",
        use: <T>(dep: string) => this.resolveWorkerDependency(dep, "pooled") as Promise<T>,
      };
      const counter = this.counter(name);
      pool = new ResourcePool(name, () => startFactory(def, ctx), def.dispose, def.pool ?? {}, {
        onCreated: () => {
          counter.created += 1;
        },
        onDisposed: () => {
          counter.disposed += 1;
        },
      });
      this.pools.set(name, pool);
    }
    return pool;
  }

  /** Begin a per-invocation task scope. */
  createTaskScope(): TaskScope {
    const taskCache = new Map<string, Promise<unknown>>();
    const taskTeardown: Teardown[] = [];

    const resolve = (
      name: string,
      building: ReadonlySet<string> = EMPTY_CHAIN,
    ): Promise<unknown> => {
      const def = this.defs.get(name);
      if (!def) {
        return Promise.reject(unregistered(name));
      }
      if (def.scope === "worker") {
        return this.resolveWorker(name); // shared singleton, even when first reached here
      }
      if (def.scope === "pooled") {
        const pending = taskCache.get(name);
        if (pending) {
          return pending; // one checkout per task, shared by every resolve
        }
        const pool = this.poolFor(name, def);
        // Cache the pending checkout BEFORE awaiting (see the task branch below)
        // so concurrent resolves within the task share a single checkout.
        const checkout = pool.acquire().catch((error) => {
          taskCache.delete(name); // failed checkout is retryable, not cached
          throw error;
        });
        taskCache.set(name, checkout);
        taskTeardown.push({
          name,
          run: async () => {
            let value: unknown;
            try {
              value = await checkout;
            } catch {
              return; // checkout failed — nothing to return to the pool
            }
            await pool.release(value); // return, don't dispose: the instance stays pooled
          },
        });
        return checkout;
      }
      if (def.scope === "request") {
        // Fresh on every resolve, never cached: N `useResource()` calls inside one
        // task yield N instances, each disposed with the task (LIFO, like the rest).
        if (building.has(name)) {
          return Promise.reject(
            new ResourceError(
              `resource "${name}": dependency cycle (${[...building, name].join(" -> ")})`,
            ),
          );
        }
        const chain = new Set(building).add(name);
        const ctx: ResourceContext = {
          scope: "request",
          use: <T>(dep: string) => resolve(dep, chain) as Promise<T>,
        };
        const counter = this.counter(name);
        counter.created += 1;
        const built = Promise.resolve()
          .then(() => startFactory(def, ctx))
          .catch((error) => {
            counter.created -= 1; // a failed build is not a live resource
            throw error;
          });
        this.trackResource(taskTeardown, name, def, built);
        return built;
      }
      // A name already in the chain is a cycle: returning its still-pending promise
      // would deadlock the build on itself rather than recurse, so reject instead.
      if (building.has(name)) {
        return Promise.reject(
          new ResourceError(
            `resource "${name}": dependency cycle (${[...building, name].join(" -> ")})`,
          ),
        );
      }
      const cached = taskCache.get(name);
      if (cached) {
        return cached;
      }
      const taskChain = new Set(building).add(name);
      const ctx: ResourceContext = {
        scope: "task",
        use: <T>(dep: string) => resolve(dep, taskChain) as Promise<T>,
      };
      const counter = this.counter(name);
      counter.created += 1;
      // Cache before the factory runs (see resolveWorker) so a self-referential
      // task resource reuses this promise instead of recursing.
      const built = Promise.resolve()
        .then(() => startFactory(def, ctx))
        .catch((error) => {
          counter.created -= 1; // a failed build is not a live resource
          taskCache.delete(name); // failed build is retryable, not cached
          throw error;
        });
      taskCache.set(name, built);
      this.trackResource(taskTeardown, name, def, built);
      return built;
    };

    const resolver: ResourceResolver = (name) => resolve(name);
    return { resolver, teardown: () => runTeardown(taskTeardown) };
  }

  /** Register a worker that shares this runtime's worker-scoped resources. */
  acquireWorker(): void {
    this.workerLeases += 1;
    // Start the shared checker on the first lease only — concurrent workers on
    // one runtime must not race recreate() on the same resource.
    if (this.workerLeases === 1) {
      this.healthChecker = new HealthChecker(this);
      this.healthChecker.start();
    }
    // Best-effort prewarm of pooled resources that asked for warm instances.
    // `prewarm` never rejects — build failures are logged inside the pool.
    for (const [name, def] of this.defs) {
      if (def.scope === "pooled" && (def.pool?.poolMin ?? 0) > 0) {
        void this.poolFor(name, def).prewarm();
      }
    }
  }

  /**
   * Release one worker lease; dispose worker-scoped resources (LIFO) and reset
   * the caches only once the last sharing worker has stopped. This keeps a
   * second `runWorker()` on the same queue from tearing down resources the
   * first worker is still using.
   */
  async teardownWorker(): Promise<void> {
    if (this.workerLeases > 0) {
      this.workerLeases -= 1;
    }
    if (this.workerLeases > 0) {
      return;
    }
    // Drain the checker first: once stop() resolves, no in-flight tick can
    // recreate a resource while the caches below are torn down.
    const checker = this.healthChecker;
    this.healthChecker = undefined;
    await checker?.stop();
    const pending = this.workerTeardown.splice(0);
    this.workerCache.clear();
    this.workerDeps.clear();
    // "Permanently unhealthy" is scoped to a worker run — the next worker
    // starts from scratch and may succeed where this one gave up.
    this.unhealthy.clear();
    // Shut pools down first — pooled instances may depend on worker resources.
    // `shutdown` never throws; per-instance dispose failures are logged.
    const pools = [...this.pools.values()];
    this.pools.clear();
    for (const pool of pools) {
      await pool.shutdown();
    }
    await runTeardown(pending);
  }

  // ── Health ────────────────────────────────────────────────────────────────

  /** Registered resource names, sorted — advertised on worker registration. */
  get names(): string[] {
    return [...this.defs.keys()].sort();
  }

  /** Definitions that asked for periodic health checks (interval > 0). */
  healthChecked(): ReadonlyMap<string, ResourceDefinition> {
    const out = new Map<string, ResourceDefinition>();
    for (const [name, def] of this.defs) {
      if (def.healthCheck && (def.healthCheckIntervalMs ?? 0) > 0) {
        out.set(name, def);
      }
    }
    return out;
  }

  /** Whether `name` was marked permanently unhealthy. */
  isUnhealthy(name: string): boolean {
    return this.unhealthy.has(name);
  }

  /** The built (possibly pending) worker instance, or undefined before first build. */
  builtWorkerInstance(name: string): Promise<unknown> | undefined {
    return this.workerCache.get(name);
  }

  /**
   * Mark a resource permanently unhealthy: every later resolve rejects with
   * {@link ResourceUnavailableError}. Terminal — there is no auto-recovery.
   */
  markUnhealthy(name: string): void {
    this.unhealthy.add(name);
    log.error(`resource "${name}" marked permanently unhealthy`);
  }

  /**
   * Dispose the current worker instance (best effort) and build a fresh one.
   * Returns false when the factory fails — the caller decides whether that
   * exhausts the recreation budget.
   */
  async recreate(name: string): Promise<boolean> {
    const def = this.defs.get(name);
    if (!def) {
      return false;
    }
    await this.disposeReplacedInstance(name, def);
    try {
      await this.resolveWorker(name);
      return true;
    } catch (error) {
      log.warn(() => `recreating resource "${name}" failed`, error);
      return false;
    }
  }

  /**
   * Hot-reload resources: dispose what is cached and rebuild on next use.
   * Returns `{name: success}`.
   *
   * `names` reloads exactly those (whatever their `reloadable` flag says);
   * omitting it sweeps every definition registered with `reloadable: true`.
   * Targets are ordered dependency-first — see {@link dependencyOrder}.
   */
  async reload(names?: readonly string[]): Promise<Record<string, boolean>> {
    const targets = new Set(
      names ?? [...this.defs].filter(([, def]) => def.reloadable).map(([name]) => name),
    );
    // A build in flight has not finished recording its `ctx.use` calls yet, so
    // ordering off `workerDeps` now could schedule a dependency after its
    // dependent. Let those builds settle first (a failed one just reloads).
    await Promise.allSettled(
      [...targets].map((name) => this.workerCache.get(name)).filter(Boolean),
    );
    const results: Record<string, boolean> = {};
    for (const name of this.dependencyOrder(targets)) {
      if (targets.has(name)) {
        results[name] = await this.reloadOne(name);
      }
    }
    return results;
  }

  /**
   * `targets` plus the resources they were built from, ordered so a resource
   * follows everything it used — rebuilding a dependency first means the
   * dependent resolves the fresh instance rather than the retired one.
   */
  private dependencyOrder(targets: Iterable<string>): string[] {
    const ordered: string[] = [];
    const done = new Set<string>();
    const visit = (name: string, chain: Set<string>): void => {
      // `chain` breaks dependency cycles; a self-referencing factory is legal.
      if (done.has(name) || chain.has(name)) {
        return;
      }
      chain.add(name);
      for (const dep of this.workerDeps.get(name) ?? []) {
        visit(dep, chain);
      }
      chain.delete(name);
      done.add(name);
      ordered.push(name);
    };
    for (const name of targets) {
      visit(name, new Set());
    }
    return ordered;
  }

  /** Reload one resource according to its scope. Unknown names report false. */
  private async reloadOne(name: string): Promise<boolean> {
    const def = this.defs.get(name);
    if (!def) {
      return false;
    }
    if (def.scope === "worker") {
      return this.recreate(name);
    }
    if (def.scope === "pooled") {
      // Drop the pool so the next checkout builds a fresh one; idle instances
      // are disposed now and checked-out ones on release.
      const pool = this.pools.get(name);
      this.pools.delete(name);
      await pool?.shutdown();
      return true;
    }
    // Task- and request-scoped resources are built per invocation — nothing is
    // cached to replace, so a reload is a successful no-op.
    return true;
  }

  /**
   * Per-resource health for the worker heartbeat, or undefined when nothing is
   * registered (so the heartbeat clears the column instead of writing `{}`).
   */
  healthSnapshot(): Record<string, HealthState> | undefined {
    if (this.defs.size === 0) {
      return undefined;
    }
    const out: Record<string, HealthState> = {};
    for (const name of this.defs.keys()) {
      out[name] = this.unhealthy.has(name) ? "unhealthy" : "healthy";
    }
    return out;
  }

  /**
   * Retire the currently-built worker instance ahead of recreation: drop it
   * from the cache, cancel its queued teardown (recreation disposes it now),
   * and run its `dispose` best-effort.
   */
  private async disposeReplacedInstance(name: string, def: ResourceDefinition): Promise<void> {
    const old = this.workerCache.get(name);
    if (!old) {
      return;
    }
    this.workerCache.delete(name);
    const staleIndex = this.workerTeardown.findIndex((entry) => entry.built === old);
    if (staleIndex !== -1) {
      this.workerTeardown.splice(staleIndex, 1);
    }
    try {
      const value = await old;
      await def.dispose?.(value);
      this.counter(name).disposed += 1;
    } catch (error) {
      log.debug(() => `disposing resource "${name}" before recreation failed`, error);
    }
  }

  /**
   * Queue a teardown thunk for a resource: run its `dispose` (if any) and record
   * the disposal. A build that failed is skipped — there's nothing to tear down.
   */
  private trackResource(
    stack: Teardown[],
    name: string,
    def: ResourceDefinition,
    built: Promise<unknown>,
  ): void {
    const { dispose } = def;
    const counter = this.counter(name);
    stack.push({
      name,
      built,
      run: async () => {
        let value: unknown;
        try {
          value = await built;
        } catch {
          return; // build failed (and was already un-counted) — nothing to dispose
        }
        if (dispose) {
          await dispose(value);
        }
        counter.disposed += 1;
      },
    });
  }
}

/** Run a factory, converting a synchronous throw into a rejected promise. */
function startFactory(def: ResourceDefinition, ctx: ResourceContext): Promise<unknown> {
  return new Promise((resolve) => resolve(def.factory(ctx)));
}

function unregistered(name: string): ResourceNotFoundError {
  return new ResourceNotFoundError(name);
}

/** Run disposal thunks in reverse (LIFO) order; surface the first error after all run. */
async function runTeardown(stack: Teardown[]): Promise<void> {
  let firstError: unknown;
  for (let i = stack.length - 1; i >= 0; i--) {
    try {
      await stack[i]?.run();
    } catch (error) {
      firstError ??= error;
    }
  }
  if (firstError !== undefined) {
    throw firstError;
  }
}
