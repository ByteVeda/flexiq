import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import {
  FlexiQError,
  Queue,
  ResourceUnavailableError,
  useResource,
  type Worker,
} from "../../src/index";
import { ResourcePool } from "../../src/resources/pool";
import { ResourceRuntime } from "../../src/resources/runtime";
import type { ResourceScope } from "../../src/resources/types";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-res-")), "q.db") });
}

async function waitFor(predicate: () => boolean, timeoutMs = 20_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

describe("ResourceRuntime", () => {
  it("builds a worker-scoped resource once across many resolutions", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("cfg", {
      scope: "worker",
      factory: () => {
        builds += 1;
        return { n: builds };
      },
    });
    const a = rt.createTaskScope();
    const b = rt.createTaskScope();
    expect(await a.resolver("cfg")).toEqual({ n: 1 });
    expect(await a.resolver("cfg")).toEqual({ n: 1 });
    expect(await b.resolver("cfg")).toEqual({ n: 1 });
    expect(builds).toBe(1);
  });

  it("re-registering a worker resource replaces the built instance", async () => {
    const rt = new ResourceRuntime();
    rt.register("cfg", { scope: "worker", factory: () => ({ version: 1 }) });
    const scope = rt.createTaskScope();
    expect(await scope.resolver("cfg")).toEqual({ version: 1 });

    rt.register("cfg", { scope: "worker", factory: () => ({ version: 2 }) });
    expect(await scope.resolver("cfg")).toEqual({ version: 2 });
  });

  it("builds a task-scoped resource once per scope, fresh across scopes", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("conn", {
      scope: "task",
      factory: () => {
        builds += 1;
        return builds;
      },
    });
    const a = rt.createTaskScope();
    expect(await a.resolver("conn")).toBe(1);
    expect(await a.resolver("conn")).toBe(1); // same scope → cached
    const b = rt.createTaskScope();
    expect(await b.resolver("conn")).toBe(2); // new scope → rebuilt
    expect(builds).toBe(2);
  });

  it("disposes worker-scoped resources LIFO on teardown", async () => {
    const rt = new ResourceRuntime();
    const order: string[] = [];
    rt.register("a", { scope: "worker", factory: () => "a", dispose: () => void order.push("a") });
    rt.register("b", { scope: "worker", factory: () => "b", dispose: () => void order.push("b") });
    const s = rt.createTaskScope();
    await s.resolver("a");
    await s.resolver("b");
    await rt.teardownWorker();
    expect(order).toEqual(["b", "a"]); // reverse of build order
  });

  it("disposes worker resources only when the last sharing worker stops", async () => {
    const rt = new ResourceRuntime();
    let disposed = 0;
    rt.register("conn", {
      scope: "worker",
      factory: () => "c",
      dispose: () => {
        disposed += 1;
      },
    });
    rt.acquireWorker();
    rt.acquireWorker();
    await rt.createTaskScope().resolver("conn");

    await rt.teardownWorker(); // one of two workers stopped
    expect(disposed).toBe(0); // still in use by the sibling worker

    await rt.teardownWorker(); // last worker stopped
    expect(disposed).toBe(1);
  });

  it("disposes task-scoped resources LIFO when the scope ends", async () => {
    const rt = new ResourceRuntime();
    const order: string[] = [];
    rt.register("x", { scope: "task", factory: () => "x", dispose: () => void order.push("x") });
    rt.register("y", { scope: "task", factory: () => "y", dispose: () => void order.push("y") });
    const s = rt.createTaskScope();
    await s.resolver("x");
    await s.resolver("y");
    await s.teardown();
    expect(order).toEqual(["y", "x"]);
  });

  it("lets a factory depend on another resource via ctx.use", async () => {
    const rt = new ResourceRuntime();
    rt.register("base", { scope: "worker", factory: () => 10 });
    rt.register("derived", {
      scope: "worker",
      factory: async (ctx) => (await ctx.use<number>("base")) * 2,
    });
    const s = rt.createTaskScope();
    expect(await s.resolver("derived")).toBe(20);
  });

  it("rejects a worker factory depending on a task-scoped resource", async () => {
    const rt = new ResourceRuntime();
    rt.register("perJob", { scope: "task", factory: () => "x" });
    rt.register("singleton", {
      scope: "worker",
      factory: (ctx) => ctx.use("perJob"),
    });
    const s = rt.createTaskScope();
    await expect(s.resolver("singleton")).rejects.toThrow(/task-scoped/);
  });

  it("throws for an unregistered resource", async () => {
    const rt = new ResourceRuntime();
    const s = rt.createTaskScope();
    await expect(s.resolver("nope")).rejects.toThrow(/No resource registered/);
  });

  it("retries a failed build instead of caching the rejection", async () => {
    const rt = new ResourceRuntime();
    let attempts = 0;
    rt.register("flaky", {
      scope: "worker",
      factory: () => {
        attempts += 1;
        if (attempts === 1) {
          throw new Error("transient");
        }
        return "ok";
      },
    });
    const s = rt.createTaskScope();
    await expect(s.resolver("flaky")).rejects.toThrow("transient");
    expect(await s.resolver("flaky")).toBe("ok"); // second attempt succeeds
  });
});

describe("ResourcePool", () => {
  it("returns a released instance to the next acquire", async () => {
    let builds = 0;
    const pool = new ResourcePool(
      "db",
      async () => {
        builds += 1;
        return { id: builds };
      },
      undefined,
      {},
    );
    const first = await pool.acquire();
    await pool.release(first);
    const second = await pool.acquire();
    expect(second).toBe(first); // same instance came back
    expect(builds).toBe(1);
  });

  it("times out with ResourceUnavailableError when the pool is exhausted", async () => {
    const pool = new ResourcePool("db", async () => ({}), undefined, {
      poolSize: 1,
      acquireTimeoutMs: 30,
    });
    await pool.acquire(); // takes the only slot
    await expect(pool.acquire()).rejects.toThrow(ResourceUnavailableError);
    await expect(pool.acquire()).rejects.toThrow(/Resource "db" pool timed out/);
    expect(pool.stats().totalTimeouts).toBe(2);
  });

  it("does not leak capacity when the factory rejects", async () => {
    let attempts = 0;
    const pool = new ResourcePool(
      "db",
      async () => {
        attempts += 1;
        if (attempts === 1) {
          throw new Error("connect failed");
        }
        return "ok";
      },
      undefined,
      { poolSize: 1, acquireTimeoutMs: 50 },
    );
    await expect(pool.acquire()).rejects.toThrow("connect failed");
    expect(pool.stats().active).toBe(0); // never counted as checked out
    expect(await pool.acquire()).toBe("ok"); // permit was returned, not leaked
  });

  it("prewarm builds poolMin instances eagerly", async () => {
    let builds = 0;
    const pool = new ResourcePool(
      "db",
      async () => {
        builds += 1;
        return builds;
      },
      undefined,
      { poolMin: 2 },
    );
    await pool.prewarm();
    expect(builds).toBe(2);
    expect(pool.stats().idle).toBe(2);
  });

  it("evicts an idle instance past maxLifetime and builds a fresh one", async () => {
    let builds = 0;
    const disposed: number[] = [];
    const pool = new ResourcePool(
      "db",
      async () => {
        builds += 1;
        return builds;
      },
      (value) => void disposed.push(value as number),
      { maxLifetimeMs: 20 },
    );
    const first = await pool.acquire();
    await pool.release(first);
    await new Promise((resolve) => setTimeout(resolve, 40)); // let it expire
    const second = await pool.acquire();
    expect(second).toBe(2); // freshly built
    expect(disposed).toEqual([1]); // expired instance was disposed
  });

  it("stats reflect active, idle, acquisitions, and timeouts", async () => {
    const pool = new ResourcePool("db", async () => ({}), undefined, {
      poolSize: 2,
      acquireTimeoutMs: 20,
    });
    const a = await pool.acquire();
    const b = await pool.acquire();
    await expect(pool.acquire()).rejects.toThrow(ResourceUnavailableError);
    expect(pool.stats()).toEqual({
      size: 2,
      active: 2,
      idle: 0,
      totalAcquisitions: 2,
      totalTimeouts: 1,
    });
    await pool.release(a);
    expect(pool.stats().active).toBe(1);
    expect(pool.stats().idle).toBe(1);
    await pool.release(b);
    expect(pool.stats().active).toBe(0);
    expect(pool.stats().idle).toBe(2);
  });
});

describe("request-scoped resources in the runtime", () => {
  it("builds a fresh instance per resolve and disposes each with the task", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    const disposed: number[] = [];
    rt.register("cursor", {
      scope: "request",
      factory: () => ({ id: ++builds }),
      dispose: (value) => {
        disposed.push((value as { id: number }).id);
      },
    });
    const scope = rt.createTaskScope();
    const first = await scope.resolver("cursor");
    const second = await scope.resolver("cursor");
    // Unlike task scope, a second resolve is not the cached first one.
    expect(first).not.toBe(second);
    expect(builds).toBe(2);
    await scope.teardown();
    expect(disposed.sort()).toEqual([1, 2]);
    expect(rt.metrics().cursor).toEqual({ created: 2, disposed: 2, active: 0 });
  });

  it("caches a task-scoped resource for the whole task, unlike request scope", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("perTask", { scope: "task", factory: () => ({ id: ++builds }) });
    const scope = rt.createTaskScope();
    expect(await scope.resolver("perTask")).toBe(await scope.resolver("perTask"));
    expect(builds).toBe(1);
    await scope.teardown();
  });
});

describe("resource scope validation", () => {
  it("rejects an unknown scope at registration", () => {
    const rt = new ResourceRuntime();
    // TypeScript stops this at compile time; a plain-JS caller would otherwise
    // land in the task branch and get different lifecycle semantics silently.
    expect(() =>
      rt.register("bad", { scope: "per-request" as ResourceScope, factory: () => 1 }),
    ).toThrow(/unknown scope/);
  });

  it("rejects a direct request-scope dependency cycle", async () => {
    const rt = new ResourceRuntime();
    rt.register("a", { scope: "request", factory: (ctx) => ctx.use("a") });
    const scope = rt.createTaskScope();
    await expect(scope.resolver("a")).rejects.toThrow(/dependency cycle/);
    await scope.teardown();
  });

  it("rejects an indirect request-scope cycle through a task resource", async () => {
    const rt = new ResourceRuntime();
    rt.register("a", { scope: "request", factory: (ctx) => ctx.use("b") });
    rt.register("b", { scope: "task", factory: (ctx) => ctx.use("a") });
    const scope = rt.createTaskScope();
    await expect(scope.resolver("a")).rejects.toThrow(/dependency cycle/);
    await scope.teardown();
  });

  it("rejects the reverse mixed-scope cycle, task through request", async () => {
    // The task branch would otherwise hand back its own pending promise and the
    // build would await itself forever.
    const rt = new ResourceRuntime();
    rt.register("a", { scope: "task", factory: (ctx) => ctx.use("b") });
    rt.register("b", { scope: "request", factory: (ctx) => ctx.use("a") });
    const scope = rt.createTaskScope();
    await expect(scope.resolver("a")).rejects.toThrow(/dependency cycle/);
    await scope.teardown();
  });

  it("rejects a self-referential task resource", async () => {
    const rt = new ResourceRuntime();
    rt.register("a", { scope: "task", factory: (ctx) => ctx.use("a") });
    const scope = rt.createTaskScope();
    await expect(scope.resolver("a")).rejects.toThrow(/dependency cycle/);
    await scope.teardown();
  });

  it("still shares one task instance across sibling dependents", async () => {
    // A diamond is not a cycle: the chain is per-path, so the cache still serves
    // the second dependent.
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("shared", { scope: "task", factory: () => ({ n: ++builds }) });
    rt.register("x", { scope: "task", factory: (ctx) => ctx.use("shared") });
    rt.register("y", { scope: "task", factory: (ctx) => ctx.use("shared") });
    const scope = rt.createTaskScope();
    expect(await scope.resolver("x")).toBe(await scope.resolver("y"));
    expect(builds).toBe(1);
    await scope.teardown();
  });
});

describe("pooled resources in the runtime", () => {
  it("checks out one pooled instance per task and returns it on teardown", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("conn", {
      scope: "pooled",
      factory: () => {
        builds += 1;
        return { id: builds };
      },
      pool: { poolSize: 1 },
    });
    const a = rt.createTaskScope();
    const first = await a.resolver("conn");
    expect(await a.resolver("conn")).toBe(first); // one checkout per task
    await a.teardown(); // released back to the pool, not disposed
    const b = rt.createTaskScope();
    expect(await b.resolver("conn")).toBe(first); // reused across tasks
    expect(builds).toBe(1);
    expect(rt.metrics().conn).toEqual({ created: 1, disposed: 0, active: 1 });
  });

  it("rejects a pooled factory depending on a task-scoped resource", async () => {
    const rt = new ResourceRuntime();
    rt.register("perJob", { scope: "task", factory: () => "x" });
    rt.register("conn", {
      scope: "pooled",
      factory: (ctx) => ctx.use("perJob"),
    });
    const scope = rt.createTaskScope();
    await expect(scope.resolver("conn")).rejects.toThrow(
      /task-scoped and cannot be resolved at pooled scope/,
    );
  });

  it("lets a pooled factory depend on a worker-scoped resource", async () => {
    const rt = new ResourceRuntime();
    rt.register("base", { scope: "worker", factory: () => 10 });
    rt.register("conn", {
      scope: "pooled",
      factory: async (ctx) => (await ctx.use<number>("base")) * 2,
    });
    const scope = rt.createTaskScope();
    expect(await scope.resolver("conn")).toBe(20);
  });

  it("prewarms pooled instances when a worker acquires the runtime", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("conn", {
      scope: "pooled",
      factory: () => {
        builds += 1;
        return builds;
      },
      pool: { poolMin: 2 },
    });
    rt.acquireWorker();
    expect(await waitFor(() => builds === 2)).toBe(true);
    await rt.teardownWorker();
  });

  it("disposes idle pooled instances at worker teardown", async () => {
    const rt = new ResourceRuntime();
    let disposals = 0;
    rt.register("conn", {
      scope: "pooled",
      factory: () => ({}),
      dispose: () => {
        disposals += 1;
      },
    });
    rt.acquireWorker();
    const scope = rt.createTaskScope();
    await scope.resolver("conn");
    await scope.teardown();
    await rt.teardownWorker();
    expect(disposals).toBe(1);
    expect(rt.metrics().conn).toEqual({ created: 1, disposed: 1, active: 0 });
  });
});

describe("resource injection in a worker", () => {
  it("resolves a worker-scoped resource via useResource(), shared across jobs", async () => {
    const queue = newQueue();
    let builds = 0;
    let disposed = false;
    queue.resource(
      "config",
      () => {
        builds += 1;
        return { token: "secret" };
      },
      {
        dispose: () => {
          disposed = true;
        },
      },
    );
    const seen: string[] = [];
    queue.task("read", async () => {
      const cfg = await useResource<{ token: string }>("config");
      seen.push(cfg.token);
    });

    queue.enqueue("read");
    queue.enqueue("read");
    worker = queue.runWorker();

    expect(await waitFor(() => seen.length === 2)).toBe(true);
    expect(seen).toEqual(["secret", "secret"]);
    expect(builds).toBe(1); // singleton across both jobs

    worker.stop();
    worker = undefined;
    expect(await waitFor(() => disposed)).toBe(true);
  });

  it("awaiting stop() resolves only once worker-scoped disposal finished", async () => {
    const queue = newQueue();
    let disposed = false;
    queue.resource("config", () => ({ token: "secret" }), {
      dispose: async () => {
        await new Promise((done) => setTimeout(done, 50));
        disposed = true;
      },
    });
    const seen: string[] = [];
    queue.task("read", async () => {
      const cfg = await useResource<{ token: string }>("config");
      seen.push(cfg.token);
    });

    queue.enqueue("read");
    worker = queue.runWorker();
    expect(await waitFor(() => seen.length === 1)).toBe(true);

    await worker.stop(); // no polling needed — teardown is done when this resolves
    worker = undefined;
    expect(disposed).toBe(true);
  });

  it("injects declared resources as a trailing deps argument", async () => {
    const queue = newQueue();
    queue.resource("db", () => ({ query: () => 7 }));
    let got = 0;
    queue.task(
      "use-db",
      (factor: number, deps: { db: { query: () => number } }) => {
        got = deps.db.query() * factor;
      },
      { inject: ["db"] },
    );

    queue.enqueue("use-db", [3]);
    worker = queue.runWorker();

    expect(await waitFor(() => got !== 0)).toBe(true);
    expect(got).toBe(21);
  });

  it("builds and disposes a task-scoped resource per job", async () => {
    const queue = newQueue();
    let builds = 0;
    let disposals = 0;
    queue.resource(
      "tx",
      () => {
        builds += 1;
        return builds;
      },
      {
        scope: "task",
        dispose: () => {
          disposals += 1;
        },
      },
    );
    queue.task("work", async () => {
      await useResource("tx");
    });

    queue.enqueue("work");
    queue.enqueue("work");
    worker = queue.runWorker();

    expect(await waitFor(() => disposals === 2)).toBe(true);
    expect(builds).toBe(2); // one per invocation
  });

  it("reuses one pooled instance across jobs through a worker", async () => {
    const queue = newQueue();
    let builds = 0;
    const seen: number[] = [];
    queue.resource(
      "conn",
      () => {
        builds += 1;
        return { id: builds };
      },
      { scope: "pooled", pool: { poolSize: 1 } },
    );
    queue.task("hit", async () => {
      const conn = await useResource<{ id: number }>("conn");
      seen.push(conn.id);
    });

    queue.enqueue("hit");
    queue.enqueue("hit");
    worker = queue.runWorker();

    expect(await waitFor(() => seen.length === 2)).toBe(true);
    expect(seen).toEqual([1, 1]); // same instance both times
    expect(builds).toBe(1);
  });

  it("disposes pooled instances when the worker stops", async () => {
    const queue = newQueue();
    let disposals = 0;
    let done = false;
    queue.resource("conn", () => ({}), {
      scope: "pooled",
      dispose: () => {
        disposals += 1;
      },
      pool: { poolSize: 1 },
    });
    queue.task("touch", async () => {
      await useResource("conn");
      done = true;
    });

    queue.enqueue("touch");
    worker = queue.runWorker();
    expect(await waitFor(() => done)).toBe(true);

    worker.stop();
    worker = undefined;
    expect(await waitFor(() => disposals === 1)).toBe(true);
  });

  it("fails a job whose pooled factory uses a task-scoped resource", async () => {
    const queue = newQueue();
    queue.resource("perJob", () => "x", { scope: "task" });
    queue.resource("conn", (ctx) => ctx.use("perJob"), { scope: "pooled" });
    let failure = "";
    queue.task("guarded", async () => {
      try {
        await useResource("conn");
      } catch (error) {
        failure = (error as Error).message;
      }
    });

    queue.enqueue("guarded");
    worker = queue.runWorker();

    expect(await waitFor(() => failure !== "")).toBe(true);
    expect(failure).toMatch(/task-scoped and cannot be resolved at pooled scope/);
  });

  it("rejects pool options on a non-pooled scope", () => {
    const queue = newQueue();
    expect(() => queue.resource("bad", () => 1, { pool: { poolSize: 2 } })).toThrow(
      /pool options require scope "pooled"/,
    );
  });

  it("throws when useResource is called outside a task", () => {
    expect(() => useResource("anything")).toThrow(FlexiQError);
  });

  it("strips the injected deps arg from typed enqueue", () => {
    const q = newQueue().task("inj", (id: string, deps: { db: number }) => `${id}:${deps.db}`, {
      inject: ["db"],
    });
    // Type-only: never executed. Asserts `deps` is not part of the enqueue args.
    const _typeCheck = () => {
      q.enqueue("inj", ["a"]);
      // @ts-expect-error deps must not be passed as an enqueue arg
      q.enqueue("inj", ["a", { db: 1 }]);
    };
    void _typeCheck;
    expect(true).toBe(true);
  });
});
