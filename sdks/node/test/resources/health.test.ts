import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { Queue, ResourceUnavailableError, type Worker } from "../../src/index";
import { HealthChecker } from "../../src/resources/health";
import { ResourceRuntime } from "../../src/resources/runtime";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-health-")), "q.db") });
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

describe("HealthChecker", () => {
  it("recreates a resource whose health check fails", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("svc", {
      scope: "worker",
      factory: () => ({ id: ++builds }),
      healthCheck: () => false,
      healthCheckIntervalMs: 100,
      // With successful recreates the fail count must keep resetting, so even
      // many failed checks past this budget never mark the resource unhealthy.
      maxRecreationAttempts: 1,
    });
    const first = await rt.createTaskScope().resolver("svc");

    const checker = new HealthChecker(rt);
    checker.start();
    expect(await waitFor(() => builds >= 3, 8000)).toBe(true);
    await checker.stop();

    expect(rt.isUnhealthy("svc")).toBe(false); // recreation succeeded every time
    const replaced = await rt.createTaskScope().resolver("svc");
    expect(replaced).not.toBe(first); // fresh instance, new identity
    expect(rt.metrics().svc?.created).toBeGreaterThanOrEqual(3);
    expect(rt.metrics().svc?.disposed).toBeGreaterThanOrEqual(2); // retired builds
  });

  it("marks a resource unhealthy once recreation attempts are exhausted", async () => {
    const rt = new ResourceRuntime();
    let calls = 0;
    rt.register("svc", {
      scope: "worker",
      factory: () => {
        calls += 1;
        if (calls > 1) {
          throw new Error("factory broken");
        }
        return "svc_v1";
      },
      healthCheck: () => false,
      healthCheckIntervalMs: 50,
      maxRecreationAttempts: 2,
    });
    expect(await rt.createTaskScope().resolver("svc")).toBe("svc_v1");

    const checker = new HealthChecker(rt);
    checker.start();
    expect(await waitFor(() => rt.isUnhealthy("svc"), 8000)).toBe(true);
    await checker.stop();

    // Terminal: resolving now (and forever) rejects.
    await expect(rt.createTaskScope().resolver("svc")).rejects.toThrow(ResourceUnavailableError);
    await expect(rt.createTaskScope().resolver("svc")).rejects.toThrow(/permanently unhealthy/);
  });

  it("treats a throwing health check as unhealthy and recreates", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("svc", {
      scope: "worker",
      factory: () => ++builds,
      healthCheck: () => {
        throw new Error("check exploded");
      },
      healthCheckIntervalMs: 50,
    });
    await rt.createTaskScope().resolver("svc");

    const checker = new HealthChecker(rt);
    checker.start();
    expect(await waitFor(() => builds >= 2, 8000)).toBe(true); // recreate path ran
    await checker.stop();
    expect(rt.isUnhealthy("svc")).toBe(false); // recreation kept succeeding
  });

  it("start() is a no-op without health-checked resources; stop() is idempotent", async () => {
    const rt = new ResourceRuntime();
    rt.register("plain", { scope: "worker", factory: () => 1 });
    rt.register("disabled", {
      scope: "worker",
      factory: () => 2,
      healthCheck: () => true,
      healthCheckIntervalMs: 0, // 0 = disabled
    });
    const checker = new HealthChecker(rt);
    checker.start(); // no timer started — nothing asked for checking
    await checker.stop();
    await checker.stop(); // safe without a timer, and repeatedly
    checker.start(); // terminal: a stopped checker never restarts
    await checker.stop();
  });

  it("stop() drains an in-flight tick and blocks recreation after it", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    let releaseCheck: ((healthy: boolean) => void) | undefined;
    rt.register("svc", {
      scope: "worker",
      factory: () => ++builds,
      healthCheck: () =>
        new Promise<boolean>((resolve) => {
          releaseCheck = resolve;
        }),
      healthCheckIntervalMs: 50,
    });
    await rt.createTaskScope().resolver("svc");

    const checker = new HealthChecker(rt);
    checker.start();
    expect(await waitFor(() => releaseCheck !== undefined, 8000)).toBe(true); // check in flight

    const stopping = checker.stop(); // must wait for the pending check
    let drained = false;
    void stopping.then(() => {
      drained = true;
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(drained).toBe(false); // still blocked on the in-flight check

    releaseCheck?.(false); // fails — but shutdown already began
    await stopping;
    expect(builds).toBe(1); // no recreate ran into the teardown window
  });

  it("rejects health checks on task- and pooled-scoped resources", () => {
    const queue = newQueue();
    expect(() =>
      queue.resource("perJob", () => 1, {
        scope: "task",
        healthCheck: () => true,
        healthCheckIntervalMs: 100,
      }),
    ).toThrow(/health checks require scope "worker"/);
    expect(() =>
      queue.resource("conn", () => 1, {
        scope: "pooled",
        healthCheck: () => true,
        healthCheckIntervalMs: 100,
      }),
    ).toThrow(/health checks require scope "worker"/);
  });

  it("advertises resource health through the worker heartbeat", async () => {
    const queue = newQueue();
    queue.resource("db", () => ({ ok: true }), {
      healthCheck: (value) => value.ok,
      healthCheckIntervalMs: 100,
    });
    queue.task("noop", () => undefined);

    worker = queue.runWorker();
    let row: { resources?: string; resourceHealth?: string } | undefined;
    const deadline = Date.now() + 12000;
    while (Date.now() < deadline && row === undefined) {
      const workers = await queue.listWorkers();
      row = workers.find((w) => w.resourceHealth?.includes('"healthy"'));
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    expect(row).toBeDefined();
    expect(JSON.parse(row?.resourceHealth ?? "{}")).toEqual({ db: "healthy" });
    expect(JSON.parse(row?.resources ?? "[]")).toEqual(["db"]);

    worker.stop();
    worker = undefined;
    // Unregistration still runs on stop — the row disappears instead of flapping.
    let gone = false;
    const stopDeadline = Date.now() + 8000;
    while (Date.now() < stopDeadline && !gone) {
      gone = (await queue.listWorkers()).length === 0;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    expect(gone).toBe(true);
  });
});
