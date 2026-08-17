import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { Queue, useResource } from "../../src/index";
import { ResourceRuntime } from "../../src/resources/runtime";

async function waitFor(predicate: () => boolean, timeoutMs = 4000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

describe("ResourceRuntime.reload", () => {
  it("sweeps only resources flagged reloadable", async () => {
    const rt = new ResourceRuntime();
    rt.register("cfg", { scope: "worker", factory: () => ({}), reloadable: true });
    rt.register("pinned", { scope: "worker", factory: () => ({}) });

    expect(await rt.reload()).toEqual({ cfg: true });
  });

  it("reloads named resources whatever their reloadable flag says", async () => {
    const rt = new ResourceRuntime();
    rt.register("pinned", { scope: "worker", factory: () => ({}) });

    expect(await rt.reload(["pinned"])).toEqual({ pinned: true });
  });

  it("disposes the old worker instance and builds a fresh one", async () => {
    const rt = new ResourceRuntime();
    const disposed: number[] = [];
    let builds = 0;
    rt.register("conn", {
      scope: "worker",
      reloadable: true,
      factory: () => {
        builds += 1;
        return builds;
      },
      dispose: (value) => void disposed.push(value as number),
    });
    const scope = rt.createTaskScope();
    expect(await scope.resolver("conn")).toBe(1);

    expect(await rt.reload()).toEqual({ conn: true });
    expect(disposed).toEqual([1]);
    expect(await rt.createTaskScope().resolver("conn")).toBe(2);
  });

  it("reloads dependencies before the resources that resolved them", async () => {
    const rt = new ResourceRuntime();
    const order: string[] = [];
    rt.register("base", {
      scope: "worker",
      reloadable: true,
      factory: () => {
        order.push("base");
        return "base";
      },
    });
    rt.register("derived", {
      scope: "worker",
      reloadable: true,
      factory: async (ctx) => {
        await ctx.use("base");
        order.push("derived");
        return "derived";
      },
    });
    await rt.createTaskScope().resolver("derived");
    order.length = 0;

    await rt.reload();
    expect(order).toEqual(["base", "derived"]);
  });

  it("reports false for a failing factory and for an unknown name", async () => {
    const rt = new ResourceRuntime();
    rt.register("broken", {
      scope: "worker",
      factory: () => {
        throw new Error("boom");
      },
    });

    expect(await rt.reload(["broken", "ghost"])).toEqual({ broken: false, ghost: false });
  });

  it("drops a pooled resource so the next checkout builds a fresh instance", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    const disposed: number[] = [];
    rt.register("pooled", {
      scope: "pooled",
      reloadable: true,
      factory: () => {
        builds += 1;
        return builds;
      },
      dispose: (value) => void disposed.push(value as number),
    });
    const first = rt.createTaskScope();
    expect(await first.resolver("pooled")).toBe(1);
    await first.teardown(); // returns the instance to the pool

    expect(await rt.reload()).toEqual({ pooled: true });
    expect(disposed).toEqual([1]);
    expect(await rt.createTaskScope().resolver("pooled")).toBe(2);
  });

  it("treats per-invocation scopes as a successful no-op", async () => {
    const rt = new ResourceRuntime();
    let builds = 0;
    rt.register("perTask", {
      scope: "task",
      reloadable: true,
      factory: () => {
        builds += 1;
        return builds;
      },
    });

    expect(await rt.reload()).toEqual({ perTask: true });
    expect(builds).toBe(0);
  });
});

describe("Queue.reloadResources", () => {
  it("rebuilds a reloadable resource a running worker then sees", async () => {
    const queue = new Queue({
      dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-reload-")), "q.db"),
    });
    let version = 1;
    queue.resource("cfg", () => ({ version }), { reloadable: true });
    const seen: number[] = [];
    queue.task("readCfg", async () => {
      const cfg = await useResource<{ version: number }>("cfg");
      seen.push(cfg.version);
    });

    const worker = queue.runWorker();
    try {
      queue.enqueue("readCfg");
      expect(await waitFor(() => seen.length === 1)).toBe(true);

      version = 2;
      expect(await queue.reloadResources()).toEqual({ cfg: true });

      queue.enqueue("readCfg");
      expect(await waitFor(() => seen.length === 2)).toBe(true);
      expect(seen).toEqual([1, 2]);
    } finally {
      await worker.stop();
    }
  });
});
