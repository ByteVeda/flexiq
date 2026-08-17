import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

// A standalone mesh node (no seeds) still routes its own jobs through the bridge:
// scheduler -> affinity-sorted local deque -> dispatcher. Stealing is a no-op
// without peers, so a job must complete exactly as on the plain path.
it("processes jobs through the mesh bridge on a single node", async () => {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-mesh-")), "queue.db");
  const queue = new Queue({ dbPath });
  queue.task("add", (a: number, b: number) => a + b);

  const id = queue.enqueue("add", [2, 3]);
  worker = queue.runWorker({
    queues: ["default"],
    mesh: { port: 17_946, seeds: [], steal: true },
  });

  const result = await queue.result(id, { timeoutMs: 8000 });
  expect(result).toBe(5);
});
