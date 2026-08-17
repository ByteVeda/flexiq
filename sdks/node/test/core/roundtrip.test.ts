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

it("enqueues, runs a task in a node worker, and reads the result back", async () => {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
  const queue = new Queue({ dbPath });
  queue.task("add", (a: number, b: number) => a + b);

  const id = queue.enqueue("add", [2, 3]);
  worker = queue.runWorker({ queues: ["default"] });

  const result = await waitFor(() => queue.getResult(id));
  expect(result).toBe(5);
});

async function waitFor<T>(read: () => T | undefined, timeoutMs = 5000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = read();
    if (value !== undefined) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("timed out waiting for job result");
}
