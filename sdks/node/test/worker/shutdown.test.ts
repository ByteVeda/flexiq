import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { Queue, useResource } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "taskito-shutdown-")), "q.db") });
}

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

describe("Queue.shutdown", () => {
  it("resolves when no worker is running", async () => {
    await expect(newQueue().shutdown()).resolves.toBeUndefined();
  });

  it("stops a running worker and disposes its resources", async () => {
    const queue = newQueue();
    let disposed = false;
    queue.resource("conn", () => ({ open: true }), {
      dispose: () => {
        disposed = true;
      },
    });
    const seen: number[] = [];
    queue.task("touch", async () => {
      await useResource("conn");
      seen.push(1);
    });

    queue.runWorker();
    queue.enqueue("touch");
    expect(await waitFor(() => seen.length === 1)).toBe(true);

    await queue.shutdown();
    expect(disposed).toBe(true);

    // Dispatch really stopped: a job enqueued after shutdown stays pending.
    const id = queue.enqueue("touch");
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(queue.getJob(id)?.status).toBe("pending");
  });

  it("stops every worker started from the queue", async () => {
    const queue = newQueue();
    queue.task("noop", () => undefined);
    const first = queue.runWorker();
    const second = queue.runWorker();

    await queue.shutdown();

    const id = queue.enqueue("noop");
    await new Promise((resolve) => setTimeout(resolve, 300));
    expect(queue.getJob(id)?.status).toBe("pending");
    // Stopping again is a no-op, not a second resource-lease release.
    await expect(first.stop()).resolves.toBeUndefined();
    await expect(second.stop()).resolves.toBeUndefined();
  });

  it("a repeated stop leaves a sibling worker's resources alone", async () => {
    const queue = newQueue();
    let disposals = 0;
    queue.resource("conn", () => ({}), {
      dispose: () => {
        disposals += 1;
      },
    });
    queue.task("touch", async () => {
      await useResource("conn");
    });

    const first = queue.runWorker();
    queue.runWorker();
    queue.enqueue("touch");
    expect(await waitFor(() => queue.resourceMetrics().conn?.active === 1)).toBe(true);

    await first.stop();
    await first.stop(); // repeated: must not release the second worker's lease
    expect(disposals).toBe(0);

    await queue.shutdown();
    expect(disposals).toBe(1);
  });
});
