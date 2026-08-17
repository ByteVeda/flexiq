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

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-ls-")), "q.db") });
}

async function waitFor(predicate: () => Promise<boolean>, timeoutMs = 14000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("filters jobs by metadata substring", async () => {
  const queue = newQueue();
  queue.task("tagged", (n: number) => n);

  await queue.enqueue("tagged", [1], { metadata: "tenant=acme" });
  await queue.enqueue("tagged", [2], { metadata: "tenant=globex" });

  const acme = await queue.listJobsFiltered({ metadataLike: "acme" });
  expect(acme).toHaveLength(1);
  expect(acme[0]?.metadata).toBe("tenant=acme");

  // The narrower listJobs has no metadata predicate, so it sees both.
  expect(await queue.listJobs({ task: "tagged" })).toHaveLength(2);
});

it("rejects an unknown status on the wider filter too", async () => {
  const queue = newQueue();
  await expect(queue.listJobsFiltered({ status: "nonsense" })).rejects.toThrow(/unknown status/);
});

it("lists a completed job from the archive", async () => {
  // A terminal job is archived as it completes — the live `jobs` table only
  // ever holds pending/running rows — so the archive is where a finished job
  // is read back from, and purgeCompleted deletes from there rather than
  // filling it.
  const queue = newQueue();
  queue.task("done", (n: number) => n);
  await queue.enqueue("done", [1]);

  worker = queue.runWorker({ queues: ["default"], concurrency: 2 });
  expect(await waitFor(async () => (await queue.stats()).completed >= 1)).toBe(true);
  worker.stop();
  worker = undefined;

  const archived = await queue.listArchived();
  expect(archived).toHaveLength(1);
  expect(archived[0]?.taskName).toBe("done");
});
