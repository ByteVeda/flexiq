import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { type OutcomeEvent, Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-retryon-")), "q.db") });
}

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

/** A permanent failure the predicate rejects. */
class PermanentError extends Error {}

it("dead-letters a rejected error without spending the retry budget", async () => {
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];
  const retries: OutcomeEvent[] = [];
  let attempts = 0;

  queue.on("job.dead", (event) => dead.push(event));
  queue.on("job.retrying", (event) => retries.push(event));
  queue.task(
    "permanent",
    () => {
      attempts += 1;
      throw new PermanentError("malformed input");
    },
    { maxRetries: 5, retryOn: (error) => !(error instanceof PermanentError) },
  );

  queue.enqueue("permanent");
  worker = queue.runWorker();

  expect(await waitFor(() => dead.length > 0)).toBe(true);
  expect(attempts).toBe(1);
  expect(retries).toHaveLength(0);
});

it("still retries an error the predicate accepts", async () => {
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];
  let attempts = 0;

  queue.on("job.dead", (event) => dead.push(event));
  queue.task(
    "transient",
    () => {
      attempts += 1;
      throw new Error("connection reset");
    },
    {
      maxRetries: 2,
      retryBackoff: { baseMs: 1, maxMs: 5 },
      retryOn: (error) => !(error instanceof PermanentError),
    },
  );

  queue.enqueue("transient");
  worker = queue.runWorker();

  expect(await waitFor(() => dead.length > 0)).toBe(true);
  expect(attempts).toBe(3); // first run plus both retries
});

it("retries when the predicate itself throws", async () => {
  const queue = newQueue();
  const dead: OutcomeEvent[] = [];
  let attempts = 0;

  queue.on("job.dead", (event) => dead.push(event));
  queue.task(
    "brokenPredicate",
    () => {
      attempts += 1;
      throw new Error("boom");
    },
    {
      maxRetries: 1,
      retryBackoff: { baseMs: 1, maxMs: 5 },
      retryOn: () => {
        throw new Error("predicate is broken");
      },
    },
  );

  queue.enqueue("brokenPredicate");
  worker = queue.runWorker();

  expect(await waitFor(() => dead.length > 0)).toBe(true);
  expect(attempts).toBe(2);
});
