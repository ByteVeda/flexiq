import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-sc-")), "q.db") });
}

/**
 * The message a failed `runWorker` threw.
 *
 * `toThrow(string)` only checks containment, and the contract here is that
 * exactly one scope is named — a message mentioning both would satisfy a
 * substring match. So compare the whole message instead.
 */
function startError(queue: Queue): string {
  try {
    queue.runWorker({ queues: ["default"] });
  } catch (error) {
    return (error as Error).message;
  }
  throw new Error("runWorker should have rejected the malformed rate limit");
}

const EXPECTED = "(expected a count of at least 1 over a unit, as in '100/m')";

// Tasks and queues configure a rate limit through one parser, so the error has
// to name which kind is wrong.
it("names the task, and only the task, for a malformed task rate limit", () => {
  const queue = newQueue();
  queue.task("t", () => {}, { rateLimit: "100/mm" as never });
  expect(startError(queue)).toBe(`invalid rateLimit '100/mm' on task 't' ${EXPECTED}`);
});

it("names the queue, and only the queue, for a malformed queue rate limit", () => {
  const queue = newQueue();
  queue.task("t", () => {});
  queue.configureQueue("default", { rateLimit: "100/mm" as never });
  expect(startError(queue)).toBe(`invalid rateLimit '100/mm' on queue 'default' ${EXPECTED}`);
});

// A count under one parses as a shape but builds a bucket that never holds the
// token a dispatch needs, so it has to be refused the same way a typo is.
it("rejects a rate limit whose count is below one", () => {
  const queue = newQueue();
  queue.task("t", () => {}, { rateLimit: "0/s" as never });
  expect(startError(queue)).toBe(`invalid rateLimit '0/s' on task 't' ${EXPECTED}`);
});
