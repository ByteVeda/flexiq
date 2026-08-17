import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { decodeTaskError, encodeTaskError, JobFailedError, Queue, type Worker } from "../../src";

let worker: Worker | undefined;
afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-terr-")), "q.db") });
}

class BoomError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BoomError";
  }
}

it("encodeTaskError matches the contract test vector byte-exact", () => {
  const error = new BoomError("it broke");
  error.stack = "frame1\nframe2";
  expect(encodeTaskError(error)).toBe(
    '{"errtype":"BoomError","message":"it broke","traceback":["frame1","frame2"]}',
  );
});

it("encodeTaskError handles non-Error thrown values", () => {
  expect(decodeTaskError(encodeTaskError("oops"))).toEqual({
    errtype: "Error",
    message: "oops",
    traceback: [],
  });
});

it("decodeTaskError returns null for anything but an object with a string message", () => {
  expect(decodeTaskError("plain failure string")).toBeNull();
  expect(decodeTaskError('["not","an","object"]')).toBeNull();
  expect(decodeTaskError('{"errtype":"X","traceback":[]}')).toBeNull();
});

it("stores a failing task's error as canonical JSON and surfaces it structured", async () => {
  const queue = newQueue();
  queue.task(
    "boom",
    () => {
      throw new BoomError("it broke");
    },
    { maxRetries: 0 },
  );
  const id = queue.enqueue("boom");
  worker = queue.runWorker();

  let caught: unknown;
  try {
    await queue.result(id);
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(JobFailedError);
  const failed = caught as JobFailedError;

  // The stored string must be the bare JSON object — nothing prefixed by the
  // native layer.
  const decoded = decodeTaskError(failed.raw);
  expect(decoded).not.toBeNull();
  expect(decoded?.errtype).toBe("BoomError");
  expect(decoded?.message).toBe("it broke");
  expect(decoded?.traceback.length).toBeGreaterThan(0);

  expect(failed.errtype).toBe("BoomError");
  expect(failed.traceback?.length).toBeGreaterThan(0);
  expect(failed.message).toBe(`Job ${id} failed: BoomError: it broke`);
});

it("JobFailedError falls back verbatim on a plain-string reason", () => {
  const failed = new JobFailedError("j1", "task timed out");
  expect(failed.errtype).toBeUndefined();
  expect(failed.traceback).toBeUndefined();
  expect(failed.raw).toBe("task timed out");
  expect(failed.message).toBe("Job j1 failed: task timed out");
});
