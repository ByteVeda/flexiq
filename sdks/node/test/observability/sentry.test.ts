import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ErrorEvent } from "@sentry/node";
import * as Sentry from "@sentry/node";
import { afterEach, expect, it } from "vitest";
import { sentryMiddleware } from "../../src/contrib/sentry";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(async () => {
  worker?.stop();
  worker = undefined;
  await Sentry.close();
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-sentry-")), "q.db") });
}

/** Init Sentry with a beforeSend hook that records events instead of sending them. */
function captureEvents(): ErrorEvent[] {
  const events: ErrorEvent[] = [];
  Sentry.init({
    dsn: "https://examplePublicKey@o0.ingest.sentry.io/0",
    beforeSend(event) {
      events.push(event);
      return null; // drop — never hits the network
    },
  });
  return events;
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 4000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("captures one event with the stack and tags when a job dead-letters", async () => {
  const events = captureEvents();
  const queue = newQueue();
  queue.use(sentryMiddleware());
  queue.task(
    "boom",
    () => {
      throw new Error("kaboom");
    },
    { maxRetries: 0 },
  );

  queue.enqueue("boom", []);
  worker = queue.runWorker();

  expect(await waitFor(() => events.length > 0)).toBe(true);
  expect(events).toHaveLength(1);
  const event = events[0];
  expect(event?.level).toBe("error");
  expect(event?.tags?.["flexiq.task_name"]).toBe("boom");
  expect(typeof event?.tags?.["flexiq.job_id"]).toBe("string");
  expect(event?.exception?.values?.[0]?.value).toBe("kaboom");
});

it("does not capture successful jobs", async () => {
  const events = captureEvents();
  const queue = newQueue();
  queue.use(sentryMiddleware());
  queue.task("ok", () => 1);

  queue.enqueue("ok", []);
  worker = queue.runWorker();

  expect(await waitFor(async () => (await queue.stats()).completed >= 1)).toBe(true);
  await new Promise((resolve) => setTimeout(resolve, 100));
  expect(events).toHaveLength(0);
});

it("reports retries as warnings when captureRetries is set", async () => {
  const events = captureEvents();
  const queue = newQueue();
  queue.use(sentryMiddleware({ captureRetries: true }));
  queue.task(
    "flaky",
    () => {
      throw new Error("nope");
    },
    { maxRetries: 1, retryBackoff: { baseMs: 1, maxMs: 5 } },
  );

  queue.enqueue("flaky", []);
  worker = queue.runWorker();

  // One retry (warning) + one dead-letter (error).
  expect(await waitFor(() => events.length >= 2)).toBe(true);
  const levels = events.map((e) => e.level);
  expect(levels).toContain("warning");
  expect(levels).toContain("error");
});
