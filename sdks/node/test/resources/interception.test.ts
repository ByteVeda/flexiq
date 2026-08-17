import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { type Middleware, Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-int-")), "q.db") });
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

it("onEnqueue can redact args before serialization", async () => {
  const queue = newQueue();
  const redact: Middleware = {
    onEnqueue: (ctx) => {
      ctx.args = ctx.args.map((a) => (typeof a === "string" && a.startsWith("pw:") ? "***" : a));
    },
  };
  queue.use(redact);
  let seen: unknown[] = [];
  queue.task("login", (...args: unknown[]) => {
    seen = args;
  });

  queue.enqueue("login", ["alice", "pw:hunter2"]);
  worker = queue.runWorker();

  expect(await waitFor(() => seen.length > 0)).toBe(true);
  expect(seen).toEqual(["alice", "***"]);
});

it("onEnqueue can rewrite options before they reach the core", () => {
  const queue = newQueue();
  queue.use({
    onEnqueue: (ctx) => {
      ctx.options.metadata = "tagged-by-mw";
    },
  });
  queue.task("noop", () => undefined);

  const id = queue.enqueue("noop");
  const job = queue.getJob(id);
  expect(job?.metadata).toBe("tagged-by-mw");
});

it("a throwing onEnqueue aborts the enqueue", async () => {
  const queue = newQueue();
  queue.use({
    onEnqueue: (ctx) => {
      if ((ctx.args[0] as number) < 0) {
        throw new Error("amount must be non-negative");
      }
    },
  });
  queue.task("charge", (_amount: number) => undefined);

  expect(() => queue.enqueue("charge", [-5])).toThrow("amount must be non-negative");
  expect((await queue.stats()).pending).toBe(0); // nothing was enqueued
});

it("runs onEnqueue hooks in registration order", () => {
  const queue = newQueue();
  const order: string[] = [];
  queue.use({ onEnqueue: () => void order.push("first") });
  queue.use({ onEnqueue: () => void order.push("second") });
  queue.task("noop", () => undefined);

  queue.enqueue("noop");
  expect(order).toEqual(["first", "second"]);
});
