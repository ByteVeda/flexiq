import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-notes-")), "q.db") });
}

it("stores structured notes at enqueue and reads them back", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const id = queue.enqueue("noop", undefined, { notes: { source: "signup", attempt: 1 } });
  const job = queue.getJob(id);

  expect(job?.notes).toBeDefined();
  expect(JSON.parse(job?.notes as string)).toEqual({ source: "signup", attempt: 1 });
});

it("leaves notes unset when not provided", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const id = queue.enqueue("noop");
  expect(queue.getJob(id)?.notes ?? null).toBeNull();
});

it("rejects notes with more than 15 top-level fields", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const tooMany = Object.fromEntries(Array.from({ length: 16 }, (_, i) => [`k${i}`, i]));
  expect(() => queue.enqueue("noop", undefined, { notes: tooMany })).toThrow(/15 top-level/);
});

it("rejects notes larger than 4 KiB encoded", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);

  const big = { blob: "x".repeat(5000) };
  expect(() => queue.enqueue("noop", undefined, { notes: big })).toThrow(/exceeds/);
});

it("onEnqueue can rewrite notes before validation", () => {
  const queue = newQueue();
  queue.use({
    onEnqueue: (ctx) => {
      ctx.options.notes = { ...ctx.options.notes, tagged: true };
    },
  });
  queue.task("noop", () => undefined);

  const id = queue.enqueue("noop", undefined, { notes: { a: 1 } });
  expect(JSON.parse(queue.getJob(id)?.notes as string)).toEqual({ a: 1, tagged: true });
});
