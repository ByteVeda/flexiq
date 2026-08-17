// S27 — opt-in CoDel load shedding. Behavioral (timing-based): a slow task at
// concurrency 1 backs the queue up, so later jobs stay above target for a full
// interval and CoDel sheds the stalest ones to the DLQ. The controller algorithm
// itself is unit-tested in Rust (`scheduler::codel`).

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
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-codel-")), "queue.db");
  return new Queue({ dbPath });
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

it("rejects non-positive-integer CoDel bounds at configureQueue", () => {
  const queue = newQueue();
  for (const bad of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    expect(() =>
      queue.configureQueue("default", { codel: { targetMs: bad, intervalMs: 30 } }),
    ).toThrow(RangeError);
    expect(() =>
      queue.configureQueue("default", { codel: { targetMs: 30, intervalMs: bad } }),
    ).toThrow(RangeError);
  }
  // A valid pair is accepted.
  queue.configureQueue("default", { codel: { targetMs: 1, intervalMs: 30 } });
});

it("sheds stale jobs to the DLQ under sustained overload", async () => {
  const queue = newQueue();
  queue.configureQueue("default", { codel: { targetMs: 1, intervalMs: 30 } });
  queue.task("slow", async () => {
    await sleep(50);
  });

  const total = 20;
  for (let i = 0; i < total; i++) queue.enqueue("slow");

  worker = queue.runWorker({ concurrency: 1, batchSize: 1 });

  // Poll until every job is accounted for (ran or shed) AND at least one was
  // shed — an aggregate-convergence check, never an instantaneous snapshot.
  const deadline = Date.now() + 25_000;
  let codelDead = 0;
  let stats = await queue.stats();
  while (Date.now() < deadline) {
    const dead = await queue.deadLetters(100);
    codelDead = dead.filter((d) => (d.error ?? "").startsWith("codel:")).length;
    stats = await queue.stats();
    if (stats.completed + stats.dead === total && codelDead >= 1) break;
    await sleep(100);
  }

  // A job can be shed between the dead-letter read and the stats read that
  // satisfied the exit condition; once converged nothing moves, so a fresh
  // dead-letter read gives the settled count.
  const dead = await queue.deadLetters(100);
  codelDead = dead.filter((d) => (d.error ?? "").startsWith("codel:")).length;

  expect(codelDead).toBeGreaterThanOrEqual(1);
  // Every dead job is a CoDel drop (the task never throws), and nothing is lost.
  expect(stats.dead).toBe(codelDead);
  expect(stats.completed + stats.dead).toBe(total);
});
