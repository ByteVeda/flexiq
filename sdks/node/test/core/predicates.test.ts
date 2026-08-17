import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  allOf,
  anyOf,
  Decision,
  EnqueueSkippedError,
  not,
  type PredicateEvent,
  PredicateRejectedError,
  PredicateValidationError,
  Queue,
} from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-pred-")), "q.db") });
}

it("enqueues when the gate passes", () => {
  const queue = newQueue().task("charge", (n: number) => n);
  queue.gate("charge", ({ args }) => args[0] > 0);
  expect(typeof queue.enqueue("charge", [5])).toBe("string");
});

it("rejects the enqueue when the gate fails", async () => {
  const queue = newQueue().task("charge", (n: number) => n);
  queue.gate("charge", ({ args }) => args[0] > 0);
  expect(() => queue.enqueue("charge", [-1])).toThrow(PredicateRejectedError);
  expect((await queue.stats()).pending).toBe(0);
});

it("requires every gate to pass", () => {
  const queue = newQueue().task("t", (n: number) => n);
  queue.gate("t", ({ args }) => args[0] > 0);
  queue.gate("t", ({ args }) => args[0] < 100);
  expect(typeof queue.enqueue("t", [50])).toBe("string");
  expect(() => queue.enqueue("t", [200])).toThrow(PredicateRejectedError);
});

it("sees args after onEnqueue rewrites them", () => {
  const queue = newQueue().task("t", (n: number) => n);
  queue.use({
    onEnqueue: (ctx) => {
      ctx.args = [Math.abs(ctx.args[0] as number)];
    },
  });
  queue.gate("t", ({ args }) => args[0] > 0);
  expect(typeof queue.enqueue("t", [-5])).toBe("string"); // rewritten to 5 → passes
});

it("composes predicates with allOf / anyOf / not", () => {
  const queue = newQueue().task("t", (n: number) => n);
  const positive = ({ args }: { args: readonly unknown[] }) => (args[0] as number) > 0;
  const even = ({ args }: { args: readonly unknown[] }) => (args[0] as number) % 2 === 0;
  queue.gate("t", allOf(positive, not(even)));
  expect(typeof queue.enqueue("t", [3])).toBe("string"); // positive, odd
  expect(() => queue.enqueue("t", [4])).toThrow(PredicateRejectedError); // even
  expect(() => queue.enqueue("t", [-3])).toThrow(PredicateRejectedError); // not positive

  const q2 = newQueue().task("t", (n: number) => n);
  q2.gate("t", anyOf(positive, even));
  expect(typeof q2.enqueue("t", [-2])).toBe("string"); // negative but even
});

it("emits predicate.rejected alongside the throw", () => {
  const queue = newQueue().task("charge", (n: number) => n);
  queue.gate("charge", ({ args }) => args[0] > 0);
  const rejected: PredicateEvent[] = [];
  queue.on("predicate.rejected", (event) => rejected.push(event));

  expect(() => queue.enqueue("charge", [-1])).toThrow(PredicateRejectedError);
  expect(rejected).toEqual([{ taskName: "charge" }]);

  queue.enqueue("charge", [5]); // a passing gate emits nothing
  expect(rejected).toHaveLength(1);
});

it("gates each job in a batch", () => {
  const queue = newQueue().task("t", (n: number) => n);
  queue.gate("t", ({ args }) => args[0] > 0);
  expect(() => queue.enqueueMany("t", [{ args: [1] }, { args: [-1] }])).toThrow(
    PredicateRejectedError,
  );
});

describe("decisions", () => {
  it("rejects with the gate's reason", async () => {
    const queue = newQueue().task("charge", (n: number) => n);
    queue.gate("charge", () => Decision.reject("tenant not allowed"));
    const rejected: PredicateEvent[] = [];
    queue.on("predicate.rejected", (event) => rejected.push(event));

    expect(() => queue.enqueue("charge", [1])).toThrow(/tenant not allowed/);
    expect(rejected).toEqual([{ taskName: "charge", reason: "tenant not allowed" }]);
    expect((await queue.stats()).pending).toBe(0);
  });

  it("skips without creating a job", async () => {
    const queue = newQueue().task("digest", (n: number) => n);
    queue.gate("digest", () => Decision.skip("feature disabled"));
    const skipped: PredicateEvent[] = [];
    queue.on("predicate.skipped", (event) => skipped.push(event));

    expect(() => queue.enqueue("digest", [1])).toThrow(EnqueueSkippedError);
    expect(queue.tryEnqueue("digest", [1])).toBeNull();
    expect((await queue.stats()).pending).toBe(0);
    expect(skipped).toEqual([
      { taskName: "digest", reason: "feature disabled" },
      { taskName: "digest", reason: "feature disabled" },
    ]);
  });

  it("tryEnqueue returns the id when the gate allows", () => {
    const queue = newQueue().task("t", (n: number) => n);
    queue.gate("t", () => Decision.allow());
    expect(typeof queue.tryEnqueue("t", [1])).toBe("string");
  });

  it("defers by delaying the job", () => {
    const queue = newQueue().task("t", (n: number) => n);
    queue.gate("t", () => Decision.defer(60_000));
    const deferred: PredicateEvent[] = [];
    queue.on("predicate.deferred", (event) => deferred.push(event));

    const before = Date.now();
    const jobId = queue.enqueue("t", [1]);
    expect(deferred).toEqual([{ taskName: "t", delayMs: 60_000 }]);
    const job = queue.getJob(jobId);
    expect(job?.scheduledAt).toBeGreaterThanOrEqual(before + 59_000);
  });

  it("a deferral replaces the caller's delay", () => {
    const queue = newQueue().task("t", (n: number) => n);
    queue.gate("t", () => Decision.defer(1_000));
    const before = Date.now();
    const job = queue.getJob(queue.enqueue("t", [1], { delayMs: 3_600_000 }));
    expect(job?.scheduledAt).toBeLessThan(before + 60_000);
  });

  it("deferUntil clamps a past instant to no delay", () => {
    expect(Decision.deferUntil(new Date(1_000), new Date(5_000))).toEqual({
      kind: "defer",
      delayMs: 0,
    });
    expect(Decision.deferUntil(new Date(9_000), new Date(5_000))).toEqual({
      kind: "defer",
      delayMs: 4_000,
    });
  });

  it("rejects a negative deferral", () => {
    expect(() => Decision.defer(-1)).toThrow(RangeError);
  });

  it("treats a null-ish gate return as a rejection", () => {
    const queue = newQueue().task("t", (n: number) => n);
    // A JavaScript caller can return nothing by accident; fail closed.
    queue.gate("t", (() => undefined) as unknown as () => boolean);
    expect(() => queue.enqueue("t", [1])).toThrow(PredicateRejectedError);
  });

  it("fails closed on a null-ish gate inside allOf / anyOf", () => {
    const nothing = (() => undefined) as unknown as () => boolean;
    const queue = newQueue().task("t", (n: number) => n);
    queue.gate(
      "t",
      allOf(() => true, nothing),
    );
    expect(() => queue.enqueue("t", [1])).toThrow(PredicateRejectedError);

    const q2 = newQueue().task("t", (n: number) => n);
    q2.gate(
      "t",
      anyOf(nothing, () => false),
    );
    expect(() => q2.enqueue("t", [1])).toThrow(PredicateRejectedError);
  });

  it("re-validates a hand-built decision's payload", () => {
    const queue = newQueue().task("t", (n: number) => n);
    // `{ kind: "defer", delayMs: -1 }` type-checks, so the factory's invariant
    // has to be enforced again where the decision is consumed.
    queue.gate("t", () => ({ kind: "defer", delayMs: -1 }) as never);
    expect(() => queue.enqueue("t", [1])).toThrow(RangeError);

    const q2 = newQueue().task("t", (n: number) => n);
    q2.gate("t", () => ({ kind: "defer", delayMs: Number.NaN }) as never);
    expect(() => q2.enqueue("t", [1])).toThrow(RangeError);

    const q3 = newQueue().task("t", (n: number) => n);
    q3.gate("t", () => ({ kind: "flatten" }) as never);
    expect(() => q3.enqueue("t", [1])).toThrow(PredicateValidationError);
  });

  it("tolerates a decision with a missing or non-string reason", () => {
    const queue = newQueue().task("t", (n: number) => n);
    const skipped: PredicateEvent[] = [];
    queue.on("predicate.skipped", (event) => skipped.push(event));
    queue.gate("t", () => ({ kind: "skip" }) as never);
    expect(queue.tryEnqueue("t", [1])).toBeNull();
    expect(skipped).toEqual([{ taskName: "t" }]);

    const q2 = newQueue().task("t", (n: number) => n);
    q2.gate("t", () => ({ kind: "reject", reason: 42 }) as never);
    expect(() => q2.enqueue("t", [1])).toThrow(/rejected enqueue of "t": 42/);
  });

  it("defers a batch entry but refuses to skip one", () => {
    const queue = newQueue().task("t", (n: number) => n);
    queue.gate("t", ({ args }) => (args[0] === 0 ? Decision.defer(60_000) : Decision.allow()));
    const before = Date.now();
    const [immediate, delayed] = queue.enqueueMany("t", [{ args: [1] }, { args: [0] }]);
    expect(queue.getJob(immediate as string)?.scheduledAt).toBeLessThan(before + 1_000);
    expect(queue.getJob(delayed as string)?.scheduledAt).toBeGreaterThanOrEqual(before + 59_000);

    const skipping = newQueue().task("t", (n: number) => n);
    skipping.gate("t", ({ args }) => (args[0] === 0 ? Decision.skip() : Decision.allow()));
    expect(() => skipping.enqueueMany("t", [{ args: [1] }, { args: [0] }])).toThrow(
      EnqueueSkippedError,
    );
  });

  it("keeps a blocking decision when composed", () => {
    const queue = newQueue().task("report", (urgent: boolean) => urgent);
    const urgent = ({ args }: { args: readonly unknown[] }) => args[0] === true;
    queue.gate(
      "report",
      anyOf(urgent, () => Decision.defer(60_000)),
    );

    const before = Date.now();
    expect(queue.getJob(queue.enqueue("report", [true]))?.scheduledAt).toBeLessThan(before + 1_000);
    expect(queue.getJob(queue.enqueue("report", [false]))?.scheduledAt).toBeGreaterThanOrEqual(
      before + 59_000,
    );
  });
});

describe("predicateStats", () => {
  it("counts one outcome per gated enqueue", () => {
    const queue = newQueue()
      .task("t", (n: number) => n)
      .task("ungated", (n: number) => n);
    queue.gate("t", ({ args }) => {
      switch (args[0]) {
        case 0:
          return Decision.skip();
        case 1:
          return Decision.defer(1_000);
        case 2:
          return Decision.reject("nope");
        default:
          return true;
      }
    });

    queue.enqueue("t", [3]);
    queue.tryEnqueue("t", [0]);
    queue.enqueue("t", [1]);
    expect(() => queue.enqueue("t", [2])).toThrow(PredicateRejectedError);
    queue.enqueue("ungated", [1]);

    expect(queue.predicateStats()).toEqual({
      allowed: 1,
      skipped: 1,
      deferred: 1,
      rejected: 1,
      errors: 0,
    });
  });

  it("counts a throwing gate and lets the error through", () => {
    const queue = newQueue().task("t", (n: number) => n);
    queue.gate("t", () => {
      throw new Error("flag service down");
    });
    expect(() => queue.enqueue("t", [1])).toThrow("flag service down");
    expect(queue.predicateStats()).toMatchObject({ errors: 1, allowed: 0 });
  });
});
