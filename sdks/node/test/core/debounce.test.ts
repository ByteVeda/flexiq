// #654 — debounce options on the task registration and enqueue APIs. A burst
// of enqueues sharing a resolved key collapses onto one pending job whose
// deadline slides forward, bounded by `debounceMaxWait`.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { DebounceOptions, Queue, QueueError, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(async () => {
  // `stop()` is async — awaiting keeps a worker from outliving its test.
  await worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-debounce-")), "queue.db");
  return new Queue({ dbPath });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForStatus(
  queue: Queue,
  id: string,
  predicate: (status: string) => boolean,
  timeoutMs = 5000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = queue.getJob(id);
    if (job && predicate(job.status)) return;
    await sleep(25);
  }
  throw new Error("timed out waiting for job state");
}

// --- collapse -------------------------------------------------------------

it("collapses a burst of enqueues onto one pending job", () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined, {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  });

  const first = queue.enqueue("report", [{ userId: "u1" }]);
  const second = queue.enqueue("report", [{ userId: "u1" }]);
  const third = queue.enqueue("report", [{ userId: "u1" }]);

  expect(second).toBe(first);
  expect(third).toBe(first);
  expect(queue.countPendingByQueue("default")).toBe(1);
});

it("slides the pending job's deadline forward", async () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined, {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  });

  const id = queue.enqueue("report", [{ userId: "u1" }]);
  const opened = queue.getJob(id)?.scheduledAt as number;
  await sleep(60);
  queue.enqueue("report", [{ userId: "u1" }]);

  expect(queue.getJob(id)?.scheduledAt as number).toBeGreaterThan(opened);
});

it("caps the slide at debounceMaxWait", async () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined, {
    // A window longer than the ceiling it is measured against, so the third
    // enqueue's `now + window` is guaranteed to overshoot.
    debounce: 400,
    debounceKey: "report:{userId}",
    debounceMaxWait: 500,
  });

  const id = queue.enqueue("report", [{ userId: "u1" }]);
  await sleep(150);
  queue.enqueue("report", [{ userId: "u1" }]);
  await sleep(150);
  queue.enqueue("report", [{ userId: "u1" }]);

  const job = queue.getJob(id);
  // min(now + 400, firstSeen + 500) — the ceiling wins, exactly.
  expect(job?.scheduledAt).toBe((job?.createdAt as number) + 500);
});

it("keeps distinct resolved keys independent", () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined, {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  });

  const first = queue.enqueue("report", [{ userId: "u1" }]);
  const second = queue.enqueue("report", [{ userId: "u2" }]);

  expect(second).not.toBe(first);
  expect(queue.countPendingByQueue("default")).toBe(2);
});

it("runs the collapsed job once after the window", async () => {
  const queue = newQueue();
  const runs: string[] = [];
  queue.task(
    "report",
    (input: { userId: string }) => {
      runs.push(input.userId);
    },
    { debounce: 300, debounceKey: "report:{userId}", debounceMaxWait: 1000 },
  );

  const id = queue.enqueue("report", [{ userId: "u1" }]);
  queue.enqueue("report", [{ userId: "u1" }]);
  worker = queue.runWorker();

  await waitForStatus(queue, id, (status) => status === "complete");
  await sleep(300);
  expect(runs).toEqual(["u1"]);
});

// --- payload replacement --------------------------------------------------

it("keeps the payload the window opened with by default", async () => {
  const queue = newQueue();
  const seen: number[] = [];
  queue.task(
    "report",
    (input: { userId: string; revision: number }) => {
      seen.push(input.revision);
    },
    { debounce: 300, debounceKey: "report:{userId}", debounceMaxWait: 1000 },
  );

  const id = queue.enqueue("report", [{ userId: "u1", revision: 1 }]);
  queue.enqueue("report", [{ userId: "u1", revision: 2 }]);
  worker = queue.runWorker();

  await waitForStatus(queue, id, (status) => status === "complete");
  expect(seen).toEqual([1]);
});

it("runs with the newest payload under debounceReplacePayload", async () => {
  const queue = newQueue();
  const seen: number[] = [];
  queue.task(
    "report",
    (input: { userId: string; revision: number }) => {
      seen.push(input.revision);
    },
    {
      debounce: 300,
      debounceKey: "report:{userId}",
      debounceMaxWait: 1000,
      debounceReplacePayload: true,
    },
  );

  const id = queue.enqueue("report", [{ userId: "u1", revision: 1 }]);
  queue.enqueue("report", [{ userId: "u1", revision: 2 }]);
  worker = queue.runWorker();

  await waitForStatus(queue, id, (status) => status === "complete");
  expect(seen).toEqual([2]);
});

// --- the imperative path --------------------------------------------------

it("debounces from the enqueue options alone", () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined);

  const options = {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  } as const;
  const first = queue.enqueue("report", [{ userId: "u1" }], options);
  const second = queue.enqueue("report", [{ userId: "u1" }], options);

  expect(second).toBe(first);
});

it("lets an enqueue override the task's window", async () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined, {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  });

  const id = queue.enqueue("report", [{ userId: "u1" }]);
  const opened = queue.getJob(id)?.scheduledAt as number;
  queue.enqueue("report", [{ userId: "u1" }], { debounce: "2m" });

  // The override inherits the registered key and ceiling, and pushes further out.
  expect(queue.getJob(id)?.scheduledAt as number).toBeGreaterThan(opened);
});

// --- key templates --------------------------------------------------------

it("resolves a positional placeholder", () => {
  const queue = newQueue();
  queue.task("sync", (_userId: string) => undefined, {
    debounce: "1m",
    debounceKey: "sync:{0}",
    debounceMaxWait: "5m",
  });

  const first = queue.enqueue("sync", ["u1"]);
  expect(queue.enqueue("sync", ["u1"])).toBe(first);
  expect(queue.enqueue("sync", ["u2"])).not.toBe(first);
});

it("throws when a placeholder matches no argument", () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined, {
    debounce: "1m",
    debounceKey: "report:{tenantId}",
    debounceMaxWait: "5m",
  });

  expect(() => queue.enqueue("report", [{ userId: "u1" }])).toThrow(/\{tenantId}/);
  expect(queue.countPendingByQueue("default")).toBe(0);
});

it("ignores an inherited property when resolving a placeholder", () => {
  const queue = newQueue();
  queue.task("report", (_input: object) => undefined, {
    debounce: "1m",
    debounceKey: "report:{tenantId}",
    debounceMaxWait: "5m",
  });

  // Only own properties key a window; a prototype's value is nobody's argument.
  const inherited = Object.create({ tenantId: "shared" }) as object;
  expect(() => queue.enqueue("report", [inherited])).toThrow(/matches no argument/);
});

it("throws when a placeholder resolves to an object", () => {
  const queue = newQueue();
  queue.task("report", (_input: { owner: unknown }) => undefined, {
    debounce: "1m",
    debounceKey: "report:{owner}",
    debounceMaxWait: "5m",
  });

  expect(() => queue.enqueue("report", [{ owner: { id: "u1" } }])).toThrow(/only strings/);
});

// --- configuration errors -------------------------------------------------

it("rejects debounce without debounceMaxWait at registration", () => {
  const queue = newQueue();
  expect(() =>
    queue.task("report", () => undefined, { debounce: "5m", debounceKey: "report" }),
  ).toThrow(QueueError);
});

it("rejects debounce without debounceKey at registration", () => {
  const queue = newQueue();
  expect(() =>
    queue.task("report", () => undefined, { debounce: "5m", debounceMaxWait: "30m" }),
  ).toThrow(/debounceKey/);
});

it("rejects a debounceMaxWait shorter than the window", () => {
  const queue = newQueue();
  expect(() =>
    queue.task("report", () => undefined, {
      debounce: "5m",
      debounceKey: "report",
      debounceMaxWait: "1m",
    }),
  ).toThrow(/at least/);
});

it("rejects debounce fields without a window", () => {
  const queue = newQueue();
  expect(() => queue.task("report", () => undefined, { debounceKey: "report" })).toThrow(
    /require debounce/,
  );
});

it("rejects uniqueKey combined with debounce", () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined, {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  });

  expect(() => queue.enqueue("report", [{ userId: "u1" }], { uniqueKey: "k1" })).toThrow(
    /uniqueKey/,
  );
});

it("rejects debounce in a batch enqueue", () => {
  const queue = newQueue();
  queue.task("report", (_input: { userId: string }) => undefined, {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  });

  expect(() => queue.enqueueMany("report", [{ args: [{ userId: "u1" }] }])).toThrow(
    /cannot debounce/,
  );
});

it("rejects debounce on a subscriber", () => {
  const queue = newQueue();
  expect(() =>
    queue.subscriber("orders", "on-order", (_input: { userId: string }) => undefined, {
      debounce: "1m",
      debounceKey: "order:{userId}",
      debounceMaxWait: "5m",
    }),
  ).toThrow(/topic deliveries/);
});

// --- DebounceOptions ------------------------------------------------------

it("parses every duration unit", () => {
  const key = "k";
  const build = (debounce: number | `${number}ms` | `${number}s` | `${number}m` | `${number}h`) =>
    DebounceOptions.from("t", { debounce, debounceKey: key, debounceMaxWait: "1d" });

  expect(build(250)?.windowMs).toBe(250);
  expect(build("250ms")?.windowMs).toBe(250);
  expect(build("30s")?.windowMs).toBe(30_000);
  expect(build("5m")?.windowMs).toBe(300_000);
  expect(build("2h")?.windowMs).toBe(7_200_000);
  expect(
    DebounceOptions.from("t", { debounce: "1d", debounceKey: key, debounceMaxWait: "1d" })
      ?.maxWaitMs,
  ).toBe(86_400_000);
});

it("rejects an unparseable duration", () => {
  expect(() =>
    DebounceOptions.from("t", {
      debounce: "5 minutes" as unknown as number,
      debounceKey: "k",
      debounceMaxWait: "1d",
    }),
  ).toThrow(/is not a duration/);
});

it("rejects a duration that overflows to Infinity", () => {
  // Enough digits and the unit multiply overflows; the native i64 boundary
  // would silently turn Infinity into 0.
  const overflowing = `${"9".repeat(320)}d` as `${number}d`;
  expect(() =>
    DebounceOptions.from("t", {
      debounce: overflowing,
      debounceKey: "k",
      debounceMaxWait: overflowing,
    }),
  ).toThrow(/finite number of milliseconds/);
});

it("returns undefined when nothing is configured", () => {
  expect(DebounceOptions.from("t", {})).toBeUndefined();
});

it("resolves a key against a later object argument", () => {
  const options = DebounceOptions.from("t", {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  });
  expect(options?.resolveKey("t", ["ignored", { userId: "u1" }])).toBe("report:u1");
});

it("rejects a placeholder that resolves to an empty value", () => {
  // "report:" is still a key, so every caller with an empty userId would share
  // one window — the same silent collapse a missing property is rejected for.
  const options = DebounceOptions.from("t", {
    debounce: "1m",
    debounceKey: "report:{userId}",
    debounceMaxWait: "5m",
  });
  expect(() => options?.resolveKey("t", [{ userId: "" }])).toThrow(/is empty/);
});
