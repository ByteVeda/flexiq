import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { checkHealth, checkReadiness, type ResourcesCheck, resourceStatus } from "../../src/health";
import { Queue, type WorkerInfo } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-health-")), "q.db") });
}

/** A Queue stand-in for the failure paths a real storage won't produce. */
function fakeQueue(overrides: Partial<Record<"stats" | "listWorkers", unknown>>): Queue {
  return {
    stats: async () => ({ pending: 0, running: 0 }),
    listWorkers: async () => [],
    ...overrides,
  } as unknown as Queue;
}

function worker(resources: string[], health: Record<string, string>): WorkerInfo {
  return {
    workerId: "w1",
    status: "running",
    lastHeartbeat: 0,
    threads: 1,
    resources: JSON.stringify(resources),
    resourceHealth: JSON.stringify(health),
  } as WorkerInfo;
}

it("reports liveness without touching storage", () => {
  expect(checkHealth()).toEqual({ status: "ok" });
});

it("is ready over a live queue with no workers", async () => {
  // The fleet size is reported, never folded into the status — degrading here
  // would leave every producer-only instance permanently unready.
  const report = await checkReadiness(newQueue());
  expect(report.status).toBe("ready");
  expect(report.checks.storage).toBe("ok");
  expect(report.checks.workers).toEqual({ count: 0, status: "none" });
  // No worker advertises resources, so the check is omitted entirely.
  expect(report.checks.resources).toBeUndefined();
});

it("counts live workers", async () => {
  const report = await checkReadiness(fakeQueue({ listWorkers: async () => [worker([], {})] }));
  expect(report.status).toBe("ready");
  expect(report.checks.workers).toEqual({ count: 1, status: "ok" });
});

it("degrades when storage is unreachable", async () => {
  const report = await checkReadiness(
    fakeQueue({
      stats: async () => {
        throw new Error("db gone");
      },
    }),
  );
  expect(report.status).toBe("degraded");
  expect(report.checks.storage).toMatch(/error: .*db gone/);
});

it("degrades when a worker resource is unhealthy", async () => {
  const report = await checkReadiness(
    fakeQueue({
      listWorkers: async () => [worker(["db", "cache"], { db: "healthy", cache: "unhealthy" })],
    }),
  );
  expect(report.status).toBe("degraded");
  expect(report.checks.resources).toEqual({
    count: 2,
    unhealthy: ["cache"],
    status: "degraded",
  } satisfies ResourcesCheck);
});

it("never throws — a failing dependency lands in checks", async () => {
  const report = await checkReadiness(
    fakeQueue({
      listWorkers: async () => {
        throw new Error("worker table locked");
      },
    }),
  );
  expect(report.status).toBe("degraded");
  expect(report.checks.workers).toMatch(/error: .*worker table locked/);
});

it("aggregates resource health across workers", async () => {
  const workers = [
    { ...worker(["db", "cache", "idle"], { db: "healthy", cache: "unhealthy" }), workerId: "w1" },
    { ...worker(["db", "cache"], { db: "unhealthy", cache: "unhealthy" }), workerId: "w2" },
  ];
  const entries = await resourceStatus(fakeQueue({ listWorkers: async () => workers }));
  expect(entries.map((e) => [e.name, e.health])).toEqual([
    ["cache", "unhealthy"], // every worker reports it broken
    ["db", "unhealthy"], // the worst report wins, so one sick worker is enough
    ["idle", "not_initialized"], // advertised, never reported
  ]);
});

it("keeps a merely degraded resource out of unhealthy", async () => {
  const workers = [
    { ...worker(["db"], { db: "degraded" }), workerId: "w1" },
    { ...worker(["db"], { db: "degraded" }), workerId: "w2" },
  ];
  const entries = await resourceStatus(fakeQueue({ listWorkers: async () => workers }));
  expect(entries.map((e) => [e.name, e.health])).toEqual([["db", "degraded"]]);
});

it("degrades when reports are mixed but none are unhealthy", async () => {
  const workers = [
    { ...worker(["db"], { db: "healthy" }), workerId: "w1" },
    { ...worker(["db"], { db: "degraded" }), workerId: "w2" },
  ];
  const entries = await resourceStatus(fakeQueue({ listWorkers: async () => workers }));
  expect(entries.map((e) => [e.name, e.health])).toEqual([["db", "degraded"]]);
});

it("ignores malformed heartbeat JSON", async () => {
  const broken = { ...worker([], {}), resources: "{oops", resourceHealth: "[]" };
  const entries = await resourceStatus(fakeQueue({ listWorkers: async () => [broken] }));
  expect(entries).toEqual([]);
});
