import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  type AutoscaleMetricsSource,
  type AutoscaleOptions,
  Autoscaler,
  computeDesiredWorkers,
  resolveAutoscaleConfig,
  type Stats,
  setLogLevel,
  WorkerProcessManager,
} from "../../src/index";

const config = (overrides: Partial<AutoscaleOptions> = {}) =>
  resolveAutoscaleConfig({
    app: "./app.js",
    minWorkers: 1,
    maxWorkers: 10,
    targetQueueDepthPerWorker: 10,
    targetUtilisation: 0.75,
    concurrencyPerWorker: 4,
    tolerance: 0, // off by default so the decision under test is the only variable
    ...overrides,
  });

describe("resolveAutoscaleConfig", () => {
  it("fills in HPA-shaped defaults", () => {
    const resolved = resolveAutoscaleConfig({ app: "./app.js" });
    expect(resolved).toMatchObject({
      minWorkers: 1,
      maxWorkers: 10,
      targetQueueDepthPerWorker: 15,
      targetUtilisation: 0.75,
      scaleUpWindowMs: 0,
      scaleDownWindowMs: 300_000,
      tolerance: 0.1,
      pollIntervalMs: 5_000,
      drainTimeoutMs: 30_000,
      concurrencyPerWorker: 4,
    });
    expect(resolved.queues).toBeUndefined();
    expect(resolved.nodeExecutable).toBe(process.execPath);
  });

  it("rejects values that would break the control loop", () => {
    expect(() => resolveAutoscaleConfig({ app: "" })).toThrow(/app is required/);
    expect(() => resolveAutoscaleConfig({ app: "a", minWorkers: -1 })).toThrow(/minWorkers/);
    expect(() => resolveAutoscaleConfig({ app: "a", maxWorkers: 0 })).toThrow(/maxWorkers/);
    expect(() => resolveAutoscaleConfig({ app: "a", minWorkers: 5, maxWorkers: 2 })).toThrow(
      /must be >= minWorkers/,
    );
    expect(() => resolveAutoscaleConfig({ app: "a", targetQueueDepthPerWorker: 0 })).toThrow(
      /targetQueueDepthPerWorker/,
    );
    expect(() => resolveAutoscaleConfig({ app: "a", targetUtilisation: 1 })).toThrow(
      /targetUtilisation/,
    );
    expect(() => resolveAutoscaleConfig({ app: "a", tolerance: 1 })).toThrow(/tolerance/);
    expect(() => resolveAutoscaleConfig({ app: "a", pollIntervalMs: 0 })).toThrow(/pollIntervalMs/);
    expect(() => resolveAutoscaleConfig({ app: "a", concurrencyPerWorker: 0 })).toThrow(
      /concurrencyPerWorker/,
    );
  });
});

describe("computeDesiredWorkers", () => {
  const decide = (pending: number, running: number, currentWorkers: number, overrides = {}) =>
    computeDesiredWorkers({ pending, running, currentWorkers, config: config(overrides) });

  it("sizes the pool from queue depth", () => {
    // 100 pending / 10 per worker = 10.
    expect(decide(100, 0, 1, { maxWorkers: 20 }).desiredWorkers).toBe(10);
  });

  it("holds steady at the utilisation target", () => {
    // 2 workers x 4 concurrency = 8 capacity; 6 running = 0.75 utilisation.
    const decision = decide(0, 6, 2);
    expect(decision.desiredWorkers).toBe(2);
    expect(decision.rationale).toMatch(/^stable/);
  });

  it("scales up when utilisation exceeds the target", () => {
    // 8 running on 8 capacity = 1.0 / 0.75 -> ceil(2 x 1.33) = 3.
    expect(decide(0, 8, 2).desiredWorkers).toBe(3);
  });

  it("clamps to minWorkers and maxWorkers", () => {
    expect(decide(0, 0, 5, { minWorkers: 3 }).desiredWorkers).toBe(3);
    expect(decide(10_000, 0, 1, { maxWorkers: 4 }).desiredWorkers).toBe(4);
  });

  it("bypasses the tolerance band when a worker is overloaded", () => {
    // 10 running on 4 capacity is overload; a 90% band would otherwise absorb it.
    const decision = decide(0, 10, 1, { tolerance: 0.9, maxWorkers: 5 });
    expect(decision.desiredWorkers).toBeGreaterThanOrEqual(2);
    expect(decision.rationale).toContain("overload=true");
  });

  it("suppresses churn inside the tolerance band", () => {
    // Depth alone asks for 21 workers — 5% over 20, inside a 10% band.
    expect(decide(201, 0, 20, { tolerance: 0.1, maxWorkers: 30 }).desiredWorkers).toBe(20);
  });

  it("lets an idle pool fall to minWorkers", () => {
    expect(decide(0, 0, 8, { minWorkers: 1 }).desiredWorkers).toBe(1);
  });
});

/** Observable stand-in for the process pool — no subprocesses involved. */
function stubPool() {
  let nextPid = 100;
  const live = new Set<number>();
  const pool = {
    spawned: [] as number[],
    terminated: [] as number[],
    crashed: [] as number[],
    live,
    /** Pretend `count` workers were already running before the test. */
    seed(count: number) {
      for (let i = 0; i < count; i += 1) {
        live.add(nextPid++);
      }
    },
  };
  vi.spyOn(WorkerProcessManager.prototype, "spawnWorker").mockImplementation(() => {
    const pid = nextPid++;
    live.add(pid);
    pool.spawned.push(pid);
    return pid;
  });
  vi.spyOn(WorkerProcessManager.prototype, "terminateWorker").mockImplementation(async (pid) => {
    live.delete(pid);
    pool.terminated.push(pid);
    return true;
  });
  vi.spyOn(WorkerProcessManager.prototype, "reapDead").mockImplementation(() => {
    const dead = pool.crashed;
    pool.crashed = [];
    for (const pid of dead) {
      live.delete(pid);
    }
    return dead;
  });
  vi.spyOn(WorkerProcessManager.prototype, "livePids").mockImplementation(() => [...live]);
  vi.spyOn(WorkerProcessManager.prototype, "countLive").mockImplementation(() => live.size);
  return pool;
}

function stats(overrides: Partial<Stats>): Stats {
  return { pending: 0, running: 0, completed: 0, failed: 0, dead: 0, cancelled: 0, ...overrides };
}

function metricsSource(counts: Partial<Stats> | Error): AutoscaleMetricsSource {
  const read = async (): Promise<Stats> => {
    if (counts instanceof Error) {
      throw counts;
    }
    return stats(counts);
  };
  return { stats: read, statsByQueue: read };
}

describe("Autoscaler.tick", () => {
  let pool: ReturnType<typeof stubPool>;

  beforeEach(() => {
    pool = stubPool();
    // One case drives a storage failure on purpose; keep its stack out of the run.
    setLogLevel("silent");
  });

  afterEach(() => {
    setLogLevel("warn");
    vi.restoreAllMocks();
  });

  const autoscaler = (source: AutoscaleMetricsSource, overrides: Partial<AutoscaleOptions> = {}) =>
    new Autoscaler(source, {
      app: "./app.js",
      targetQueueDepthPerWorker: 10,
      concurrencyPerWorker: 4,
      tolerance: 0,
      scaleDownWindowMs: 0,
      ...overrides,
    });

  it("spawns up to the desired count on a deep queue", async () => {
    pool.seed(1);
    const decision = await autoscaler(metricsSource({ pending: 100 }), {
      maxWorkers: 20,
    }).tick();

    expect(decision.desiredWorkers).toBe(10);
    expect(pool.spawned).toHaveLength(9);
    expect(pool.live.size).toBe(10);
  });

  it("drains workers the pool no longer needs", async () => {
    pool.seed(5);
    await autoscaler(metricsSource({ pending: 0 }), { minWorkers: 1 }).tick();

    expect(pool.terminated).toHaveLength(4);
    expect(pool.live.size).toBe(1);
  });

  it("holds the pool when metrics are unreadable", async () => {
    pool.seed(3);
    const decision = await autoscaler(metricsSource(new Error("storage offline")), {
      minWorkers: 3,
    }).tick();

    expect(decision.desiredWorkers).toBe(3);
    expect(pool.spawned).toHaveLength(0);
    expect(pool.terminated).toHaveLength(0);
  });

  it("replaces a crashed worker to keep minWorkers", async () => {
    pool.seed(2);
    pool.crashed = [[...pool.live][0] as number];
    await autoscaler(metricsSource({ pending: 0 }), { minWorkers: 2 }).tick();

    expect(pool.spawned).toHaveLength(1);
    expect(pool.live.size).toBe(2);
  });

  it("holds the pool through a lull inside the scale-down window", async () => {
    pool.seed(4);
    let pending = 25;
    const scaler = autoscaler(
      { stats: async () => stats({ pending }), statsByQueue: async () => stats({ pending }) },
      { minWorkers: 1, scaleDownWindowMs: 300_000 },
    );

    // 25 pending over a target of 10 wants 3 workers, so one of the four goes.
    await scaler.tick();
    expect(pool.live.size).toBe(3);

    // The queue empties, but the window keeps the highest recent
    // recommendation — the lull alone must not tear the pool down to one.
    pending = 0;
    const decision = await scaler.tick();
    expect(decision.desiredWorkers).toBe(3);
    expect(decision.rationale).toContain("windowed -> 3");
    expect(pool.terminated).toHaveLength(1);
  });

  it("sums metrics across only the queues its workers consume", async () => {
    pool.seed(1);
    const seen: string[] = [];
    const source: AutoscaleMetricsSource = {
      stats: async () => {
        throw new Error("should not read global stats when queues are set");
      },
      statsByQueue: async (name) => {
        seen.push(name);
        return stats({ pending: 50 });
      },
    };

    const decision = await autoscaler(source, {
      queues: ["emails", "reports"],
      maxWorkers: 20,
    }).tick();

    expect(seen.sort()).toEqual(["emails", "reports"]);
    expect(decision.pending).toBe(100);
    expect(decision.desiredWorkers).toBe(10);
  });
});
