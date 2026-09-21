// AUTO-GENERATED from /bench/results/latest.json by scripts/sync-benchmarks.mjs — do not edit directly.
// The numbers come from `bench/run.py`; to change them, re-run the harness and commit its artifact.

/** One entrant's row. Every field is measured, none is derived in the browser. */
export interface BenchmarkRuntime {
  id: string;
  engine: string;
  engineVersion: string;
  language: string;
  backend: string;
  /** What "concurrency 4" meant for this entrant, in its own units. */
  concurrencyModel: string;
  enqueuePerSecond: number;
  drainPerSecond: number;
  /** Jobs still unfinished when submission stopped — measured, not inferred. */
  backlogAtSubmitEnd: number | null;
  latencyMs: { p50: number; p95: number; p99: number; max: number };
  idle: { cpuPct: number; rssMb: number };
}

export interface BenchmarkRun {
  runId: string;
  generatedAt: string;
  source: string;
  scenario: { jobs: number; payloadBytes: number; concurrency: number; warmupJobs: number };
  machine: {
    os: string;
    cpu: string;
    cores: number;
    ramGb: number;
    python: string;
    node: string | null;
  };
  /** Null when the run had no Redis-backed entrants. */
  redis: {
    kind: string;
    provider: string;
    region: string | null;
    version: string;
    rttMsAvg: number | null;
  } | null;
  runtimes: BenchmarkRuntime[];
  /** Caveats written by the harness, rendered verbatim beside the chart. */
  notes: string[];
}

export const BENCHMARK: BenchmarkRun = {
  "runId": "202609212311450b1b",
  "generatedAt": "2026-09-21T18:09:57Z",
  "source": "bench/run.py",
  "scenario": {
    "jobs": 500,
    "payloadBytes": 256,
    "concurrency": 4,
    "warmupJobs": 50
  },
  "machine": {
    "os": "Linux 7.0.0-31-generic",
    "cpu": "AMD Ryzen 5 5600H with Radeon Graphics",
    "cores": 12,
    "ramGb": 13.5,
    "python": "3.12.12",
    "node": "v24.12.0"
  },
  "redis": {
    "kind": "remote",
    "provider": "Redis Cloud",
    "region": "ap-south-1",
    "version": "8.6.2",
    "rttMsAvg": 34.45
  },
  "runtimes": [
    {
      "id": "flexiq-redis",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Python 3.12.12",
      "backend": "redis",
      "concurrencyModel": "4 worker threads (workers=4)",
      "enqueuePerSecond": 7.7,
      "drainPerSecond": 1,
      "backlogAtSubmitEnd": null,
      "latencyMs": {
        "p50": 216961.38,
        "p95": 401910.49,
        "p99": 415367.25,
        "max": 421790.1
      },
      "idle": {
        "cpuPct": 0.67,
        "rssMb": 45.5
      }
    },
    {
      "id": "celery-redis",
      "engine": "Celery",
      "engineVersion": "5.6.3",
      "language": "Python 3.12.12",
      "backend": "redis",
      "concurrencyModel": "4 prefork processes (-c 4, Celery's default pool)",
      "enqueuePerSecond": 28.4,
      "drainPerSecond": 13,
      "backlogAtSubmitEnd": null,
      "latencyMs": {
        "p50": 10480.97,
        "p95": 19870.99,
        "p99": 20700.74,
        "max": 20914.94
      },
      "idle": {
        "cpuPct": 0.03,
        "rssMb": 212.2
      }
    },
    {
      "id": "dramatiq-redis",
      "engine": "Dramatiq",
      "engineVersion": "2.2.1",
      "language": "Python 3.12.12",
      "backend": "redis",
      "concurrencyModel": "1 process x 4 threads (--processes 1 --threads 4)",
      "enqueuePerSecond": 29.7,
      "drainPerSecond": 29.6,
      "backlogAtSubmitEnd": null,
      "latencyMs": {
        "p50": 52.23,
        "p95": 69.06,
        "p99": 70.95,
        "max": 421.04
      },
      "idle": {
        "cpuPct": 0.2,
        "rssMb": 53.6
      }
    },
    {
      "id": "rq-redis",
      "engine": "RQ",
      "engineVersion": "2.12.0",
      "language": "Python 3.12.12",
      "backend": "redis",
      "concurrencyModel": "4 `rq worker` processes (forks a child per job — RQ's default)",
      "enqueuePerSecond": 13.5,
      "drainPerSecond": 4.5,
      "backlogAtSubmitEnd": null,
      "latencyMs": {
        "p50": 34753.36,
        "p95": 68570.45,
        "p99": 72382.35,
        "max": 72997.78
      },
      "idle": {
        "cpuPct": 0,
        "rssMb": 131.5
      }
    },
    {
      "id": "bullmq-redis",
      "engine": "BullMQ",
      "engineVersion": "6.3.8",
      "language": "Node v24.12.0",
      "backend": "redis",
      "concurrencyModel": "4 concurrent jobs in one process (concurrency: 4)",
      "enqueuePerSecond": 28.8,
      "drainPerSecond": 28.8,
      "backlogAtSubmitEnd": null,
      "latencyMs": {
        "p50": 45.27,
        "p95": 160.87,
        "p99": 171.04,
        "max": 402.24
      },
      "idle": {
        "cpuPct": 0.03,
        "rssMb": 95.2
      }
    },
    {
      "id": "flexiq-node-redis",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Node v24.12.0",
      "backend": "redis",
      "concurrencyModel": "4 concurrent jobs in one process (concurrency: 4)",
      "enqueuePerSecond": 7.3,
      "drainPerSecond": 0.8,
      "backlogAtSubmitEnd": null,
      "latencyMs": {
        "p50": 285933.74,
        "p95": 543368.95,
        "p99": 557974.44,
        "max": 561141
      },
      "idle": {
        "cpuPct": 0.43,
        "rssMb": 95.5
      }
    },
    {
      "id": "flexiq-sqlite",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Python 3.12.12",
      "backend": "sqlite",
      "concurrencyModel": "4 worker threads (workers=4)",
      "enqueuePerSecond": 7098.5,
      "drainPerSecond": 142.2,
      "backlogAtSubmitEnd": null,
      "latencyMs": {
        "p50": 1770.01,
        "p95": 3288.74,
        "p99": 3397.84,
        "max": 3447.23
      },
      "idle": {
        "cpuPct": 0.27,
        "rssMb": 50.8
      }
    },
    {
      "id": "flexiq-node-sqlite",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Node v24.12.0",
      "backend": "sqlite",
      "concurrencyModel": "4 concurrent jobs in one process (concurrency: 4)",
      "enqueuePerSecond": 2012.9,
      "drainPerSecond": 761.8,
      "backlogAtSubmitEnd": null,
      "latencyMs": {
        "p50": 316,
        "p95": 433.27,
        "p99": 450.11,
        "max": 454.78
      },
      "idle": {
        "cpuPct": 0.2,
        "rssMb": 100.6
      }
    }
  ],
  "notes": [
    "Every entrant runs its workers and its producer as separate processes, submits serially, and is measured to completion rather than to enqueue.",
    "Concurrency 4 means a different thing to each entrant — see `concurrency_model` on every row.",
    "Each entrant runs with its own defaults. No tuning was applied to any of them.",
    "Redis is remote (Redis Cloud, ap-south-1), 34.45 ms average round trip. Every Redis-backed entrant pays that floor on every command, so the absolute numbers are a property of this link as much as of the engines. A colocated Redis moves all of them.",
    "The SQLite rows are a different deployment, not a faster one: a local file against a network service. They are here because no-broker is what FlexiQ is for, not so the local number can stand in for the networked one.",
    "The Redis server's eviction policy is `volatile-lru`, not `noeviction`. Under memory pressure it may drop queued jobs — that affects every Redis-backed entrant equally, but it is a durability caveat on the run rather than a performance one.",
    "One producer submitted faster than `flexiq-redis`, `celery-redis`, `dramatiq-redis`, `rq-redis`, `flexiq-node-redis`, `flexiq-sqlite`, `flexiq-node-sqlite` could drain, so a backlog built up during the run. Their latency percentiles are dominated by time spent queued behind it rather than by the cost of handling one job — read the completion rate as the primary result for those entrants, and the latency as its consequence. Every entrant received the identical load.",
    "The host was not idle at the start of the run (1-minute load average 1.37). Treat the absolute figures as a floor."
  ]
};
