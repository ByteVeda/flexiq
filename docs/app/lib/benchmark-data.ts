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
  "runId": "202609212242306084",
  "generatedAt": "2026-09-21T17:41:03Z",
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
    "rttMsAvg": 38.48
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
      "drainPerSecond": 0.9,
      "latencyMs": {
        "p50": 276349.88,
        "p95": 446386.55,
        "p99": 460287,
        "max": 468368.52
      },
      "idle": {
        "cpuPct": 1.03,
        "rssMb": 45.7
      }
    },
    {
      "id": "celery-redis",
      "engine": "Celery",
      "engineVersion": "5.6.3",
      "language": "Python 3.12.12",
      "backend": "redis",
      "concurrencyModel": "4 prefork processes (-c 4, Celery's default pool)",
      "enqueuePerSecond": 25.1,
      "drainPerSecond": 12.7,
      "latencyMs": {
        "p50": 9814.72,
        "p95": 18514.36,
        "p99": 19244.92,
        "max": 19434.76
      },
      "idle": {
        "cpuPct": 0.07,
        "rssMb": 212.3
      }
    },
    {
      "id": "dramatiq-redis",
      "engine": "Dramatiq",
      "engineVersion": "2.2.1",
      "language": "Python 3.12.12",
      "backend": "redis",
      "concurrencyModel": "1 process x 4 threads (--processes 1 --threads 4)",
      "enqueuePerSecond": 23,
      "drainPerSecond": 23,
      "latencyMs": {
        "p50": 44.16,
        "p95": 146.26,
        "p99": 271.49,
        "max": 628.9
      },
      "idle": {
        "cpuPct": 0.23,
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
      "enqueuePerSecond": 14.5,
      "drainPerSecond": 4.5,
      "latencyMs": {
        "p50": 37544.43,
        "p95": 71623.95,
        "p99": 74647.36,
        "max": 75703.81
      },
      "idle": {
        "cpuPct": 0.06,
        "rssMb": 131.7
      }
    },
    {
      "id": "bullmq-redis",
      "engine": "BullMQ",
      "engineVersion": "6.3.8",
      "language": "Node v24.12.0",
      "backend": "redis",
      "concurrencyModel": "4 concurrent jobs in one process (concurrency: 4)",
      "enqueuePerSecond": 28.2,
      "drainPerSecond": 28.2,
      "latencyMs": {
        "p50": 46.29,
        "p95": 122.53,
        "p99": 172.1,
        "max": 521.04
      },
      "idle": {
        "cpuPct": 0,
        "rssMb": 93.4
      }
    },
    {
      "id": "flexiq-node-redis",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Node v24.12.0",
      "backend": "redis",
      "concurrencyModel": "4 concurrent jobs in one process (concurrency: 4)",
      "enqueuePerSecond": 7.2,
      "drainPerSecond": 0.8,
      "latencyMs": {
        "p50": 275539.8,
        "p95": 515294.06,
        "p99": 532517.03,
        "max": 535834.85
      },
      "idle": {
        "cpuPct": 0.4,
        "rssMb": 99
      }
    },
    {
      "id": "flexiq-sqlite",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Python 3.12.12",
      "backend": "sqlite",
      "concurrencyModel": "4 worker threads (workers=4)",
      "enqueuePerSecond": 5734.5,
      "drainPerSecond": 143.6,
      "latencyMs": {
        "p50": 1703.51,
        "p95": 3239.74,
        "p99": 3345.15,
        "max": 3393.96
      },
      "idle": {
        "cpuPct": 0.27,
        "rssMb": 50.5
      }
    },
    {
      "id": "flexiq-node-sqlite",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Node v24.12.0",
      "backend": "sqlite",
      "concurrencyModel": "4 concurrent jobs in one process (concurrency: 4)",
      "enqueuePerSecond": 7690.2,
      "drainPerSecond": 745.6,
      "latencyMs": {
        "p50": 347.71,
        "p95": 582.81,
        "p99": 602.28,
        "max": 605.89
      },
      "idle": {
        "cpuPct": 0.2,
        "rssMb": 104.6
      }
    }
  ],
  "notes": [
    "Every entrant runs its workers and its producer as separate processes, submits serially, and is measured to completion rather than to enqueue.",
    "Concurrency 4 means a different thing to each entrant — see `concurrency_model` on every row.",
    "Each entrant runs with its own defaults. No tuning was applied to any of them.",
    "Redis is remote (Redis Cloud, ap-south-1), 38.48 ms average round trip. Every Redis-backed entrant pays that floor on every command, so the absolute numbers are a property of this link as much as of the engines. A colocated Redis moves all of them.",
    "The SQLite rows are a different deployment, not a faster one: a local file against a network service. They are here because no-broker is what FlexiQ is for, not so the local number can stand in for the networked one.",
    "The Redis server's eviction policy is `volatile-lru`, not `noeviction`. Under memory pressure it may drop queued jobs — that affects every Redis-backed entrant equally, but it is a durability caveat on the run rather than a performance one.",
    "The host was not idle at the start of the run (1-minute load average 1.11). Treat the absolute figures as a floor."
  ]
};
