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
  "runId": "202609220058543783",
  "generatedAt": "2026-09-21T19:56:33Z",
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
    "rttMsAvg": 38.64
  },
  "runtimes": [
    {
      "id": "flexiq-redis",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Python 3.12.12",
      "backend": "redis",
      "concurrencyModel": "4 worker threads (workers=4)",
      "enqueuePerSecond": 7.5,
      "drainPerSecond": 1.1,
      "backlogAtSubmitEnd": 435,
      "latencyMs": {
        "p50": 217307.73,
        "p95": 384329.61,
        "p99": 395772.68,
        "max": 402595.51
      },
      "idle": {
        "cpuPct": 0.47,
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
      "enqueuePerSecond": 29.2,
      "drainPerSecond": 14.2,
      "backlogAtSubmitEnd": 261,
      "latencyMs": {
        "p50": 9086.83,
        "p95": 17243.96,
        "p99": 17976.98,
        "max": 18159.83
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
      "enqueuePerSecond": 23.5,
      "drainPerSecond": 23.4,
      "backlogAtSubmitEnd": 1,
      "latencyMs": {
        "p50": 48.76,
        "p95": 150.85,
        "p99": 278.17,
        "max": 552.92
      },
      "idle": {
        "cpuPct": 0.13,
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
      "enqueuePerSecond": 13.9,
      "drainPerSecond": 4.5,
      "backlogAtSubmitEnd": 339,
      "latencyMs": {
        "p50": 37521.51,
        "p95": 71311.83,
        "p99": 74303.08,
        "max": 75221.71
      },
      "idle": {
        "cpuPct": 0,
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
      "enqueuePerSecond": 23.6,
      "drainPerSecond": 23.6,
      "backlogAtSubmitEnd": 1,
      "latencyMs": {
        "p50": 64.92,
        "p95": 155.6,
        "p99": 198.92,
        "max": 450.64
      },
      "idle": {
        "cpuPct": 0,
        "rssMb": 96.2
      }
    },
    {
      "id": "flexiq-node-redis",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Node v24.12.0",
      "backend": "redis",
      "concurrencyModel": "4 concurrent jobs in one process (concurrency: 4)",
      "enqueuePerSecond": 7.8,
      "drainPerSecond": 0.8,
      "backlogAtSubmitEnd": 447,
      "latencyMs": {
        "p50": 280714.27,
        "p95": 520533.6,
        "p99": 541480.8,
        "max": 544659.56
      },
      "idle": {
        "cpuPct": 0.47,
        "rssMb": 94.6
      }
    },
    {
      "id": "flexiq-sqlite",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Python 3.12.12",
      "backend": "sqlite",
      "concurrencyModel": "4 worker threads (workers=4)",
      "enqueuePerSecond": 5417.7,
      "drainPerSecond": 143.3,
      "backlogAtSubmitEnd": 484,
      "latencyMs": {
        "p50": 1702.36,
        "p95": 3247.31,
        "p99": 3346.9,
        "max": 3397.41
      },
      "idle": {
        "cpuPct": 0.27,
        "rssMb": 50.4
      }
    },
    {
      "id": "flexiq-node-sqlite",
      "engine": "FlexiQ",
      "engineVersion": "2.0.0",
      "language": "Node v24.12.0",
      "backend": "sqlite",
      "concurrencyModel": "4 concurrent jobs in one process (concurrency: 4)",
      "enqueuePerSecond": 4569.4,
      "drainPerSecond": 772.2,
      "backlogAtSubmitEnd": 481,
      "latencyMs": {
        "p50": 330.89,
        "p95": 526.82,
        "p99": 534.44,
        "max": 538.3
      },
      "idle": {
        "cpuPct": 0.2,
        "rssMb": 99.3
      }
    }
  ],
  "notes": [
    "Every entrant runs its workers and its producer as separate processes, submits serially, and is measured to completion rather than to enqueue.",
    "Concurrency 4 means a different thing to each entrant — see `concurrency_model` on every row.",
    "Each entrant runs with its own defaults. No tuning was applied to any of them.",
    "Redis is remote (Redis Cloud, ap-south-1), 38.64 ms average round trip. Every Redis-backed entrant pays that floor on every command, so the absolute numbers are a property of this link as much as of the engines. A colocated Redis moves all of them.",
    "The SQLite rows are a different deployment, not a faster one: a local file against a network service. They are here because no-broker is what FlexiQ is for, not so the local number can stand in for the networked one.",
    "The Redis server's eviction policy is `volatile-lru`, not `noeviction`. Under memory pressure it may drop queued jobs — that affects every Redis-backed entrant equally, but it is a durability caveat on the run rather than a performance one.",
    "One producer submits as fast as it can, so latency here is end-to-end under a load an entrant may not keep up with. Each row records `drain.backlog_at_submit_end` — how many of its jobs were still unfinished the moment submission stopped. Where that is a large share of the run, the percentiles are mostly time spent queued rather than the cost of handling one job, and the completion rate is the result to read. Every entrant received the identical load.",
    "The host was not idle at the start of the run (1-minute load average 1.29). Treat the absolute figures as a floor."
  ]
};
