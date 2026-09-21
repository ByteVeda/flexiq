#!/usr/bin/env node
// Generate the docs benchmark module from the committed results artifact.
//
// bench/results/latest.json is the single source of truth — it is written by
// bench/run.py and by nothing else. This script reshapes it into a typed TS
// module (docs/app/lib/benchmark-data.ts) that the landing fold and the
// benchmarks page import, so the chart cannot drift from the run that produced
// it: there is no second place to edit a number.
//
// Runs automatically before `docs` dev/build. `--check` fails instead of
// writing, which is what CI uses.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

if (process.argv.includes("--help") || process.argv.includes("-h")) {
  console.log(`Regenerate docs/app/lib/benchmark-data.ts from bench/results/latest.json.

usage: node scripts/sync-benchmarks.mjs [--check]

  --check   exit non-zero if the generated file is stale, and write nothing

The artifact is produced by \`bench/run.py\`; see bench/README.md. Runs
automatically before \`docs\` dev/build.`);
  process.exit(0);
}

const check = process.argv.includes("--check");
const repoRoot = new URL("../", import.meta.url);
const source = fileURLToPath(new URL("bench/results/latest.json", repoRoot));
const target = fileURLToPath(new URL("docs/app/lib/benchmark-data.ts", repoRoot));

// The schema the artifact declares. A consumer that silently read a shape it
// was not written for would put wrong numbers on the landing page, which is
// the one failure this whole exercise exists to prevent.
const SUPPORTED_SCHEMA = 1;

let artifact;
try {
  artifact = JSON.parse(readFileSync(source, "utf8"));
} catch (error) {
  console.error(
    `sync-benchmarks: cannot read ${source}.\n` +
      "Produce it with `uv run --project bench python bench/run.py` — see bench/README.md.",
  );
  throw error;
}

if (artifact.schema !== SUPPORTED_SCHEMA) {
  throw new Error(
    `sync-benchmarks: artifact schema ${artifact.schema}, expected ${SUPPORTED_SCHEMA}`,
  );
}

/** Rows that failed are dropped here rather than rendered as a gap in the chart. */
const runtimes = artifact.runtimes
  .filter((runtime) => !runtime.error)
  .map((runtime) => ({
    id: runtime.id,
    engine: runtime.engine,
    engineVersion: runtime.engine_version,
    language: runtime.language,
    backend: runtime.backend,
    concurrencyModel: runtime.concurrency_model,
    enqueuePerSecond: runtime.enqueue.per_second,
    drainPerSecond: runtime.drain.per_second,
    latencyMs: {
      p50: runtime.latency_ms.p50,
      p95: runtime.latency_ms.p95,
      p99: runtime.latency_ms.p99,
      max: runtime.latency_ms.max,
    },
    idle: { cpuPct: runtime.idle.cpu_pct, rssMb: runtime.idle.rss_mb },
  }));

if (runtimes.length === 0) {
  throw new Error("sync-benchmarks: the artifact has no successful runtime rows");
}

const redis = artifact.backends?.redis ?? null;
const data = {
  runId: artifact.run_id,
  generatedAt: artifact.generated_at,
  source: artifact.source,
  scenario: {
    jobs: artifact.scenario.jobs,
    payloadBytes: artifact.scenario.payload_bytes,
    concurrency: artifact.scenario.concurrency,
    warmupJobs: artifact.scenario.warmup_jobs,
  },
  machine: {
    os: artifact.machine.os,
    cpu: artifact.machine.cpu,
    cores: artifact.machine.cores,
    ramGb: artifact.machine.ram_gb,
    python: artifact.machine.python,
    node: artifact.machine.node,
  },
  redis: redis && {
    kind: redis.kind,
    provider: redis.provider,
    region: redis.region,
    version: redis.version,
    rttMsAvg: redis.rtt_ms?.avg ?? null,
  },
  runtimes,
  notes: artifact.notes ?? [],
};

// `--check` compares bytes, so the file has to be byte-identical to what this
// script emits — which means Biome must not reformat it afterwards. It is
// excluded in docs/biome.json for that reason, the way `content` and
// `public/demos` are; a generated file the formatter also owns would make the
// staleness gate fail on formatting rather than on staleness.
const banner =
  "// AUTO-GENERATED from /bench/results/latest.json by scripts/sync-benchmarks.mjs — do not edit directly.\n" +
  "// The numbers come from `bench/run.py`; to change them, re-run the harness and commit its artifact.\n";

const types = `
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
`;

const body = `${banner}${types}
export const BENCHMARK: BenchmarkRun = ${JSON.stringify(data, null, 2)};
`;

if (check) {
  let current = "";
  try {
    current = readFileSync(target, "utf8");
  } catch {
    current = "";
  }
  if (current !== body) {
    console.error(
      `sync-benchmarks: ${target} is stale. Run \`node scripts/sync-benchmarks.mjs\`.`,
    );
    process.exit(1);
  }
  console.log(`${target} is up to date`);
  process.exit(0);
}

writeFileSync(target, body);
console.log(`synced ${target} from bench/results/latest.json (${runtimes.length} runtimes)`);
