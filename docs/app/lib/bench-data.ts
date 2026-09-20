// The shape of ./bench-data.json, which is generated from
// /bench/results/latest.json by scripts/sync-bench.mjs and runs before every
// docs dev/build. Rerun the benchmark to change the numbers:
// `python bench/run.py --publish`.
//
// The data is JSON and these types are hand-written, rather than one generated
// .ts carrying both: a generated TypeScript file has to come out byte-identical
// to whatever biome would format it into, or `pnpm lint` fails on a file nobody
// edited. This module is formatted once and then left alone.
import data from "./bench-data.json";

export interface BenchRow {
  name: string;
  label: string;
  configuration: string;
  language: string;
  backend: string;
  concurrencyModel: string;
  versions: Record<string, string | null>;
  enqueuePerSecond: number | null;
  batchPerSecond: number | null;
  batchDeclined: string | null;
  completionPerSecond: number | null;
  /** Service latency, from the paced phase — what one caller waits. */
  p50Ms: number | null;
  p90Ms: number | null;
  p99Ms: number | null;
  pacedRate: number | null;
  /** The paced phase only means something while nothing queues. True when this
   *  system could not hold the offered rate, which makes the percentiles above
   *  queueing delay instead — every surface that prints them must mark it. */
  saturated: boolean | null;
  /** The same measurement under the burst: throughput restated, kept because
   *  the contrast with the paced figure is the instructive part. */
  burstP99Ms: number | null;
  /** Sampled before the worker has run a job: provisioning cost, not memory
   *  retained after a drain. See bench/README.md. */
  idleRssMb: number | null;
  idleCpuPercent: number | null;
  declined: string | null;
}

export interface BenchData {
  measuredAt: string;
  machine: {
    label: string;
    notes: string;
    cpu: string;
    cores: number;
    ramGb: number | null;
    kernel: string;
  };
  commit: string;
  scenario: {
    name: string;
    jobs: number;
    payload_bytes: number;
    concurrency: number;
    latency_jobs: number;
    latency_rate_per_second: number;
    [key: string]: unknown;
  };
  /** Names of the rows that ran on stock configuration — the comparison proper. */
  defaults: string[];
  systems: BenchRow[];
}

// Through `unknown`: TypeScript infers a distinct literal type per row from the
// JSON, and each row's `versions` names only the libraries that row actually
// used, so the inferred union is not assignable to `Record<string, string>`.
// The guarantee that this shape is right comes from the generator and from
// `validate()` in bench/harness/report.py, not from structural inference over
// one committed file.
export const BENCH = data as unknown as BenchData;
