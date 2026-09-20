#!/usr/bin/env node
// Generate the docs' benchmark data module from the committed results file.
//
// bench/results/latest.json is the single source of truth — it is written by
// `python bench/run.py --publish` and edited by nobody. This script narrows it
// to what the landing chart and the docs table render, and emits
// docs/app/lib/bench-data.json. Run automatically before `docs` dev/build, the
// same way scripts/sync-changelog.mjs runs.
//
// It also rewrites the marked block in the root README.md, for the same reason:
// the README is where the performance claim is made, and a number typed there
// by hand is a number that goes stale the next time the benchmark runs.
//
// Why generate rather than import the JSON directly: the results file lives
// outside the docs Vite root, and a chart that reads whatever shape happens to
// be in it would render silently wrong when the schema moves. Here, an
// unexpected schema is a build failure with a line number.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

if (process.argv.includes("--help") || process.argv.includes("-h")) {
  console.log(`Regenerate docs/app/lib/bench-data.json from bench/results/latest.json.

usage: node scripts/sync-bench.mjs

Takes no options. The output is generated — rerun the benchmark
(\`python bench/run.py --publish\`) instead of editing it.
Runs automatically before \`docs\` dev/build.`);
  process.exit(0);
}

/** Bumped in lockstep with SCHEMA in bench/harness/report.py. */
const SCHEMA = 1;

const repoRoot = new URL("../", import.meta.url);
const source = fileURLToPath(new URL("bench/results/latest.json", repoRoot));
const target = fileURLToPath(new URL("docs/app/lib/bench-data.json", repoRoot));
const readmeTarget = fileURLToPath(new URL("README.md", repoRoot));

const results = JSON.parse(readFileSync(source, "utf8"));
if (results.schema !== SCHEMA) {
  throw new Error(
    `sync-bench: ${source} is schema ${results.schema}, this script knows schema ${SCHEMA}. ` +
      "Update both together — a chart rendered from fields that moved is wrong without failing.",
  );
}

/** One row, flattened. `null` survives as null: a system that failed its run
 *  renders as a gap that says so, never as a zero. */
const row = ([name, system]) => ({
  name,
  label: system.label,
  configuration: system.configuration,
  language: system.language,
  backend: system.backend,
  concurrencyModel: system.concurrency_model,
  versions: system.versions,
  enqueuePerSecond: system.enqueue?.per_job?.per_second ?? null,
  batchPerSecond: system.enqueue?.batch?.per_second ?? null,
  batchDeclined: system.enqueue?.batch_declined ?? null,
  completionPerSecond: system.drain?.completion_per_second ?? null,
  // Service latency, from the paced phase — what one caller waits.
  p50Ms: system.latency?.latency_ms?.p50 ?? null,
  p90Ms: system.latency?.latency_ms?.p90 ?? null,
  p99Ms: system.latency?.latency_ms?.p99 ?? null,
  pacedRate: system.latency?.achieved_rate_per_second ?? null,
  // True when the system could not hold the offered rate, which makes the
  // three figures above queueing delay rather than service time. Every surface
  // that prints them has to say so.
  saturated: system.latency?.saturated ?? null,
  // Queueing delay under the burst — throughput restated, kept because the
  // contrast with the paced figure is the most instructive thing here.
  burstP99Ms: system.drain?.latency_ms?.p99 ?? null,
  idleRssMb: system.idle?.rss_mb_mean ?? null,
  idleCpuPercent: system.idle?.cpu_percent ?? null,
  declined: system.enqueue_declined ?? null,
});

const rows = Object.entries(results.systems).map(row);
const defaults = rows.filter((r) => r.configuration === "defaults").map((r) => r.name);

const payload = {
  measuredAt: results.machine.measured_at,
  machine: {
    label: results.machine.label,
    notes: results.machine.notes,
    cpu: results.machine.cpu_model,
    cores: results.machine.cpu_count,
    ramGb: results.machine.ram_gb,
    kernel: results.machine.kernel,
  },
  commit: results.machine.git.commit,
  scenario: results.scenario,
  // The comparison is the default-configuration rows. The tuned FlexiQ rows are
  // in `systems` too, and the page that shows them has to say why they exist.
  defaults,
  systems: rows,
};

// Data only, and JSON rather than TypeScript on purpose: a generated .ts file
// has to come out byte-identical to what biome would format it into, or the
// docs lint fails on a file no human touched. The types live in bench-data.ts
// beside it, hand-written and formatted once.
writeFileSync(target, `${JSON.stringify(payload, null, 2)}\n`);
console.log(`synced ${target} from bench/results/latest.json (${rows.length} systems)`);

// ── README ────────────────────────────────────────────────────────
// The block between the markers is generated; everything around it is prose
// somebody wrote. Losing the markers is a hard error rather than a silent
// skip — a README that quietly stops updating is how the last set of
// unsupported numbers survived as long as it did.
const START = "<!-- bench:start -->";
const END = "<!-- bench:end -->";

/** Thousands get separators and no decimals; smaller numbers keep one, so that
 *  a sub-millisecond p50 does not round away to "1" and a 3,430 ms one does not
 *  arrive with three decimals of false precision. */
const compact = (value) => {
  if (value == null) return "—";
  return value >= 1000
    ? Math.round(value).toLocaleString("en-US")
    : value.toFixed(1);
};

const readmeRows = payload.defaults
  .map((name) => payload.systems.find((s) => s.name === name))
  .filter(Boolean)
  .map(
    (r) =>
      `| ${r.label} | ${compact(r.enqueuePerSecond)} | ${compact(r.completionPerSecond)} | ${compact(r.p50Ms)}${r.saturated ? " ⚠" : ""} | ${compact(r.p99Ms)}${r.saturated ? " ⚠" : ""} | ${compact(r.idleRssMb)} |`,
  )
  .join("\n");

const block = [
  START,
  "",
  `<!-- Generated by scripts/sync-bench.mjs from bench/results/latest.json. Rerun \`python bench/run.py --publish\` to change it. -->`,
  "",
  `${payload.scenario.jobs.toLocaleString("en-US")} jobs, a ${payload.scenario.payload_bytes}-byte payload and concurrency ${payload.scenario.concurrency}, every system on its own defaults, measured on ${payload.machine.label}. Throughput is from a burst; the percentiles are from a separate phase paced at ${payload.scenario.latency_rate_per_second}/s, so they are what one caller waits rather than the backlog in front of it:`,
  "",
  "| System | Enqueue/s | Completion/s | p50 ms | p99 ms | Idle MB |",
  "|---|---:|---:|---:|---:|---:|",
  readmeRows,
  "",
  ...(payload.systems.some((r) => r.saturated)
    ? [
        "⚠ marks a system that could not hold the paced rate — its percentiles are queueing delay, not service time, and should be read as a second throughput result.",
        "",
      ]
    : []),
  `FlexiQ does not win this on every axis. The rows where it loses are in the table above, the harness that produced them is in [\`bench/\`](bench/), and the raw numbers — including what was measured on what kernel from which commit — are in [\`bench/results/\`](bench/results/). ${payload.machine.notes}`,
  "",
  END,
].join("\n");

const readme = readFileSync(readmeTarget, "utf8");
const from = readme.indexOf(START);
const to = readme.indexOf(END);
if (from === -1 || to === -1) {
  throw new Error(
    `sync-bench: ${readmeTarget} has no ${START} / ${END} block. ` +
      "The benchmark numbers in the README are generated; restore the markers rather than typing them.",
  );
}
writeFileSync(readmeTarget, readme.slice(0, from) + block + readme.slice(to + END.length));
console.log(`synced ${readmeTarget} benchmark block`);
