import { useState } from "react";
import { BENCHMARK, type BenchmarkRuntime } from "@/lib/benchmark-data";

/**
 * The published benchmark, drawn from the committed artifact and nothing else.
 *
 * Every value here came out of `bench/results/latest.json`, which came out of
 * `bench/run.py`. There is no hand-written number on this page, which is the
 * whole point: a performance claim a reader cannot reproduce is worth less
 * than no claim.
 *
 * Bars are decorative and marked `aria-hidden`. The figure a reader needs is
 * text beside them, so the chart degrades to a readable list with styles off
 * and reads correctly to a screen reader without an ARIA chart vocabulary
 * nobody implements consistently.
 *
 * Two groups, never one ranking. The Redis entrants share a backend and a
 * network, so they are comparable with each other; the SQLite rows are a
 * different deployment — a local file against a network service — and merging
 * them into one bar chart would be the exact sleight of hand this harness
 * exists to avoid.
 */

type MetricId = "enqueue" | "completion" | "p99" | "idle";

interface Metric {
  id: MetricId;
  tab: string;
  heading: string;
  unit: string;
  /** Whether a bigger bar is a better result — drives the ordering and the note. */
  higherIsBetter: boolean;
  value: (runtime: BenchmarkRuntime) => number;
  format: (value: number) => string;
}

const METRICS: Metric[] = [
  {
    id: "completion",
    tab: "Completion",
    heading: "Jobs completed per second",
    unit: "jobs/s",
    higherIsBetter: true,
    value: (r) => r.drainPerSecond,
    format: (v) => v.toLocaleString(undefined, { maximumFractionDigits: 1 }),
  },
  {
    id: "enqueue",
    tab: "Enqueue",
    heading: "Jobs submitted per second",
    unit: "jobs/s",
    higherIsBetter: true,
    value: (r) => r.enqueuePerSecond,
    format: (v) => v.toLocaleString(undefined, { maximumFractionDigits: 1 }),
  },
  {
    id: "p99",
    tab: "Latency p99",
    heading: "End-to-end latency, 99th percentile",
    unit: "ms",
    higherIsBetter: false,
    value: (r) => r.latencyMs.p99,
    format: (v) => v.toLocaleString(undefined, { maximumFractionDigits: 0 }),
  },
  {
    id: "idle",
    tab: "Idle cost",
    heading: "CPU while the queue is empty",
    unit: "% of one core",
    higherIsBetter: false,
    value: (r) => r.idle.cpuPct,
    format: (v) => v.toFixed(2),
  },
];

/** FlexiQ's own rows are marked, so a reader can see which entrant is ours. */
function isOurs(runtime: BenchmarkRuntime): boolean {
  return runtime.engine === "FlexiQ";
}

function ordered(
  runtimes: BenchmarkRuntime[],
  metric: Metric,
): BenchmarkRuntime[] {
  return [...runtimes].sort((a, b) =>
    metric.higherIsBetter
      ? metric.value(b) - metric.value(a)
      : metric.value(a) - metric.value(b),
  );
}

function Bars({
  runtimes,
  metric,
  labelledBy,
}: {
  runtimes: BenchmarkRuntime[];
  metric: Metric;
  labelledBy: string;
}) {
  const rows = ordered(runtimes, metric);
  // Scale to the largest bar in *this* group, never across groups: a shared
  // scale would squash the Redis rows against a local-disk number.
  const peak = Math.max(...rows.map(metric.value), 1);
  return (
    <ul className="bm-bars" aria-labelledby={labelledBy}>
      {rows.map((runtime) => (
        <li className="bm-row" key={runtime.id}>
          <span className="bm-label" title={runtime.concurrencyModel}>
            {runtime.engine}
            <span className="bm-sub">{runtime.language.split(" ")[0]}</span>
          </span>
          <span className="bm-track" aria-hidden="true">
            <span
              className={`bm-fill${isOurs(runtime) ? " ours" : ""}`}
              style={{
                width: `${Math.max(1.5, (metric.value(runtime) / peak) * 100)}%`,
              }}
            />
          </span>
          <span className="bm-value">
            {metric.format(metric.value(runtime))}
            <span className="bm-unit">{metric.unit}</span>
          </span>
        </li>
      ))}
    </ul>
  );
}

export function BenchmarkChart() {
  const [active, setActive] = useState<MetricId>("completion");
  const metric = METRICS.find((m) => m.id === active) ?? METRICS[0];
  const shared = BENCHMARK.runtimes.filter((r) => r.backend === "redis");
  const local = BENCHMARK.runtimes.filter((r) => r.backend !== "redis");
  const redis = BENCHMARK.redis;

  return (
    <figure className="bm">
      {/* `aria-pressed` toggles rather than an ARIA tablist: what changes
          below is not a tab panel, and the hero's language switcher already
          sets this precedent on the same site. No grouping role — the heading
          underneath names the selected metric, so the group would add an ARIA
          landmark with nothing to say. */}
      <div className="bm-tabs">
        {METRICS.map((entry) => (
          <button
            key={entry.id}
            type="button"
            aria-pressed={entry.id === active}
            className={`bm-tab${entry.id === active ? " active" : ""}`}
            onClick={() => setActive(entry.id)}
          >
            {entry.tab}
          </button>
        ))}
      </div>

      <p className="bm-heading">
        {metric.heading}
        <span className="bm-dir">
          {metric.higherIsBetter ? "higher is better" : "lower is better"}
        </span>
      </p>

      {/* Labels, not headings. The figure sits under an `h2` on the landing
          page and under the `h1` on the benchmarks page, so any heading level
          chosen here skips one somewhere — and these caption two groups of
          bars rather than structuring the document. The `ul` carries the same
          text as its accessible name, so the grouping still reaches a screen
          reader. */}
      {shared.length > 0 ? (
        <div className="bm-group">
          <p className="bm-group-title" id="bm-group-shared">
            Same backend — every entrant on one Redis
            {redis?.rttMsAvg ? (
              <span className="bm-note">{redis.rttMsAvg} ms away</span>
            ) : null}
          </p>
          <Bars
            runtimes={shared}
            metric={metric}
            labelledBy="bm-group-shared"
          />
        </div>
      ) : null}

      {local.length > 0 ? (
        <div className="bm-group">
          <p className="bm-group-title" id="bm-group-local">
            No broker — FlexiQ on a local SQLite file
            <span className="bm-note">
              a different deployment, not a faster entrant
            </span>
          </p>
          <Bars runtimes={local} metric={metric} labelledBy="bm-group-local" />
        </div>
      ) : null}

      <figcaption className="bm-caption">
        {BENCHMARK.scenario.jobs} jobs · {BENCHMARK.scenario.payloadBytes}-byte
        payload · concurrency {BENCHMARK.scenario.concurrency} ·{" "}
        {BENCHMARK.machine.cores} cores, {BENCHMARK.machine.ramGb} GB · run{" "}
        {BENCHMARK.generatedAt}. Produced by{" "}
        <a href="https://github.com/ByteVeda/flexiq/blob/master/bench/run.py">
          <code>bench/run.py</code>
        </a>
        .
      </figcaption>
    </figure>
  );
}

/** The caveats the harness wrote into the artifact, rendered verbatim. */
export function BenchmarkNotes() {
  return (
    <ul className="bm-notes">
      {BENCHMARK.notes.map((note) => (
        <li key={note}>{note}</li>
      ))}
    </ul>
  );
}

/** Every measured column, for the page that argues the numbers rather than shows them. */
export function BenchmarkTable() {
  return (
    <div className="bm-table-wrap">
      <table className="bm-table">
        <thead>
          <tr>
            <th>Entrant</th>
            <th>Backend</th>
            <th>Concurrency</th>
            <th>Enqueue/s</th>
            <th>Completed/s</th>
            <th>p50</th>
            <th>p95</th>
            <th>p99</th>
            <th>Idle CPU</th>
            <th>Idle RSS</th>
          </tr>
        </thead>
        <tbody>
          {BENCHMARK.runtimes.map((runtime) => (
            <tr key={runtime.id}>
              <td>
                {runtime.engine} {runtime.engineVersion}
                <span className="bm-sub">{runtime.language}</span>
              </td>
              <td>{runtime.backend}</td>
              <td>{runtime.concurrencyModel}</td>
              <td>{runtime.enqueuePerSecond}</td>
              <td>{runtime.drainPerSecond}</td>
              <td>{runtime.latencyMs.p50} ms</td>
              <td>{runtime.latencyMs.p95} ms</td>
              <td>{runtime.latencyMs.p99} ms</td>
              <td>{runtime.idle.cpuPct}%</td>
              <td>{runtime.idle.rssMb} MB</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
