import { BENCH, type BenchRow } from "@/lib/bench-data";
import { SectionHead } from "./sections";

/**
 * The published benchmark, on the page that makes the claim.
 *
 * Every number here is read from `bench/results/latest.json` via
 * `scripts/sync-bench.mjs` — nothing on this page is typed by hand, and a
 * figure that is not in that file cannot appear. The link under each panel goes
 * to the harness that produced it, which is the only thing that makes a
 * published number worth anything.
 *
 * Four panels rather than one chart: jobs per second, milliseconds and
 * megabytes do not share an axis, and putting them on one would be the
 * dual-axis lie. Each panel is a single measure with its own scale and says
 * out loud which direction is good.
 *
 * Colour: FlexiQ carries the brand hue, every other system a neutral. That is
 * "the subject against the field", not a ranking — the neutral is deliberately
 * below the chroma floor a categorical palette would demand, because these are
 * not competing identities. Colour is never the only channel: every bar is
 * labelled with its own value, and the rows are named.
 */

interface Panel {
  key: keyof Pick<
    BenchRow,
    "enqueuePerSecond" | "completionPerSecond" | "p99Ms" | "idleRssMb"
  >;
  title: string;
  unit: string;
  better: "higher" | "lower";
  /** What the reader is actually looking at, in one line. */
  lead: string;
}

const PANELS: Panel[] = [
  {
    key: "enqueuePerSecond",
    title: "Enqueue",
    unit: "jobs/s",
    better: "higher",
    lead: "One call per job, producer side — what a request handler waits for.",
  },
  {
    key: "completionPerSecond",
    title: "Completion",
    unit: "jobs/s",
    better: "higher",
    lead: "Measured to completion, not to enqueue — the rate a backlog clears.",
  },
  {
    key: "p99Ms",
    title: "Latency p99",
    unit: "ms",
    better: "lower",
    lead: `Enqueue to completion, worst 1 in 100 — measured under a separate load paced at ${BENCH.scenario.latency_rate_per_second}/s so that nothing queues.`,
  },
  {
    key: "idleRssMb",
    title: "Idle memory",
    unit: "MB",
    better: "lower",
    lead: "Resident set of a worker with nothing to do, sampled before it has run a job.",
  },
];

/** Compact, and never lying about precision: 4.7k, 139, 0.9. */
function format(value: number): string {
  if (value >= 10_000) return `${(value / 1000).toFixed(1)}k`;
  if (value >= 100) return value.toFixed(0);
  if (value >= 10) return value.toFixed(1);
  return value.toFixed(2).replace(/0$/, "");
}

const isFlexiq = (row: BenchRow) => row.name.startsWith("flexiq");

function BenchPanel({ panel, rows }: { panel: Panel; rows: BenchRow[] }) {
  const values = rows
    .map((row) => row[panel.key])
    .filter((v): v is number => v != null);
  // Bars are shares of the largest value in *this* panel. A shared scale across
  // panels would be meaningless — they are different units.
  const peak = Math.max(...values, 0);

  return (
    <figure className="bench-panel">
      <figcaption>
        <h3>
          {panel.title} <span className="bench-unit">{panel.unit}</span>
        </h3>
        <p>{panel.lead}</p>
        <p className="bench-dir">{panel.better} is better</p>
      </figcaption>
      <ul className="bench-bars">
        {rows.map((row) => {
          const value = row[panel.key];
          return (
            <li
              key={row.name}
              className={isFlexiq(row) ? "is-subject" : undefined}
            >
              <span className="bench-name">
                {row.label}
                {/* A system that could not hold the paced rate is reporting
                    queueing delay, not service time. Marking it is not a
                    footnote — without it the column is quietly two different
                    measurements side by side. */}
                {panel.key === "p99Ms" && row.saturated ? (
                  <abbr title="Could not hold the paced rate — this is queueing delay, not service latency">
                    {" "}
                    ⚠
                  </abbr>
                ) : null}
              </span>
              <span className="bench-track">
                {value == null ? (
                  <span className="bench-absent">did not complete the run</span>
                ) : (
                  <span
                    className="bench-fill"
                    style={{
                      width: `${peak > 0 ? Math.max((value / peak) * 100, 1.5) : 0}%`,
                    }}
                  />
                )}
              </span>
              <span className="bench-value">
                {value == null ? "—" : format(value)}
              </span>
            </li>
          );
        })}
      </ul>
    </figure>
  );
}

export function BenchChart() {
  const rows = BENCH.defaults
    .map((name) => BENCH.systems.find((system) => system.name === name))
    .filter((row): row is BenchRow => row != null);

  return (
    <section className="section bench" id="benchmarks">
      <div className="wrap">
        <SectionHead
          kicker="Benchmarks"
          title={<>Numbers, and the script that made them</>}
          lead={`${BENCH.scenario.jobs.toLocaleString()} jobs, a ${BENCH.scenario.payload_bytes}-byte payload and concurrency ${BENCH.scenario.concurrency}, run against every system on its own defaults. FlexiQ does not win this on every axis, and the rows where it loses are here too.`}
        />

        <div className="bench-grid reveal">
          {PANELS.map((panel) => (
            <BenchPanel key={panel.key} panel={panel} rows={rows} />
          ))}
        </div>

        <p className="bench-machine">
          Measured on {BENCH.machine.label} — {BENCH.machine.cores} cores,{" "}
          {BENCH.machine.cpu}. {BENCH.machine.notes}
        </p>
        <p className="bench-links">
          <a href="https://github.com/ByteVeda/flexiq/tree/master/bench">
            The harness
          </a>{" "}
          ·{" "}
          <a href="https://github.com/ByteVeda/flexiq/blob/master/bench/results/latest.json">
            the raw results
          </a>{" "}
          ·{" "}
          <a href="/more/examples/benchmark">
            every figure, the tuned rows, and what this does not measure
          </a>
        </p>
      </div>
    </section>
  );
}
