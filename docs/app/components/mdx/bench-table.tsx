import { BENCH, type BenchRow } from "@/lib/bench-data";

/**
 * The comparison table, rendered from `bench/results/latest.json`.
 *
 * This component exists so that no benchmark figure in the docs is ever typed
 * by a human. The page this replaces carried a table of numbers attributed to
 * "public benchmarks and community reports" — flexiq at ~55,000/s against
 * Celery's ~5,000/s — that nobody had run and nobody could reproduce. Anything
 * shown here came out of `bench/run.py`, or it is not shown.
 *
 * `null` renders as "—", never as a zero and never as a blank that could be
 * mistaken for one.
 *
 * Two latency columns, on purpose. **p50/p99** come from the paced phase and
 * are service latency — what one caller waits. **p99 burst** is the same
 * measurement taken while 20,000 jobs are in flight, where it is dominated by
 * the queue in front of each job and is really throughput restated. Showing
 * only the second is the mistake this table was rebuilt to stop making.
 */

const num = (value: number | null, digits = 0): string =>
  value == null
    ? "—"
    : value.toLocaleString(undefined, {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
      });

function Row({ row }: { row: BenchRow }) {
  return (
    <tr>
      <td>
        {row.label}
        {row.configuration === "defaults" ? null : (
          <>
            <br />
            <small>
              <code>{row.configuration}</code>
            </small>
          </>
        )}
      </td>
      <td>{num(row.enqueuePerSecond)}</td>
      <td>
        {row.batchPerSecond == null ? (
          <abbr title={row.batchDeclined ?? "not measured"}>n/a</abbr>
        ) : (
          num(row.batchPerSecond)
        )}
      </td>
      <td>{num(row.completionPerSecond)}</td>
      <td>
        {num(row.p50Ms, 1)}
        {row.saturated ? (
          <abbr title="Could not hold the paced rate — queueing delay, not service latency">
            {" "}
            ⚠
          </abbr>
        ) : null}
      </td>
      <td>
        {num(row.p99Ms, 1)}
        {row.saturated ? (
          <abbr title="Could not hold the paced rate — queueing delay, not service latency">
            {" "}
            ⚠
          </abbr>
        ) : null}
      </td>
      <td>{num(row.burstP99Ms, 0)}</td>
      <td>{num(row.idleRssMb, 1)}</td>
      <td>{num(row.idleCpuPercent, 2)}</td>
    </tr>
  );
}

/**
 * @param scope `defaults` is the comparison — every system as it ships.
 *   `tuned` is the FlexiQ-only pair carrying its documented throughput knobs,
 *   which only means anything on a page that explains why it is FlexiQ-only.
 */
export function BenchTable({
  scope = "defaults",
}: {
  scope?: "defaults" | "tuned";
}) {
  const rows = BENCH.systems.filter((row) =>
    scope === "defaults"
      ? BENCH.defaults.includes(row.name)
      : !BENCH.defaults.includes(row.name),
  );

  return (
    <table>
      <thead>
        <tr>
          <th>System</th>
          <th>Enqueue/s</th>
          <th>Batch/s</th>
          <th>Completion/s</th>
          <th>p50 ms</th>
          <th>p99 ms</th>
          <th>p99 burst</th>
          <th>Idle MB</th>
          <th>Idle CPU</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => (
          <Row key={row.name} row={row} />
        ))}
      </tbody>
    </table>
  );
}

/** The provenance line: what machine, what day, what commit. */
export function BenchProvenance() {
  const { machine, scenario, measuredAt, commit } = BENCH;
  return (
    <p>
      <strong>{scenario.jobs.toLocaleString()} jobs</strong>, a{" "}
      {scenario.payload_bytes}-byte payload, concurrency {scenario.concurrency}.
      Measured {measuredAt.slice(0, 10)} on {machine.label} — {machine.cores}{" "}
      cores, {machine.cpu}, {machine.kernel} — from commit{" "}
      <code>{commit?.slice(0, 7)}</code>. {machine.notes}
    </p>
  );
}
