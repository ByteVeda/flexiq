import {
  CodeFigure,
  FlowDiagram,
  type FlowStage,
  PoolFigure,
  SchedulerFigure,
  StoreFigure,
} from "./flow-diagram";

export function SectionHead({
  kicker,
  title,
  lead,
}: {
  kicker: string;
  title: React.ReactNode;
  lead?: string;
}) {
  return (
    <div className="section-head reveal">
      <div className="kicker">{kicker}</div>
      <h2>{title}</h2>
      {lead ? <p>{lead}</p> : null}
    </div>
  );
}

/**
 * The four participants, and the one line each that a reader would otherwise
 * have to open a guide to learn.
 *
 * Written from the code, not from the pitch: `.delay()` returns a `JobResult`
 * handle (`sdks/python/flexiq/task.py`), the scheduler claims in batches
 * (`Storage::claim_execution_batch`) and caps concurrent work per task
 * (`SchedulerConfig::max_in_flight`), and the result goes back to the same row
 * the enqueue wrote — which is the whole "no broker in the middle" claim.
 */
const STAGES: FlowStage[] = [
  {
    label: "your code",
    title: "enqueue",
    sub: "python · node · java",
    figure: <CodeFigure />,
    wire: "one write",
    lines: [
      { v: "job = add.delay(2, 3)", code: true },
      { v: "returns a handle, not the answer" },
      { v: "nothing to point at a broker" },
    ],
  },
  {
    label: "the queue",
    title: "jobs",
    sub: "SQLite · PG",
    figure: <StoreFigure />,
    wire: "claim",
    lines: [
      { k: "task", v: "add" },
      { k: "status", v: "pending" },
      { v: "one row, whichever SDK wrote it" },
    ],
  },
  {
    label: "scheduler",
    title: "dispatch",
    sub: "Rust · Tokio",
    accent: true,
    figure: <SchedulerFigure />,
    wire: "run",
    lines: [
      { v: "claim_execution_batch()", code: true },
      { v: "a batch per poll, not a job" },
      { v: "max_in_flight caps the claim" },
    ],
  },
  {
    label: "workers",
    title: "execute",
    sub: "6 · pool",
    accent: true,
    figure: <PoolFigure />,
    lines: [
      { v: "add(2, 3) → 5", code: true },
      { v: "your function, in your process" },
      { v: "retries and dead-letter on failure" },
    ],
  },
];

export function HowItWorks() {
  return (
    <section className="section how">
      <div className="wrap">
        <SectionHead
          kicker="How it works"
          title={
            <>
              From{" "}
              <span
                style={{
                  color: "var(--indigo-br)",
                  fontFamily: "var(--mono)",
                  fontSize: ".84em",
                }}
              >
                .delay()
              </span>{" "}
              to result
            </>
          }
          lead="Your application code enqueues a job. The Rust scheduler hands it to a worker. The result lands back in the shared store — same core, same queue, no broker in the middle, whichever SDK you called it from."
        />
        <div className="diagram reveal">
          <FlowDiagram
            stages={STAGES}
            returnLabel="the result is written back to that same row — your handle reads it there"
          />
        </div>
      </div>
    </section>
  );
}
