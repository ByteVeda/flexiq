import { Link } from "react-router";
import { BenchmarkChart } from "@/components/diagrams";
import { BENCHMARK } from "@/lib/benchmark-data";
import { SectionHead } from "./sections";

/**
 * The evidence fold: the one place on this index that puts a number on the
 * speed claim, and the script that produced it in the same breath.
 *
 * It is here rather than on the marketing site because this is where a reader
 * who wants to check goes. Compact on purpose — the argument (what was
 * measured, what it does not show, where FlexiQ loses) belongs to
 * `about/benchmarks`, and the closing links hand off to it rather than
 * restating it, the way the server fold hands off to `/server`.
 *
 * The chart reads `docs/app/lib/benchmark-data.ts`, which is generated from
 * `bench/results/latest.json` by `scripts/sync-benchmarks.mjs`. Nothing on
 * this page is typed by hand, including the machine in the caption.
 */
export function BenchmarkFold() {
  const rtt = BENCHMARK.redis?.rttMsAvg;
  return (
    <section className="section bmf" id="benchmarks">
      <div className="wrap">
        <SectionHead
          kicker="Benchmarks"
          title={
            <>
              Numbers you can{" "}
              <span
                style={{
                  color: "var(--indigo-br)",
                  fontFamily: "var(--mono)",
                  fontSize: ".84em",
                }}
              >
                re-run
              </span>
            </>
          }
          lead={
            `One scenario — ${BENCHMARK.scenario.jobs} jobs, a fixed payload, fixed concurrency, ` +
            "measured to completion — against Celery, Dramatiq, RQ and BullMQ. Enqueue " +
            "throughput, latency percentiles and idle cost are reported separately, because " +
            "an engine that wins one can lose another."
          }
        />

        <div className="reveal">
          <BenchmarkChart />
        </div>

        <div className="bmf-note reveal">
          <p>
            FlexiQ does not win every axis, and the losses are published with
            the wins.
            {rtt
              ? ` Redis in this run is ${rtt} ms away, which every Redis-backed entrant pays on every command — a colocated server moves all of these numbers.`
              : null}
          </p>
          <div className="doclinks">
            <Link className="doclink" to="/about/benchmarks">
              What was measured, and what it does not show <Arrow />
            </Link>
            <a
              className="doclink"
              href="https://github.com/ByteVeda/flexiq/tree/master/bench"
            >
              Run it yourself <Arrow />
            </a>
          </div>
        </div>
      </div>
    </section>
  );
}

/** The `.doclink` chevron. Decorative — the link text carries the meaning. */
function Arrow() {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M5 12h14" />
      <path d="m12 5 7 7-7 7" />
    </svg>
  );
}
