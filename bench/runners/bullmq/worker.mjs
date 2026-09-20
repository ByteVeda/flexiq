// BullMQ's worker: one Node process holding `concurrency` jobs at once.
//
// That is BullMQ's own concurrency model and it is genuinely not the same shape
// as Celery's N processes or FlexiQ's N threads — an event loop cannot run two
// job bodies in parallel. For a body that does one Redis write it is a fair
// comparison; for a CPU-bound body it would not be, and `bench/README.md` says
// so rather than letting the table imply otherwise.

import { Worker } from "bullmq";
import { connect, record } from "./sink.mjs";

const system = process.env.BENCH_SYSTEM ?? "bullmq";
const concurrency = Number(process.env.BENCH_CONCURRENCY ?? "4");
const sink = connect(process.env.BENCH_SINK_URL);

const worker = new Worker(
  "bench",
  async (job) => {
    const { seq, enqueuedNs, body } = job.data;
    if (!body || body.length < 1) throw new Error("empty payload");
    await record(sink, system, seq, enqueuedNs);
  },
  {
    connection: { url: process.env.BENCH_BULLMQ_URL },
    concurrency,
    // Nothing keeps a finished job: the sink is the only record of a
    // completion, the same as everywhere else in this harness.
    removeOnComplete: { count: 0 },
    removeOnFail: { count: 0 },
  },
);

worker.on("error", (err) => {
  console.error("bullmq worker error:", err);
});

const stop = async () => {
  await worker.close();
  await sink.quit();
  process.exit(0);
};
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
