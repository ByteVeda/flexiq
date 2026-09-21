/**
 * BullMQ on Redis.
 *
 * Stock settings throughout: completed jobs stay in Redis because that is
 * BullMQ's default, and the cleanup pass removes them afterwards rather than
 * `removeOnComplete` quietly buying BullMQ a round trip the others still pay.
 */

import { Queue, Worker } from "bullmq";

import { handle, parseArgs, produce, redisClient, runUntilSignalled } from "./shared.mjs";

const QUEUE = process.env.BENCH_QUEUE;
const TASK = process.env.BENCH_TASK;

const { command, first, count } = parseArgs(process.argv.slice(2));

if (command === "worker") {
  const worker = new Worker(QUEUE, async (job) => handle(job.data), {
    connection: redisClient(),
    concurrency: Number(process.env.BENCH_CONCURRENCY),
  });
  runUntilSignalled(() => worker.close());
} else if (command === "produce") {
  const queue = new Queue(QUEUE, { connection: redisClient() });
  await produce(first, count, async (payload) => (await queue.add(TASK, payload)).id);
  await queue.close();
  // `close()` settles, but BullMQ leaves enough on the loop that the process
  // does not end on its own. A producer that lingers is a producer the next
  // phase waits on, so it leaves under its own power.
  process.exit(0);
} else {
  process.stderr.write(`usage: bullmq.mjs worker | produce --first N --count M\n`);
  process.exit(2);
}
