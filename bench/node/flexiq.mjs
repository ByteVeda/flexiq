/**
 * FlexiQ from the Node SDK, on SQLite and on Redis.
 *
 * Here so BullMQ is compared against FlexiQ in its own runtime. A Python
 * FlexiQ against a Node BullMQ would confound the engine with the language,
 * and the engine is the thing under test.
 */

import { Queue } from "@byteveda/flexiq";

import { handle, parseArgs, produce, runUntilSignalled } from "./shared.mjs";

const QUEUE = process.env.BENCH_QUEUE;
const TASK = process.env.BENCH_TASK;
const BACKEND = process.env.BENCH_FLEXIQ_BACKEND ?? "sqlite";

function openQueue() {
  const queue =
    BACKEND === "redis"
      ? new Queue({ backend: "redis", dsn: process.env.BENCH_REDIS_URL })
      : new Queue({ dbPath: process.env.BENCH_DB });
  queue.task(TASK, (payload) => handle(payload));
  return queue;
}

const { command, first, count } = parseArgs(process.argv.slice(2));

if (command === "worker") {
  const queue = openQueue();
  const worker = queue.runWorker({
    queues: [QUEUE],
    concurrency: Number(process.env.BENCH_CONCURRENCY),
  });
  runUntilSignalled(() => worker.stop());
} else if (command === "produce") {
  const queue = openQueue();
  await produce(first, count, async (payload) =>
    queue.enqueue(TASK, [payload], { queue: QUEUE, maxRetries: 0 }),
  );
  // The handle owns no socket the process needs closed: the native side is
  // torn down with the process, and a producer that lingers would be counted
  // against the next phase.
  process.exit(0);
} else {
  process.stderr.write(`usage: flexiq.mjs worker | produce --first N --count M\n`);
  process.exit(2);
}
