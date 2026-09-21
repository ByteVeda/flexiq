/**
 * The Node half of the scenario: the same payload, the same sink, the same
 * two subcommands.
 *
 * `worker` runs until SIGTERM; `produce` submits a slice of the run and prints
 * one JSON line the Python harness reads. Both are here so BullMQ and FlexiQ
 * cannot drift apart in how they measure — only in how they queue.
 */

import { openSync, writeSync } from "node:fs";

import IORedis from "ioredis";

let sinkFd = null;

/** The append-only latency sink, opened once per process. */
function sink() {
  if (sinkFd === null) {
    sinkFd = openSync(process.env.BENCH_SINK, "a");
  }
  return sinkFd;
}

/**
 * Wall clock in milliseconds, sub-millisecond resolution.
 *
 * `Date.now()` is integer milliseconds, which is the same order as the
 * measurement on a local backend. `timeOrigin + performance.now()` is the same
 * epoch with a microsecond tick, so a Node latency is comparable with a Python
 * one rather than quantised against it.
 */
export function nowMs() {
  return performance.timeOrigin + performance.now();
}

export function build(index, pad) {
  return { i: index, t: nowMs(), pad };
}

/** The measured task body — identical to `harness.workers.payload.handle`. */
export function handle(payload) {
  const completed = nowMs();
  writeSync(sink(), `${payload.i} ${(completed - payload.t).toFixed(3)} ${completed.toFixed(3)}\n`);
}

/** `--first N --count M`, and the subcommand. */
export function parseArgs(argv) {
  const [command] = argv;
  const flags = {};
  for (let i = 1; i < argv.length; i += 2) {
    flags[argv[i].replace(/^--/, "")] = argv[i + 1];
  }
  return { command, first: Number(flags.first ?? 0), count: Number(flags.count ?? 0) };
}

/**
 * The result line, written synchronously.
 *
 * `process.stdout.write` to a pipe is asynchronous, and a producer that exits
 * straight after submitting would lose the line it exists to print.
 */
export function emit(result) {
  writeSync(1, `${JSON.stringify(result)}\n`);
}

/**
 * A fresh ioredis client for one BullMQ object.
 *
 * BullMQ 6 treats ioredis as optional and, under native ESM, refuses to
 * construct one from an option bag — it wants a client instance. One per
 * Queue or Worker, never shared: BullMQ's blocking commands occupy a
 * connection for as long as they wait.
 */
export function redisClient() {
  return new IORedis(process.env.BENCH_REDIS_URL, {
    // BullMQ's own documented requirement for a blocking consumer, not a
    // tuning knob.
    maxRetriesPerRequest: null,
  });
}

/** Time a serial submission loop the way the Python producer does. */
export async function produce(first, count, submit) {
  const pad = process.env.BENCH_PAD ?? "";
  const startedMs = nowMs();
  const clock = performance.now();
  const jobIds = [];
  for (let i = first; i < first + count; i += 1) {
    jobIds.push(await submit(build(i, pad)));
  }
  emit({ seconds: (performance.now() - clock) / 1000, started_ms: startedMs, job_ids: jobIds });
}

/** Keep a worker process alive until the harness signals it, then shut down. */
export function runUntilSignalled(stop) {
  const keepAlive = setInterval(() => {}, 1 << 30);
  const shutdown = async () => {
    clearInterval(keepAlive);
    await stop();
    process.exit(0);
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}
