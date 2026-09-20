// The BullMQ producer, and the version probe.
//
// Usage mirrors the Python runners exactly — `run.py` cannot tell them apart:
//
//   node driver.mjs enqueue --jobs N --payload-bytes B --seq-offset S --mode M [--rate R]
//   node driver.mjs versions
//
// The one asymmetry worth stating: this producer is Node, where the other four
// are Python. BullMQ's enqueue figure therefore carries no cross-language
// penalty, and neither should it — a BullMQ user writes JavaScript.

import { readFileSync } from "node:fs";
import { Queue } from "bullmq";
import { nowNs } from "./sink.mjs";

const argv = process.argv.slice(2);
const command = argv[0];
const flag = (name, fallback) => {
  const at = argv.indexOf(`--${name}`);
  return at === -1 ? fallback : argv[at + 1];
};

/** The payload, character for character what `harness/scenario.py` builds. */
function payload(size) {
  let tile = "";
  for (let i = 0; i < 94; i += 1) tile += String.fromCharCode(33 + (i % 94));
  return tile.repeat(Math.floor(size / tile.length) + 1).slice(0, size);
}

function versionOf(name) {
  try {
    const url = new URL(`./node_modules/${name}/package.json`, import.meta.url);
    return JSON.parse(readFileSync(url, "utf8")).version;
  } catch {
    return null;
  }
}

if (command === "versions") {
  console.log(
    JSON.stringify({
      versions: { bullmq: versionOf("bullmq"), ioredis: versionOf("ioredis"), node: process.version },
      concurrency_model: "1 Node process, `concurrency` jobs in flight on one event loop",
      supports_batch: true,
    }),
  );
  process.exit(0);
}

if (command !== "enqueue") {
  console.error(`unknown command: ${command}`);
  process.exit(2);
}

const jobs = Number(flag("jobs"));
const body = payload(Number(flag("payload-bytes")));
const seqOffset = Number(flag("seq-offset", "0"));
const mode = flag("mode", "per-job");
const rate = Number(flag("rate", "0"));

/** The Python pacer's twin — see `pace()` in runners/_common.py. Held against a
 *  schedule computed from the start rather than a fixed sleep per iteration,
 *  so the submit cost does not accumulate into the interval. */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function hold(started, i) {
  if (rate <= 0) return;
  const slack = started + (i / rate) * 1000 - performance.now();
  if (slack > 0) await sleep(slack);
}

const queue = new Queue("bench", {
  connection: { url: process.env.BENCH_BULLMQ_URL },
  defaultJobOptions: { removeOnComplete: true, removeOnFail: true, attempts: 1 },
});

const started = performance.now();
if (mode === "batch") {
  await queue.addBulk(
    Array.from({ length: jobs }, (_, i) => ({
      name: "drain",
      data: { seq: seqOffset + i, enqueuedNs: Number(nowNs()), body },
    })),
  );
} else {
  for (let i = 0; i < jobs; i += 1) {
    await hold(started, i);
    await queue.add("drain", { seq: seqOffset + i, enqueuedNs: Number(nowNs()), body });
  }
}
const seconds = (performance.now() - started) / 1000;

await queue.close();
console.log(
  JSON.stringify({
    jobs,
    mode,
    rate,
    seconds: Number(seconds.toFixed(6)),
    per_second: Number((jobs / seconds).toFixed(1)),
  }),
);
process.exit(0);
