// Runs `orders.process` jobs that a producer in another language enqueued, then
// hands each order on to `orders.notify` for a worker in a third language.
//
// This process has no idea a Python producer exists. The task name and the CBOR
// wire format are the entire contract between them.
//
// Two deployments share this one module. `node worker.mjs` polls the database
// itself. `flexiq executor worker.mjs` imports the module for its task registry
// alone and lets a scheduler hold the database — so everything that opens or
// polls storage sits behind the `runDirectly` guard at the bottom, and the
// registration above it runs either way.

import { pathToFileURL } from "node:url";

import { CborSerializer, Queue } from "@byteveda/flexiq";

const dbPath = process.env.FLEXIQ_DB ?? "../flexiq.db";
// Set by the gRPC variant only: a job enqueued through the producer door
// always carries its token's namespace, so a worker with no namespace would
// never see it. Unset here, same as every process in the original example.
const namespace = process.env.FLEXIQ_NAMESPACE;

// Set by the attached-executor variant only: flexiq-server's producer door,
// and a `produce`-scoped token for it. See `enqueueNotify` for why an executor
// needs them.
const producerUrl = process.env.FLEXIQ_PRODUCER_URL;
const producerToken = process.env.FLEXIQ_TOKEN;
if (producerUrl && !producerToken) {
  throw new Error("FLEXIQ_PRODUCER_URL needs FLEXIQ_TOKEN — a produce-scoped token for that door");
}

// Node's `fetch` has no default timeout, and this call is awaited inside a task
// body — so a producer door that accepts the connection and never answers would
// hold an executor slot until the job's own timeout expired. Bound it well
// inside that.
const PRODUCER_TIMEOUT_MS = 10_000;

// Each SDK's own default serializer is same-language-only. CBOR is the
// cross-SDK format, and every runtime here opts into it explicitly.
//
// Under `flexiq executor` the connection options are ignored: the queue built
// here stands in for the scheduler's storage rather than opening any of its
// own, which is the whole point of running detached.
const queue = new Queue({ dbPath, serializer: new CborSerializer(), namespace });

/**
 * Hand one processed order to the `orders.notify` stage.
 *
 * An in-process worker writes the job straight into the file it polls. An
 * attached executor has no database at all, and `queue.enqueue` raises there by
 * design — an enqueue that quietly vanished would be worse than one that
 * failed — so the job goes back out through flexiq-server's producer door.
 * Executing and producing are two doors with two credentials, which is what
 * makes an executor safe to run without database access.
 *
 * `structured` has the server build the CBOR envelope from plain JSON, exactly
 * as `grpc_producer.sh` does, so this path needs no codec and no dependency.
 */
async function enqueueNotify(order) {
  if (!producerUrl) {
    queue.enqueue("orders.notify", [order], { queue: "notify" });
    return;
  }

  const response = await fetch(`${producerUrl}/v1/jobs`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${producerToken}`,
    },
    body: JSON.stringify({
      taskName: "orders.notify",
      structured: { args: [order] },
      options: { queue: "notify" },
    }),
    signal: AbortSignal.timeout(PRODUCER_TIMEOUT_MS),
  });

  // Thrown rather than logged: a hand-off that failed has to fail the job with
  // it, or the order is processed and never notified.
  if (!response.ok) {
    throw new Error(
      `producer door refused orders.notify: ${response.status} ${await response.text()}`,
    );
  }
}

queue.task("orders.process", async (order) => {
  const total = (order.amount_cents / 100).toFixed(2);
  console.log(`[node] processing ${order.order_id} — ${total} ${order.currency}`);

  // Enqueued from Node, consumed by the Java worker. Same wire format, so the
  // object survives the hop with its types intact.
  await enqueueNotify({
    order_id: order.order_id,
    customer: order.customer,
    total,
    currency: order.currency,
    processed_by: "node",
  });

  return { order_id: order.order_id, status: "processed" };
});

// What `flexiq executor worker.mjs` imports this module for.
export default queue;

// Running the file *is* the request for an in-process worker; importing it is
// not. `import.meta.filename` and `import.meta.main` are newer than the SDK's
// minimum Node, so the comparison is spelled out.
const runDirectly = process.argv[1]
  ? import.meta.url === pathToFileURL(process.argv[1]).href
  : false;

if (runDirectly) {
  // Poll only this stage's queue. A worker claims whatever is in the queues it
  // polls, so sharing one queue would let this process pick up `orders.notify`
  // jobs it has no handler for and dead-letter them.
  queue.runWorker({ queues: ["process"] });
  console.log(`[node] worker running against ${dbPath}; ctrl-c to stop`);

  for (const signal of ["SIGINT", "SIGTERM"]) {
    process.on(signal, async () => {
      console.log("\n[node] stopping");
      await queue.shutdown();
      process.exit(0);
    });
  }
}
