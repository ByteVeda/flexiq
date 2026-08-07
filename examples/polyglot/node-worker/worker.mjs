// Runs `orders.process` jobs that a producer in another language enqueued, then
// hands each order on to `orders.notify` for a worker in a third language.
//
// This process has no idea a Python producer exists. The task name and the CBOR
// wire format are the entire contract between them.

import { CborSerializer, Queue } from "@byteveda/taskito";

const dbPath = process.env.TASKITO_DB ?? "../taskito.db";

// Each SDK's own default serializer is same-language-only. CBOR is the
// cross-SDK format, and every runtime here opts into it explicitly.
const queue = new Queue({ dbPath, serializer: new CborSerializer() });

queue.task("orders.process", (order) => {
  const total = (order.amount_cents / 100).toFixed(2);
  console.log(`[node] processing ${order.order_id} — ${total} ${order.currency}`);

  // Enqueued from Node, consumed by the Java worker. Same wire format, so the
  // object survives the hop with its types intact.
  queue.enqueue(
    "orders.notify",
    [
      {
        order_id: order.order_id,
        customer: order.customer,
        total,
        currency: order.currency,
        processed_by: "node",
      },
    ],
    { queue: "notify" },
  );

  return { order_id: order.order_id, status: "processed" };
});

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
