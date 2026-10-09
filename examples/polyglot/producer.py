"""Enqueue work from Python that workers in other languages will run.

Nothing here knows a handler for `orders.process` — the task name and the wire
format are the whole contract, so the consumer can live in any runtime.

Each stage gets its own named queue so a worker only ever dequeues jobs it can
actually handle: workers poll queues, not task names, so a single shared queue
would let one runtime claim another's jobs and dead-letter them.

Each order is enqueued under its own `checkout` span, and the OpenTelemetry
middleware in every runtime carries that trace through Node and on to Java: one
order, one trace, three languages.
"""

from __future__ import annotations

import argparse
import sys

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from flexiq import Queue
from flexiq.contrib.otel import OpenTelemetryMiddleware
from flexiq.serializers import CborSerializer


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default="flexiq.db", help="shared queue database")
    parser.add_argument("--orders", type=int, default=3, help="how many orders to enqueue")
    args = parser.parse_args()

    # CBOR is the cross-SDK wire format. Every runtime in this example sets it
    # explicitly: each SDK's own default is same-language-only, so leaving it
    # unset is what actually breaks interop.
    queue = Queue(
        args.db, serializer=CborSerializer(), middleware=[OpenTelemetryMiddleware()]
    )
    # No exporter: the spans exist to be propagated. Point a real exporter at a
    # collector to see the whole trace.
    trace.set_tracer_provider(TracerProvider())
    tracer = trace.get_tracer("flexiq.polyglot.producer")

    for n in range(1, args.orders + 1):
        order = {
            "order_id": f"ord-{n:04d}",
            "customer": "ada@example.com",
            "amount_cents": 1000 * n,
            "currency": "EUR",
        }
        with tracer.start_as_current_span("checkout") as span:
            job = queue.enqueue("orders.process", args=(order,), queue="process")
        trace_id = format(span.get_span_context().trace_id, "032x")
        print(f"enqueued orders.process {order['order_id']} job={job.id} trace={trace_id}")

    print(f"\n{args.orders} order(s) queued in {args.db}. Start the workers to drain them.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
