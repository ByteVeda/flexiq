# Polyglot example — one jobs table, three languages

A Python producer enqueues orders. A Node worker processes them and enqueues a
follow-up. A Java worker sends the notification. All three run against **one
database and one jobs table**, and none of them imports the others.

```text
Python producer ──▶ queue "process" ──▶ Node worker ──▶ queue "notify" ──▶ Java worker
```

The only contract between them is the task name and the wire format. That is the
point of the example: a task queue whose producer and consumer do not have to
agree on a language.

## Run it

Three terminals, from this directory. The workers can start before or after the
producer — jobs wait in storage either way.

```bash
# 1. Python producer
pip install flexiq==1.0.0
python producer.py --db flexiq.db --orders 3

# 2. Node worker — processes orders, enqueues notifications
cd node-worker && npm install && FLEXIQ_DB=../flexiq.db npm start

# 3. Java worker — consumes notifications
cd java-worker && FLEXIQ_DB=../flexiq.db ./gradlew run
```

Expected output:

```text
enqueued orders.process ord-0001 job=019fd561-...
[node] processing ord-0001 — 10.00 EUR
[java] notifying ada@example.com about ord-0001 — 10.00 EUR (processed by node)
```

## The two things that actually matter

**1. Set the serializer explicitly.** Every SDK defaults to a serializer that is
same-language-only (`SmartSerializer` in Python, `JsonSerializer` in Node and
Java). Cross-runtime payloads use CBOR — the `0x02` wire envelope from
`BINDING_CONTRACT.md` — and every process here opts in:

```python
Queue(db, serializer=CborSerializer())                 # Python
```
```js
new Queue({ dbPath, serializer: new CborSerializer() }) // Node
```
```java
FlexiQ.builder().sqlite(db).serializer(new CborSerializer()).open()  // Java
```

Leaving it unset is the single most likely reason a cross-language setup fails.
CBOR also round-trips 64-bit integers, byte strings and decimals losslessly,
which JSON does not.

**2. Give each stage its own queue.** A worker claims whatever is in the queues
it polls, regardless of whether it has a handler for that task. Put two stages on
one queue and a worker will dequeue the other's jobs and dead-letter them. Each
process here polls exactly one:

```js
queue.runWorker({ queues: ["process"] });   // Node
```
```java
flexiq.worker().queues("notify").start();  // Java
```

## Payload shape

The wire body is `[args, kwargs]`. Java's `enqueue(name, payload)` maps to a
single positional argument, so a task that is called from every runtime should
take **one object**:

```python
queue.enqueue("orders.process", args=(order,), queue="process")
```

Handlers receive that object directly — `(order) => …` in Node,
`Map.class` in Java.

## Storage

SQLite here because it needs no setup. The same example runs unchanged on
PostgreSQL or Redis — point every process at the same backend instead of the
same file. SQLite is fine for a single host; for workers on separate machines,
use PostgreSQL.

## Server mode: the gRPC variant

Everything above shares one thing: a file. Every process opens `flexiq.db`
directly, which means every process needs the Rust core compiled into it — the
polyglot story is gated on someone having written a native binding for that
language.

This variant swaps the **producer** for one that doesn't need a binding at
all — a shell script, talking to a running `flexiq-server` over its gRPC
producer door instead of opening the file. The workers are unchanged: they
still open `flexiq.db` directly, same as above. (Turning them into attached
executors too — so nothing but `flexiq-server` touches the file — is filed as
follow-up work: [#796](https://github.com/ByteVeda/flexiq/issues/796) for the
Node worker, [#797](https://github.com/ByteVeda/flexiq/issues/797) for Java.)

Reach for the original example when every stage is a process that can hold a
database credential. Reach for this one when the producer is a script, a
webhook handler, or anything else with no native binding to reach for.

```bash
# 1. Run flexiq-server against the same file, with the gRPC door open.
#    FLEXIQ_NAMESPACE is mandatory for that door — see the "server mode" docs.
docker run --rm -p 50051:50051 -v "$PWD:/data" \
  -e FLEXIQ_DSN=/data/flexiq.db \
  -e FLEXIQ_NAMESPACE=polyglot \
  -e FLEXIQ_GRPC_LISTEN=0.0.0.0:50051 \
  ghcr.io/byteveda/flexiq-server:1.0.0

# 2. Mint a producer token (any shell that can reach the same file):
docker run --rm -v "$PWD:/data" \
  -e FLEXIQ_DSN=/data/flexiq.db -e FLEXIQ_NAMESPACE=polyglot \
  ghcr.io/byteveda/flexiq-server:1.0.0 \
  token create --name polyglot-producer --scope produce
export FLEXIQ_TOKEN=fqt_...   # printed by the command above

# 3. Enqueue over gRPC instead of running producer.py:
FLEXIQ_TOKEN=$FLEXIQ_TOKEN ./grpc_producer.sh 3

# 4. Workers need the same namespace — a job enqueued through the gRPC door
#    always carries its token's namespace, and a worker polling the default
#    (unnamespaced) rows would never see it.
(cd node-worker && FLEXIQ_DB=../flexiq.db FLEXIQ_NAMESPACE=polyglot npm start)
(cd java-worker && FLEXIQ_DB=../flexiq.db FLEXIQ_NAMESPACE=polyglot ./gradlew run)
```

`grpc_producer.sh` needs [`grpcurl`](https://github.com/fullstorydev/grpcurl)
and `jq` — nothing else, which is the whole point.

## Running against a local build

The commands above use published packages. To run against this repository
instead:

Each command runs from this directory, so the subshells leave you where you
started. The symlink target is relative to the directory holding the link —
`node_modules/@byteveda` — which is five levels below the repository root.

```bash
# Python
(cd ../../sdks/python && uv run maturin develop)

# Node — link the workspace SDK into the example
(cd node-worker && mkdir -p node_modules/@byteveda \
  && ln -s ../../../../../sdks/node node_modules/@byteveda/flexiq)

# Java — publish locally, then Gradle resolves it from mavenLocal()
(cd ../../sdks/java && ./gradlew publishToMavenLocal)
```

A locally built Java SDK stages a native library only for the host platform. If
the runtime cannot find it, point at it directly:

```bash
./gradlew run -Dflexiq.native.lib=/path/to/libflexiq_java.so
```

Once all three are wired up, `python scripts/polyglot_e2e.py` (from the
repository root) drives this same pipeline unattended and asserts it drained.
CI runs it on every supported Python, Node and JDK version — it is what keeps a
cross-language wire change from merging green.
