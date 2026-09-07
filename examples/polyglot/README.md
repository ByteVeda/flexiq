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
pip install flexiq==2.0.0
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
producer door instead of opening the file. Either worker can move off the file
too, as an attached executor — see
[the Node section](#the-node-worker-as-an-attached-executor) and
[the Java section](#the-java-worker-as-an-attached-executor) below.

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
  ghcr.io/byteveda/flexiq-server:2.0.0

# 2. Mint a producer token (any shell that can reach the same file):
docker run --rm -v "$PWD:/data" \
  -e FLEXIQ_DSN=/data/flexiq.db -e FLEXIQ_NAMESPACE=polyglot \
  ghcr.io/byteveda/flexiq-server:2.0.0 \
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

### The Node worker as an attached executor

The producer above stopped opening `flexiq.db`. The Node worker can stop too:
`flexiq executor` dials the scheduler, announces the tasks it can run, and runs
whatever it is sent. That process holds no database credential and opens no
port of its own.

`worker.mjs` serves both deployments from one module — running the file starts
a worker, importing it only registers tasks — so the handler is the same code
either way.

```bash
# 1. flexiq-server again — step 1 above, with the attach door open beside the
#    gRPC one.
#    FLEXIQ_QUEUES is what the scheduler claims. Leave `notify` out: the Java
#    worker is still polling storage for those jobs itself, and a job claimed
#    here that no attached executor advertises would just wait for a placement
#    that never comes.
#    A non-loopback FLEXIQ_LISTEN refuses to start without FLEXIQ_ATTACH_TOKEN;
#    the attach port dispatches code.
export FLEXIQ_ATTACH_TOKEN=$(openssl rand -hex 32)
docker run --rm -p 50051:50051 -p 7777:7777 -v "$PWD:/data" \
  -e FLEXIQ_DSN=/data/flexiq.db \
  -e FLEXIQ_NAMESPACE=polyglot \
  -e FLEXIQ_QUEUES=process \
  -e FLEXIQ_GRPC_LISTEN=0.0.0.0:50051 \
  -e FLEXIQ_LISTEN=0.0.0.0:7777 \
  -e FLEXIQ_ATTACH_TOKEN \
  ghcr.io/byteveda/flexiq-server:2.0.0

# 2. A second produce-scoped token, for the worker's own hand-off. Its own
#    rather than the script's: a token is named, scoped and revocable on its
#    own, which only buys anything if each client holds one.
docker run --rm -v "$PWD:/data" \
  -e FLEXIQ_DSN=/data/flexiq.db -e FLEXIQ_NAMESPACE=polyglot \
  ghcr.io/byteveda/flexiq-server:2.0.0 \
  token create --name polyglot-node-chain --scope produce
export FLEXIQ_NODE_TOKEN=fqt_...   # printed by the command above

# 3. The worker, attached instead of polling. The Java worker and
#    ./grpc_producer.sh are unchanged from the block above.
(cd node-worker \
  && FLEXIQ_ATTACH=localhost:7777 FLEXIQ_ATTACH_TOKEN=$FLEXIQ_ATTACH_TOKEN \
     FLEXIQ_PRODUCER_URL=http://localhost:50051 FLEXIQ_TOKEN=$FLEXIQ_NODE_TOKEN \
     npm run executor -- --slots 2)
```

`FLEXIQ_DB` and `FLEXIQ_NAMESPACE` are absent on purpose. An executor opens no
storage, so both are the scheduler's to decide, and the queue the module builds
is a stand-in that never connects to anything.

**An executor runs work; it cannot enqueue.** It has no database, and the SDK
raises rather than letting a job vanish. `orders.process` fans out to
`orders.notify`, so `FLEXIQ_PRODUCER_URL` sends that hand-off back through the
same producer door `grpc_producer.sh` uses — `POST /v1/jobs` over plain HTTP on
the gRPC listener, `structured` args, no CBOR encoder anywhere in the worker.
Executing and producing stay two doors with two credentials, which is exactly
what lets the executor run with no database access at all.

### The Java worker as an attached executor

`orders.notify` is the last stage in the pipeline — `NotifyWorker` enqueues
nothing downstream, so there's no producer-door hand-off to wire up here,
unlike the Node worker above. `flexiq executor` discovers `notifyCustomer`
through `META-INF/services`, generated at compile time by the `@TaskHandler`
annotation on it — no `main` runs to register it.

```bash
# 1. flexiq-server again — step 1 of the gRPC variant above, with the attach
#    door open beside the gRPC one.
#    FLEXIQ_QUEUES is what the scheduler claims. Leave `process` out: the
#    Node worker is still polling storage for those jobs itself, and a job
#    claimed here that no attached executor advertises would just wait for a
#    placement that never comes.
export FLEXIQ_ATTACH_TOKEN=$(openssl rand -hex 32)
docker run --rm -p 50051:50051 -p 7777:7777 -v "$PWD:/data" \
  -e FLEXIQ_DSN=/data/flexiq.db \
  -e FLEXIQ_NAMESPACE=polyglot \
  -e FLEXIQ_QUEUES=notify \
  -e FLEXIQ_GRPC_LISTEN=0.0.0.0:50051 \
  -e FLEXIQ_LISTEN=0.0.0.0:7777 \
  -e FLEXIQ_ATTACH_TOKEN \
  ghcr.io/byteveda/flexiq-server:2.0.0

# 2. The worker, attached instead of polling. The Node worker and
#    ./grpc_producer.sh are unchanged from the gRPC variant above.
#    FLEXIQ_SERIALIZER=cbor matches the CBOR wire format orders.notify was
#    enqueued with — the executor has no application main to set this on a
#    Queue the way the Node and Python executors do, so the CLI needs it
#    directly.
(cd java-worker \
  && FLEXIQ_ATTACH=localhost:7777 FLEXIQ_ATTACH_TOKEN=$FLEXIQ_ATTACH_TOKEN \
     FLEXIQ_SERIALIZER=cbor \
     ./gradlew runExecutor)
```

`FLEXIQ_DB` and `FLEXIQ_NAMESPACE` are absent on purpose, same reason as the
Node executor: this process opens no storage of its own.

## Running against a local build

The commands above use published packages. To run against this repository
instead:

Each command runs from this directory, so the subshells leave you where you
started. A symlink target is relative to the directory holding the link, not to
this one: `node_modules/@byteveda`, five levels below the repository root, and
`node_modules/.bin` for the CLI.

```bash
# Python
(cd ../../sdks/python && uv run maturin develop)

# Node — build the workspace SDK, then link it into the example. The second
# link is the CLI `npm run executor` resolves from node_modules/.bin.
(cd ../../sdks/node && pnpm build)
(cd node-worker && mkdir -p node_modules/@byteveda node_modules/.bin \
  && ln -s ../../../../../sdks/node node_modules/@byteveda/flexiq \
  && ln -s ../@byteveda/flexiq/dist/cli.js node_modules/.bin/flexiq)

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
