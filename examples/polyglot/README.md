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
pip install taskito==0.23.0
python producer.py --db taskito.db --orders 3

# 2. Node worker — processes orders, enqueues notifications
cd node-worker && npm install && TASKITO_DB=../taskito.db npm start

# 3. Java worker — consumes notifications
cd java-worker && TASKITO_DB=../taskito.db ./gradlew run
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
Taskito.builder().sqlite(db).serializer(new CborSerializer()).open()  // Java
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
taskito.worker().queues("notify").start();  // Java
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
  && ln -s ../../../../../sdks/node node_modules/@byteveda/taskito)

# Java — publish locally, then Gradle resolves it from mavenLocal()
(cd ../../sdks/java && ./gradlew publishToMavenLocal)
```

A locally built Java SDK stages a native library only for the host platform. If
the runtime cannot find it, point at it directly:

```bash
./gradlew run -Dtaskito.native.lib=/path/to/libtaskito_java.so
```

Once all three are wired up, `python scripts/polyglot_e2e.py` (from the
repository root) drives this same pipeline unattended and asserts it drained.
CI runs it on every supported Python, Node and JDK version — it is what keeps a
cross-language wire change from merging green.
