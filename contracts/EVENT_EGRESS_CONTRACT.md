# FlexiQ — event egress contract

What a consumer of FlexiQ's job lifecycle events may rely on, and what it must
do in return. It is the contract for GitHub issue #848: the scheduler and the
server's producer doors describe each job transition as a CloudEvent and send
it out of the process — to an HTTP endpoint or a Redis stream — so a data
warehouse, an audit log or an alerting pipeline can follow jobs without
polling FlexiQ's storage. Read this if you are writing that consumer, in any
language, with no FlexiQ SDK.

Everything here is normative. **MUST**, **MUST NOT**, **SHOULD** and **MAY**
carry their RFC 2119 meanings; a claim without one of them is background.
Every field name, header, status code and literal below is taken from
`crates/flexiq-core/src/events/` (`event.rs`, `config.rs`, `hub.rs`,
`sink/http.rs`, `sink/redis_streams.rs`), `crates/flexiq-core/src/scheduler/events.rs`
and `crates/flexiq-server/src/events.rs` — the source of truth this document
restates. Where the two ever disagree, the code wins.

## Where this sits

`crates/flexiq-core/BINDING_CONTRACT.md` is what a language shell implements
against `flexiq-core` in the same process. `contracts/REMOTE_SDK_CONTRACT.md`
is what a client dials **in** to `flexiq-server` through.
[`PUSH_DISPATCH_CONTRACT.md`](PUSH_DISPATCH_CONTRACT.md) is the server dialling
**out** to run a job. Event egress also dials out, but only to report: a
consumer never influences a job, and nothing it answers reaches the queue.

It is also not the dashboard's webhook subscriptions. Those share the event
names below but are a separate feature with their own `x-flexiq-signature`
scheme; the two never sign the same bytes under the same header.

Every runtime parses the same configuration document in `flexiq-core`:

- **`flexiq-server`** reads it from the file `FLEXIQ_EVENTS_FILE` names, and
  bounds its shutdown drain with `FLEXIQ_EVENTS_DRAIN`.
- **An SDK shell** takes the same JSON document, and a drain budget, as
  worker options. The hub starts with the worker and drains when it stops.

## The configuration document

One JSON object. **Unknown fields are refused at every level** — a misspelt
`allow` that silently read as empty would fail open. A document that fails any
rule below stops the runtime at start, before any event exists.

```json
{
  "source": "/flexiq",
  "sinks": [
    {"kind": "http", "name": "warehouse", "url": "https://events.example.com/in",
     "allow": ["events.example.com"], "hmac_secret_env": "EVT_HMAC",
     "filter": {"types": ["job.dead", "job.completed"]},
     "delivery": {"max_batch": 100}},
    {"kind": "redis_streams", "name": "stream", "url_env": "EVT_REDIS_URL",
     "stream": "flexiq:events"}
  ]
}
```

### Top level

| Field | Type | Default | Rule |
|---|---|---|---|
| `source` | string | `"/flexiq"` | CloudEvents `source` on every event. Not empty or whitespace. |
| `sinks` | array | required | At least one. Each has a `kind`: `http` or `redis_streams`. |

### Every sink

| Field | Type | Default | Rule |
|---|---|---|---|
| `kind` | string | required | `http` or `redis_streams`. Any other value is refused. |
| `name` | string | required | Not empty; unique in the document. It is the `sink` metrics label and the name in logs. |
| `filter` | object | admit all | See [Filters](#filters). |
| `include_payload` | bool | `false` | See [Payloads](#payloads). |
| `delivery.buffer` | integer | `10000` | Events held before new ones are dropped. At least 1. |
| `delivery.max_attempts` | integer | `5` | Attempts per batch, the first included. At least 1. |
| `delivery.max_batch` | integer | `1` | Events per request (HTTP) or pipeline (Redis). 1 to 1000. |

### `http`

| Field | Type | Default | Rule |
|---|---|---|---|
| `url` | string | required | `https`, or `http` only to a loopback host with `allow_loopback`. No userinfo. Its host must be allowed by `allow`. |
| `allow` | array of strings | required | Hosts and networks the URL may resolve to. Not empty. See [Egress guard](#egress-guard). |
| `allow_loopback` | bool | `false` | Permit a loopback destination and cleartext `http` to it. For tests and sidecars. |
| `bearer_token_env` | string | none | Name of an environment variable holding a bearer token. |
| `hmac_secret_env` | string | none | Name of an environment variable holding the signing secret. See [Signing](#signing). |
| `timeout_ms` | integer | `10000` | Whole-request budget. At least 1. |
| `connect_timeout_ms` | integer | `5000` | Connect budget. At least 1. |

### `redis_streams`

| Field | Type | Default | Rule |
|---|---|---|---|
| `url_env` | string | required | Name of an environment variable holding the `redis://` URL. Not empty. |
| `stream` | string | required | Stream key events are appended to. Not empty. |
| `max_len` | integer | `100000` | Approximate cap on the stream's length (`MAXLEN ~`). At least 1. |

### Rules checked when the sinks are built

Beyond the shape, each sink is built at start, and any of these refuses it:

- A `kind` this build was compiled without: `http` needs `flexiq-core`'s
  `events-http` feature, `redis_streams` its `redis` feature. The error names
  the feature (`EventsConfigError::NotCompiled`).
- An `allow` entry that does not parse, a `url` that fails the rules above.
- A `*_env` field naming a variable that is unset or empty, or a bearer token
  no header can carry.
- A Redis URL `redis` cannot use. The error names the category, never the URL.

These are the messages a runtime reports, as captured from the parser and the
HTTP sink:

```
events sink 'a': allow must name at least one host or network
events sink 'a': delivery.max_batch must be between 1 and 1000
events sink 'a': unknown event type 'job.boom'
events config is not valid: unknown field `alow`, expected one of `name`, `url`, `allow`, `allow_loopback`, `bearer_token_env`, `hmac_secret_env`, `timeout_ms`, `connect_timeout_ms`, `filter`, `include_payload`, `delivery` at line 1 column 93
events sink 'web': url host 'evil.example.com' is not on the allowlist
events sink 'a': url host 'e.example.com' must be reached over https; cleartext http is allowed only to a loopback host with allow_loopback
```

### Secrets

Every credential is named by an environment variable, never written in the
document: `bearer_token_env`, `hmac_secret_env`, and `url_env` (a Redis URL
carries its password). The document can then live in a ConfigMap.

`flexiq-server` reads each variable once, while building the sinks, then
**removes every one of them from its environment** before any role starts, so
none survives into `/proc/<pid>/environ` or a crash dump. An embedded SDK
runtime reads them each time a worker starts its hub and leaves them in place.

### Filters

```json
"filter": {"namespaces": [], "queues": [], "tasks": [], "types": ["job.dead"]}
```

Four allowlists. An empty or absent list admits everything; an event must pass
**all four**. `namespaces` names the default namespace as `default`. `types`
takes the short names in [Event types](#event-types) and refuses an unknown
one at start; the other three are not checked against anything, so a typo
there silently admits nothing.

## Event types

The CloudEvents `type` is `org.byteveda.flexiq.` followed by the short name.

| Short name | Emitted by | When |
|---|---|---|
| `job.enqueued` | server doors | A door wrote a new job. |
| `job.started` | scheduler | A job was claimed and handed off for execution. |
| `job.completed` | scheduler | An attempt succeeded. |
| `job.failed` | scheduler | An attempt failed. Always followed by `job.retrying` or `job.dead`. |
| `job.retrying` | scheduler | The failed attempt was rescheduled. |
| `job.dead` | scheduler | The job was dead-lettered: out of retries, not retryable, or shed. |
| `job.cancelled` | scheduler, server doors | The job was cancelled, before or during its run. |
| `job.sleeping` | scheduler | An attempt ended in a durable step sleep; the job is pending again. |

### When each fires

- **`job.enqueued`** — only from `flexiq-server`'s producer doors, and only
  for a row the call actually wrote: gRPC `Enqueue` and `EnqueueBatch` (and
  the JSON facade, which calls them), triggers, the admin trigger-now of a
  periodic task, and dead-letter replays from the admin service and the
  dashboard (a replay is a new job, so it is announced as an enqueue). The
  dashboard's job replay also emits it. A unique or idempotent enqueue that
  answered with an existing job emits nothing, and neither does a debounced
  enqueue that slid an existing job's window. An atomic batch announces its
  jobs only after its transaction commits.
- **`job.started`** — when the scheduler has claimed a job and handed it to
  its worker pool (an in-process pool, an attached executor's dispatcher, or
  a push target's), after the hand-off succeeded. It means "claimed and on its
  way", not "the task body began".
- **`job.completed`**, **`job.failed`**, **`job.retrying`**, **`job.dead`**,
  **`job.cancelled`**, **`job.sleeping`** — when the scheduler settles an
  attempt's result. A failure emits two events: `job.failed`, then
  `job.retrying` or `job.dead`, both carrying the error; `job.retrying` names
  **the attempt that failed**, not the next one, which has its own
  `job.started`. Execution timeouts the scheduler's reaper settles, and jobs
  recovered from a dead worker, go through the same path: `job.failed` (with
  `timed_out: true` for a timeout, `false` for a recovery) and the matching
  second event.
- **`job.dead` for a shed** — when the scheduler dead-letters a job without
  running it: a rate limit with `on_excess` set to drop, or a CoDel overload
  shed. It carries `reason` (the dead-letter reason, beginning `rate_limit:`
  or `codel:`) and **no `job.failed`**: a shed is not a failed attempt.
- **`job.cancelled`** — for a running job, when the scheduler settles the
  attempt the task abandoned. For a pending job, only when a
  `flexiq-server` cancel door (gRPC `CancelJob` or its JSON facade route, the
  dashboard's cancel) cancelled it outright.

### What is not seen

A consumer MUST NOT treat the stream as a complete audit of the queue. These
transitions emit nothing:

- **Enqueues outside `flexiq-server`'s doors**: a job an embedded SDK
  enqueues in its own process, a periodic task the scheduler fires, a
  dead-letter entry the scheduler replays automatically, and the node jobs of
  a submitted workflow. Each still emits from `job.started` on, if its
  scheduler has sinks.
- **Pending-job cancels outside the server's cancel doors**, including one
  made through an embedded SDK, and dependents cancelled by a cascade.
- **Pending jobs that expire** before they run.
- **A result that lost its claim** (another scheduler or a requeue has taken
  the job over). That attempt's events belong to whichever claim settles it.
- **Anything in a runtime with no sinks.** Each scheduler emits only for jobs
  it dispatched and results it settled, into its own hub.

## The event

Structured-mode CloudEvents 1.0, JSON. This is a `job.retrying` event from a
sink with `include_payload`, as `to_cloudevent` renders it:

```json
{
  "data": {
    "attempt": 1,
    "error": "ConnectionError: refused",
    "job_id": "0193",
    "namespace": "acme",
    "payload_base64": "AqA=",
    "queue": "emails",
    "task": "send_welcome",
    "timed_out": false,
    "wall_time_ns": 5000000
  },
  "datacontenttype": "application/json",
  "flexiqnamespace": "acme",
  "flexiqqueue": "emails",
  "flexiqtask": "send_welcome",
  "id": "0193:1:7:job.retrying",
  "source": "/flexiq",
  "specversion": "1.0",
  "subject": "0193",
  "time": "2026-01-01T00:00:01.000Z",
  "type": "org.byteveda.flexiq.job.retrying"
}
```

### Attributes

| Attribute | Value |
|---|---|
| `specversion` | `"1.0"` |
| `id` | The dedupe key — see [The id](#the-id-and-deduplication). |
| `source` | The document's `source`. |
| `type` | `org.byteveda.flexiq.` + the short name. |
| `subject` | The job id. |
| `time` | When the runtime observed the transition, RFC 3339 UTC with millisecond precision. |
| `datacontenttype` | `"application/json"` |
| `flexiqnamespace` | The job's namespace; `default` for the default namespace. |
| `flexiqqueue` | The job's queue. |
| `flexiqtask` | The task name. |

### `data`

`job_id`, `namespace`, `queue` and `task` are always present. The rest appear
only when the transition has a value for them — a consumer MUST treat each as
optional, and MUST ignore fields it does not know.

| Field | Type | Present on |
|---|---|---|
| `job_id` | string | every event |
| `namespace` | string | every event (`default` for the default namespace) |
| `queue` | string | every event; empty in the rare case a result settles for a job this scheduler never dispatched |
| `task` | string | every event |
| `attempt` | integer | the attempt's `retry_count`: 0 for the first run. `0` on `job.enqueued`, the row's count on a door `job.cancelled` |
| `error` | string | `job.failed`, and the `job.retrying` / `job.dead` that follows it |
| `timed_out` | bool | the same two-event failure pair |
| `wake_at_ms` | integer | `job.sleeping`: Unix ms the job is rescheduled to |
| `wall_time_ns` | integer | settled outcomes whose execution time was measured |
| `reason` | string | `job.dead` from a shed |
| `payload_base64` | string | see [Payloads](#payloads) |

The claim epoch is not a `data` field; it appears only inside the `id`.

## The id and deduplication

`id` is `<job_id>:<attempt>:<epoch>:<type>`, where `<type>` is the short name
and `-` stands for a part the emitter did not know. Examples captured from the
code: `0192:0:3:job.completed`, and `0195:3:-:job.dead` for a shed, which has
no claim epoch. A door's `job.enqueued` has attempt `0` and no epoch, so it
reads `<job_id>:0:-:job.enqueued`; a door's `job.cancelled` has the row's
attempt and no epoch.

The id is deterministic: every redelivery of one transition carries the same
id. Each part is there because the others can repeat — the attempt separates
retries, the epoch separates two claims of one attempt after a requeue, and
the type separates the `job.failed` and `job.retrying` that one failure
produces.

A consumer **MUST** dedupe on `id` (or on the pair `source` + `id`, if it
reads several FlexiQ deployments). Two different transitions never share an
id; one transition delivered twice always does.

## Delivery semantics

**At-most-once overall**: an event is lost when its sink's buffer is full,
when its delivery attempts run out, or when the process dies. **Each accepted
event may also be delivered more than once**, so consumers dedupe on the
CloudEvents `id`. **Never exactly-once.**

Concretely:

- **No back-pressure.** Emitting never blocks and never does I/O. A sink whose
  buffer is full drops the event and counts it; a slow sink never slows the
  queue, and never delays another sink — each sink has its own buffer and
  thread.
- **Duplicates** come from a retry after a response was lost (the endpoint
  accepted, the answer never arrived), from a batch retried after part of it
  landed, and — rarely — from a debounced enqueue announced twice. All carry
  the same `id`.
- **No ordering guarantee.** One sink delivers in the order its thread took
  events off its buffer, but emits race across threads (a fast task's
  `job.completed` can be emitted before its `job.started`), drops leave gaps,
  and separate runtimes are not ordered at all. Order by `attempt`, then
  `time`, and treat a missing event as possible.
- **Retries**: a batch that fails transiently is retried up to `max_attempts`
  attempts in total, with capped exponential backoff and full jitter — the
  wait before retry *n* is uniform in `[0, min(10 s, 100 ms × 2^(n-1))]`.
  Nothing else is sent by that sink while it waits.
- **A batch is delivered or dropped whole.** One rejection drops every event
  in it.
- **A panicking sink backend** is read as a rejection of that batch, not a
  crash: the sink keeps running.
- **Shutdown** stops accepting events, then delivers what is buffered within
  the runtime's drain budget. No attempt starts and no backoff sleeps past the
  deadline; what is still buffered or in flight then is dropped and counted as
  `shutdown`. `flexiq-server`'s budget is `FLEXIQ_EVENTS_DRAIN`, in whole
  seconds (default 5, zero refused), spent after its roles have stopped so
  their final events are covered. An SDK shell drains when its worker stops,
  within that runtime's drain option (a runtime that waits for in-flight
  handlers on stop starts the budget once they have finished).

## Payloads

Off by default: job arguments can be personal data. A sink sends them only
with `include_payload: true`, and the runtime logs a warning naming each such
sink when the hub starts: `events sink '<name>' sends job payloads`.

Even then, a payload rides only where the emitter already held the job row —
`job.enqueued`, `job.started` and a shed's `job.dead` — as
`data.payload_base64`: standard base64 (with padding) of the job's payload
bytes. Those bytes are the tagged wire envelope
[`REMOTE_SDK_CONTRACT.md`](REMOTE_SDK_CONTRACT.md#serialization) defines, not
JSON. No other event carries one; FlexiQ never reads storage to add it. A
sink without `include_payload` never receives the bytes at all.

## HTTP sink

One `POST` to `url` per batch.

### Batching and content type

| `delivery.max_batch` | Body | `content-type` |
|---|---|---|
| `1` (default) | One CloudEvent object (structured mode) | `application/cloudevents+json` |
| `> 1` | A JSON array of CloudEvents — **always**, even of one | `application/cloudevents-batch+json` |

A sink's content type never changes, so a receiver can dispatch on it once. A
batch holds whatever was already buffered, up to `max_batch`: the sink never
waits for a batch to fill.

### Headers

| Header | Sent | Value |
|---|---|---|
| `content-type` | always | as above |
| `authorization` | with `bearer_token_env` | `Bearer <token>` |
| `x-flexiq-event-timestamp` | with `hmac_secret_env` | Unix **milliseconds**, decimal |
| `x-flexiq-event-signature` | with `hmac_secret_env` | `v1=<64 lowercase hex characters>` |

### Status handling

| Response | Outcome |
|---|---|
| `2xx` | Delivered. The body is not read. |
| `408`, `429`, `5xx` | Retried, up to `max_attempts`. |
| Connect error, timeout, I/O error, an egress refusal when the name resolves | Retried, up to `max_attempts`. |
| Anything else, `3xx` included (redirects are never followed) | Rejected: dropped at once, never retried. |

An endpoint **SHOULD** answer `2xx` once it has durably taken the batch, and
**MUST NOT** answer `2xx` for a batch it will not keep. It SHOULD answer `4xx`
only for a request that can never succeed — a rejected batch is gone.

### Signing

With `hmac_secret_env` set, each request is signed:

```
x-flexiq-event-signature: v1=<hex(HMAC-SHA256(secret, "<timestamp>.<body>"))>
```

- `secret` is the UTF-8 bytes of the environment variable's value.
- `<timestamp>` is exactly the `x-flexiq-event-timestamp` header value.
- `<body>` is the **raw request body bytes, as received**. A verifier MUST
  NOT re-serialise the JSON: key order is not part of this contract.
- The timestamp and signature are recomputed on every attempt; the body of a
  retried batch is unchanged.

A verifier **MUST** compare signatures in constant time and **SHOULD** refuse
a timestamp outside a tolerance window of its choosing (a few minutes) to
bound replay; it SHOULD then dedupe on the event `id`, which also absorbs a
replay inside the window.

The header names are deliberately neither the webhook `x-flexiq-signature` nor
push dispatch's `x-flexiq-dispatch-*`: the signed bytes differ, and sharing a
name would let one verifier accept the other's messages.

**Worked example.** A request the HTTP sink actually sent, captured by a test
server — secret `whsec-example`, `max_batch` 1:

```
content-type: application/cloudevents+json
x-flexiq-event-timestamp: 1790287573410
x-flexiq-event-signature: v1=015a8e23f5777f7b7f154c0c333245b6017e6f95a129ddae8072bc26bd3e6682
content-length: 402

{"data":{"attempt":0,"job_id":"0192","namespace":"default","queue":"emails","task":"send_welcome","wall_time_ns":41250000},"datacontenttype":"application/json","flexiqnamespace":"default","flexiqqueue":"emails","flexiqtask":"send_welcome","id":"0192:0:3:job.completed","source":"/flexiq","specversion":"1.0","subject":"0192","time":"2026-01-01T00:00:00.123Z","type":"org.byteveda.flexiq.job.completed"}
```

Recomputed independently:

```python
import hashlib, hmac
body = b'{"data":{"attempt":0,...,"type":"org.byteveda.flexiq.job.completed"}'  # the 402 bytes above
hmac.new(b"whsec-example", b"1790287573410." + body, hashlib.sha256).hexdigest()
# '015a8e23f5777f7b7f154c0c333245b6017e6f95a129ddae8072bc26bd3e6682'
```

### Egress guard

The HTTP sink sends through push dispatch's client and egress policy
(`DispatchClient` + `EgressPolicy`), so every rule of that guard applies:

- **Deny by default.** `allow` is required and non-empty. Each entry is a CIDR
  (contains `/`), a bare IP address, a domain suffix (leading `.`, e.g.
  `.example.com`), or an exact host name; names match case-insensitively.
- **Checked at start and at every connect.** The URL's host must be allowed
  when the sink is built, and every address a name resolves to is vetted when
  it is dialled — one refused address refuses the whole resolution, which
  closes DNS rebinding. An address passes when a CIDR covers it, or when a
  name rule named the host.
- **Refused whatever `allow` says**: link-local (cloud metadata endpoints
  included), the metadata literals, multicast, broadcast and unspecified
  addresses — and loopback unless `allow_loopback` is set. There is no
  "allow private" escape hatch beyond naming the network in `allow`.
- **`https` only**, except cleartext `http` to a loopback host with
  `allow_loopback`.
- **No proxy, no redirects.** Proxy environment variables are ignored (a
  proxy would resolve the target outside the pinned resolver), and a `3xx`
  is a rejection.

## Redis Streams sink

One pipelined `XADD <stream> MAXLEN ~ <max_len> * <fields…>` per event in the
batch. Each entry carries the routing attributes as their own fields, so a
consumer can filter without parsing JSON, plus the whole structured CloudEvent
— byte for byte what the HTTP sink would send — as `event`:

| Field | Value |
|---|---|
| `id` | The event id |
| `type` | The full CloudEvents type |
| `source` | The document's `source` |
| `time` | RFC 3339 UTC, milliseconds |
| `namespace` | Namespace label (`default` for the default namespace) |
| `queue` | Queue |
| `task` | Task name |
| `event` | The structured CloudEvent JSON, payload per the sink's `include_payload` |

A shed as the sink writes it, captured from the code:

```
id        = 0195:3:-:job.dead
type      = org.byteveda.flexiq.job.dead
source    = /flexiq
time      = 2026-01-01T00:00:03.000Z
namespace = default
queue     = emails
task      = send_welcome
event     = {"data":{"attempt":3,"job_id":"0195","namespace":"default","queue":"emails","reason":"rate_limit: task 'send_welcome' is over its dispatch rate limit, and its on_excess is drop","task":"send_welcome"},"datacontenttype":"application/json","flexiqnamespace":"default","flexiqqueue":"emails","flexiqtask":"send_welcome","id":"0195:3:-:job.dead","source":"/flexiq","specversion":"1.0","subject":"0195","time":"2026-01-01T00:00:03.000Z","type":"org.byteveda.flexiq.job.dead"}
```

- The stream entry id is Redis's (`*`), not the event id; dedupe on the `id`
  field.
- **`MAXLEN ~` trims the stream.** A consumer that falls more than roughly
  `max_len` entries behind loses the oldest; size `max_len` for the longest
  outage a consumer group must survive.
- A connection or I/O error is retried (the connection is dropped and
  re-dialled); any error reply from the server — `WRONGTYPE`, an ACL refusal,
  and also transient replies such as `LOADING` — rejects the batch. Connect,
  read and write each time out after 5 seconds.
- A pipeline that failed part-way is retried whole, so entries that already
  landed appear again under the same `id`.

## Metrics

Per sink, in Prometheus text format. `flexiq-server` appends them to its
`/metrics` output (the dashboard's and the gRPC listener's) only when events
are configured; an SDK shell exposes the same counters through its worker's
event-sink statistics.

| Metric | Type | Labels |
|---|---|---|
| `flexiq_events_delivered_total` | counter | `sink` |
| `flexiq_events_dropped_total` | counter | `sink`, `reason` |
| `flexiq_events_queued` | gauge | `sink` |

`reason` is one of:

| `reason` | Dropped because |
|---|---|
| `buffer_full` | The sink's buffer was full when the event was emitted. |
| `rejected` | The destination refused the batch for good, or the sink backend panicked on it. |
| `failed` | Every one of `max_attempts` attempts failed transiently. |
| `shutdown` | The runtime was shutting down: emitted after close, or still buffered or in flight at the drain deadline. |

Every reason is rendered for every sink from start, zero included, so a rate
query has a series before the first drop. `flexiq_events_queued` counts
events accepted but not yet delivered or dropped; it is approximate while
events are in motion. A sink's drops are also logged, naming the sink, the
reason and a count — never an event's contents or a credential.

```
flexiq_events_delivered_total{sink="web"} 0
flexiq_events_dropped_total{sink="web",reason="buffer_full"} 0
flexiq_events_dropped_total{sink="web",reason="rejected"} 0
flexiq_events_dropped_total{sink="web",reason="failed"} 0
flexiq_events_dropped_total{sink="web",reason="shutdown"} 0
flexiq_events_queued{sink="web"} 0
```

## What this contract does not promise

- **No exactly-once, no completeness.** Dedupe on `id`; reconcile against
  FlexiQ's own APIs if a missing event matters.
- **No ordering**, within a sink or across them.
- **No event for transitions a runtime did not observe** — see
  [What is not seen](#what-is-not-seen).
- **No acknowledgement back into the queue.** A consumer's answer decides
  only whether that sink retries; it never affects the job.
- **No persistence of undelivered events.** The buffer is in memory; a
  process that dies loses it.
