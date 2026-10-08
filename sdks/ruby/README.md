# FlexiQ Ruby client

A Ruby client for the **producer door** of a running
[`flexiq-server`](../../crates/flexiq-server): submit work, read it back, wait for it, cancel it,
count it, and submit static workflows.
No database credential, no native extension, no gRPC toolchain — it speaks the server's JSON
facade with the Ruby standard library alone.

It is a producer client, not an SDK: it **cannot execute tasks**. It submits work that
somebody else's workers drain.

## Install

Ruby 3.3 or newer. The gem is not on RubyGems yet; take it from this repository with Bundler:

```ruby
# Gemfile
gem "flexiq", git: "https://github.com/ByteVeda/flexiq", glob: "sdks/ruby/*.gemspec"
```

## Quickstart

```ruby
require "flexiq"

client = FlexiQ::Client.new("https://flexiq.internal:50051", token: ENV.fetch("FLEXIQ_TOKEN"))

result = client.enqueue("orders.send_receipt",
                        args: [{ "order_id" => "ord-0001" }],
                        queue: "emails",
                        unique_key: "receipt-ord-0001")
job = client.get_job(result.job.id, include_result: true)
job.status       # => :pending, :running, :complete, :failed, :dead or :cancelled
job.terminal?    # => false until it finishes for good

client.cancel_job(job.id)
client.queue_stats("emails").pending
```

The URL is the address the server's `FLEXIQ_GRPC_LISTEN` binds (these examples use port `50051`);
the JSON facade shares that listener.

## Surface

| Method | Route | Notes |
|---|---|---|
| `enqueue(task, args:, kwargs:, **options)` | `POST /v1/jobs` | Options are `FlexiQ::EnqueueOptions` fields. |
| `enqueue_batch(requests)` | `POST /v1/jobs:batchEnqueue` | One `BatchItemResult` per item. Not atomic. |
| `get_job(id, include_payload:, include_result:)` | `GET /v1/jobs/{id}` | Payload and result only on request. |
| `list_jobs(status:, queue:, task_name:, page_size:, page_token:)` | `GET /v1/jobs` | One `JobPage`, newest first. See [Listing](#listing). |
| `each_job(status:, queue:, task_name:, page_size:)` | `GET /v1/jobs` | An `Enumerator` over every page. |
| `watch_jobs(ids)` | `GET /v1/jobs:watch` (SSE) | Yields each job's transitions until all are finished. See [Waiting](#waiting). |
| `watch_queue(queue = nil, resume_cursor:)` | `GET /v1/jobs:watch` (SSE) | Yields a queue's live transitions until you break. |
| `wait(id, timeout:)` | `GET /v1/jobs:watch`, then `GET /v1/jobs/{id}` | The finished `Job`, with its result. |
| `cancel_job(id)` | `POST /v1/jobs/{id}:cancel` | Idempotent. |
| `queue_stats(queue = nil)` | `GET /v1/queues/{queue}/stats`, `GET /v1/stats` | `nil` counts the whole namespace. |
| `submit_workflow(name, graph, params_json:)` | `POST /v1/workflows` | The run id. Static graphs only; not idempotent. See [Workflows](#workflows). |
| `get_workflow_run(run_id)` | `GET /v1/workflows/{run_id}` | A `WorkflowRunView`: the run and every node. |

Enqueue options: `queue`, `priority`, `max_retries`, `scheduled_at` (`Time`), `timeout`
(seconds), `unique_key`, `metadata`, `notes`, `depends_on`, `expires_at` (`Time`),
`result_ttl` (seconds), `debounce` (`FlexiQ::Debounce` or a Hash of its fields).

The namespace is the token's, fixed when an operator minted it. No call can name another, and a
job in another namespace reads as `JOB_NOT_FOUND`. If enqueues succeed and nothing ever runs
them, check the namespace first.

## Listing

`list_jobs` reads one page, newest first. Every filter is optional, and `status:` takes the
`FlexiQ::JobStatus` symbols (`:pending`, `:failed`, …). Rows never carry payload or result; read
one job with `get_job` for those.

```ruby
page = client.list_jobs(queue: "emails", status: :failed, page_size: 100)
page.jobs            # => [FlexiQ::Job, ...]
page.next_page_token # => pass back as page_token: for the next page; nil on the last

client.each_job(queue: "emails", status: :failed).first(500) # follows the cursor for you
```

`next_page_token` is opaque. Pass it back unchanged and read nothing out of it; a page token the
server did not issue is refused with `RPCError` reason `INVALID_REQUEST`. `page_size` defaults to
the server's choice (50) and is capped (500), so trust `last_page?`, not the row count.

A credential whose `produce` or `read` grant is narrowed must name what the grant narrows: a
`queue:` it reaches for a queue-narrowed grant, a `task_name:` it reaches for a task-narrowed one,
and both when the grant narrows both. Otherwise `list_jobs` is refused with `SCOPE_DENIED`. A
listing is never filtered down to the rows the credential can see.

## Waiting

Watch a job instead of polling `get_job`. `wait` blocks until the job is finished and reads it
back with its result:

```ruby
job = client.wait(job_id, timeout: 60) # raises FlexiQ::WaitTimeoutError after 60 s; nil = no limit
job.status        # => :complete, :dead or :cancelled
job.decode_result
```

`watch_jobs` yields every step on the way. Each id opens with a `:snapshot` of its current
state (or a `FlexiQ::JobNotFound`), then live transitions follow, and the call returns once
every job is finished:

```ruby
client.watch_jobs([a, b]) do |item|
  case item
  in FlexiQ::JobTransition => t then puts "#{t.job_id} #{t.kind} -> #{t.status}"
  in FlexiQ::JobNotFound => n then puts "#{n.job_id} not visible to this token"
  end
end
```

Decide that a job is finished by `JobTransition#terminal?`, never by its status: `:failed` is
not final while a retry or a dead-letter is still to come.

`watch_queue` follows every job in a queue until you `break`. It has no snapshot. It opens with a
`FlexiQ::WatchCheckpoint`, and then yields one `JobTransition` per live change. Every item
carries a `cursor`; keep the last one, checkpoints included, and pass it back as
`resume_cursor:` to replay what you missed:

```ruby
client.watch_queue("emails", resume_cursor: saved) do |item|
  case item
  in FlexiQ::WatchGap then reconcile_with(client.list_jobs(queue: "emails"))
  in FlexiQ::JobTransition => t then handle(t)
  else nil
  end
  saved = item.cursor if item.respond_to?(:cursor)
end
```

Watches keep themselves alive:

- **Dropped connection, `WATCH_OVERFLOW` or `SHUTTING_DOWN`:** the watch reopens after a
  backoff. An id watch reopens on the jobs that are not finished yet, each with a fresh
  snapshot. A queue watch resumes from its last cursor.
- **Expired cursor** (`WATCH_CURSOR_EXPIRED`): the server keeps a bounded window, per process.
  The queue watch yields a `FlexiQ::WatchGap` and starts again from now. The transitions in the
  gap are gone; `list_jobs` reads current state, not history.
- **Failure before the first item:** it raises. This covers a wrong URL, a missing `read`
  scope, and a token already at its cap of concurrent watches (`WATCH_LIMIT`;
  `RPCError#watch_limit` is the cap).

A queue watch only sees transitions that the server process it reached handles itself. Each
watch holds a connection of its own, so other calls on the same client are never blocked.
`watch_read_timeout:` (default 60 s) is how long a watch may stay silent before it counts as
dropped. The server sends a keepalive every 15 s.

## Workflows

A workflow is a graph of steps. Submitting one enqueues a job per node, chained by the edges, and
any worker with workflow tracking enabled advances the run, not only whoever submitted it.

```ruby
graph = FlexiQ::WorkflowGraph.new(
  nodes: [
    FlexiQ::WorkflowNode.new(name: "charge", task_name: "orders.charge", args: [order], queue: "payments"),
    FlexiQ::WorkflowNode.new(name: "ship", task_name: "orders.ship", args: [order],
                             condition: :on_success, timeout: 30, compensate: "orders.unship")
  ],
  edges: [%w[charge ship]] # or FlexiQ::WorkflowEdge.new(from: "charge", to: "ship")
)
run_id = client.submit_workflow("checkout", graph, params_json: '{"order":"ord-0001"}')

view = client.get_workflow_run(run_id)
view.run.state            # => :pending, :running, :completed, :failed, … (FlexiQ::WorkflowState)
view.run.terminal?
view.node("ship").status  # => :pending, :running, :completed, … (FlexiQ::WorkflowNodeStatus)
view.node("ship").job_id  # read it with get_job
```

A node takes the arguments `enqueue` does and sends the same `raw` bytes, plus `queue`,
`max_retries`, `timeout` (seconds), `priority`, `condition` (`:on_success`, `:on_failure`,
`:always`) and `compensate`. A Hash of a node's fields works wherever a node does. The graph is
checked before the call: at least one node, unique names, and no edge to an undeclared node.

**This door advances static graphs only.** A node setting `gate`, `cache`, `fan_out`, `fan_in`
or `sub_workflow` is refused before anything is written; only a live SDK process can drive those.
The refusal names one node, so a graph with several clears them one call at a time:

```ruby
begin
  client.submit_workflow("review", graph)
rescue FlexiQ::RPCError => e
  raise unless e.reason == FlexiQ::Reason::WORKFLOW_CONSTRUCT_UNSUPPORTED

  e.workflow_construct # => { node: "review", field: "gate" }
end
```

Every submission is version 1 of `name`. Resubmitting a name with a different graph is refused
with `INVALID_ARGUMENT`; submit a changed graph under a new name.

**`submit_workflow` is not idempotent**, and a workflow has no `unique_key`. A run cannot be found
by name, so a call that raised `TransportError` or an ambiguous `RPCError` (`UNAVAILABLE`,
`DEADLINE_EXCEEDED`, `CANCELLED`) may have submitted a run whose id is lost. A retry submits a
second run. Never retry it blind.

A narrowed `produce` grant is checked on every node, as `enqueue` would check it, with an empty
queue counted as `default`. The first node outside the grant refuses the whole graph:
`SCOPE_DENIED`, with `RPCError#node` naming that node. A node's `compensate` task must be reached
on every queue. `get_workflow_run` answers `NOT_FOUND` unless the grant reaches every node's job.

## Credentials and TLS

Every call sends `Authorization: Bearer <token>`. The token is opaque; the client never parses it.

`https://` is the default and verifies the server's certificate. Point at a private CA, or
present a client certificate for mTLS, with `tls:`:

```ruby
FlexiQ::Client.new(url, token: token, tls: {
  ca_file: "/etc/flexiq/ca.pem",
  cert: OpenSSL::X509::Certificate.new(File.read("client.pem")),
  key: OpenSSL::PKey.read(File.read("client.key"))
})
```

`http://` is refused unless you pass `insecure: true`, which is for a loopback listener you
trust. A bearer token on a plaintext network hop can be replayed by anything that sees it.

Other keywords: `open_timeout`, `read_timeout`, `write_timeout`, `watch_read_timeout` (seconds)
and `user_agent`.

## Errors

Every error is a `FlexiQ::Error`:

| Class | Means |
|---|---|
| `FlexiQ::RPCError` | The server refused. Branch on `#reason` (`FlexiQ::Reason::QUEUE_FULL`, …), never on the message. |
| `FlexiQ::TransportError` | No answer arrived: connect, TLS, timeout, or a body that was not FlexiQ's. |
| `FlexiQ::ConfigurationError` | The client was built with settings it will not honour. |
| `FlexiQ::CodecError` | A value could not be encoded into, or decoded from, a payload. |
| `FlexiQ::WaitTimeoutError` | `wait` ran out of time. The job is untouched. |

```ruby
begin
  client.enqueue("orders.charge", args: [order], queue: "payments", unique_key: order["id"])
rescue FlexiQ::RPCError => e
  case e.reason
  when FlexiQ::Reason::QUEUE_FULL then sleep(e.retry_after || 1) # e.queue_full => {queue:, pending:, cap:}
  when FlexiQ::Reason::SCOPE_DENIED then raise "token lacks #{e.scope}"
  else raise
  end
end
```

`RPCError#retryable?` says whether the condition clears on its own. It never says a write is
safe to resend.

## Retries

The client never retries a call; only watches reopen themselves, since a watch writes nothing.
A write that raised `TransportError`, or `RPCError` with code
`UNAVAILABLE`, `DEADLINE_EXCEEDED` or `CANCELLED`, may have landed before the connection dropped,
and nothing on the wire says which. Resend a write only with `unique_key` set, reusing the same
value. `unique_key` dedupes against the *active* job only — once that job finishes the key is
free — so keep the total retry window shorter than the job's own life.

## Payloads

Arguments travel as the `raw` CBOR envelope this gem writes itself — tag byte `0x02`, then
`[args, kwargs]` — held to the contract's three rules: definite-length containers,
shortest-form integers and 64-bit floats. The bytes are therefore the same ones any other FlexiQ
runtime writes for the same call, which is what keeps cross-runtime `auto:` idempotency keys
equal. Hash keys keep their insertion order.

| Ruby | CBOR |
|---|---|
| `nil`, `true`, `false` | simple values |
| `Integer` (64-bit range) | integer |
| `Float` | 64-bit float |
| `String` | text — or a byte string when its encoding is `Encoding::BINARY` |
| `Symbol` | text |
| `Array`, `Hash` | array, map |

Prefer one Hash argument (`args: [{...}]`): it binds cleanly in every runtime's handlers.

Reading back: `job.decode_payload` returns `[args, kwargs]`, `job.decode_result` the task's
return value, and `job.task_error` a `FlexiQ::TaskError` — structured when the failure carried the
canonical JSON, otherwise the raw text verbatim.

## Development

```bash
bin/setup            # bundle install
bundle exec rake     # rubocop + unit suite (includes the wire-vector conformance test)
bundle exec rake e2e # against a real server; build one first:
                     #   cargo build -p flexiq-server --features grpc
```

The e2e suite runs `FLEXIQ_SERVER_BIN`, or else `target/{debug,release}/flexiq-server`.

## Contract

[`contracts/REMOTE_SDK_CONTRACT.md`](../../contracts/REMOTE_SDK_CONTRACT.md) is the rule this
client implements, and [`contracts/wire-vectors.json`](../../contracts/wire-vectors.json) is the
conformance bar its suite asserts.
