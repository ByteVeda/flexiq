# FlexiQ Ruby client

A Ruby client for the **producer door** of a running
[`flexiq-server`](../../crates/flexiq-server): submit work, read it back, cancel it, count it.
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

client = FlexiQ::Client.new("https://flexiq.internal:8443", token: ENV.fetch("FLEXIQ_TOKEN"))

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

The URL is the server's gRPC listener (`FLEXIQ_GRPC_LISTEN`); the JSON facade shares it.

## Surface

| Method | Route | Notes |
|---|---|---|
| `enqueue(task, args:, kwargs:, **options)` | `POST /v1/jobs` | Options are `FlexiQ::EnqueueOptions` fields. |
| `enqueue_batch(requests)` | `POST /v1/jobs:batchEnqueue` | One `BatchItemResult` per item. Not atomic. |
| `get_job(id, include_payload:, include_result:)` | `GET /v1/jobs/{id}` | Payload and result only on request. |
| `cancel_job(id)` | `POST /v1/jobs/{id}:cancel` | Idempotent. |
| `queue_stats(queue = nil)` | `GET /v1/queues/{queue}/stats`, `GET /v1/stats` | `nil` counts the whole namespace. |

Enqueue options: `queue`, `priority`, `max_retries`, `scheduled_at` (`Time`), `timeout`
(seconds), `unique_key`, `metadata`, `notes`, `depends_on`, `expires_at` (`Time`),
`result_ttl` (seconds), `debounce` (`FlexiQ::Debounce` or a Hash of its fields).

The namespace is the token's, fixed when an operator minted it. No call can name another, and a
job in another namespace reads as `JOB_NOT_FOUND`. If enqueues succeed and nothing ever runs
them, check the namespace first.

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

Other keywords: `open_timeout`, `read_timeout`, `write_timeout` (seconds) and `user_agent`.

## Errors

Every error is a `FlexiQ::Error`:

| Class | Means |
|---|---|
| `FlexiQ::RPCError` | The server refused. Branch on `#reason` (`FlexiQ::Reason::QUEUE_FULL`, …), never on the message. |
| `FlexiQ::TransportError` | No answer arrived: connect, TLS, timeout, or a body that was not FlexiQ's. |
| `FlexiQ::ConfigurationError` | The client was built with settings it will not honour. |
| `FlexiQ::CodecError` | A value could not be encoded into, or decoded from, a payload. |

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

The client never retries. A write that raised `TransportError`, or `RPCError` with code
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
