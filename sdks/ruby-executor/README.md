# FlexiQ Ruby executor

A Ruby client for the **executor door** of a running
[`flexiq-server`](../../crates/flexiq-server): attach over gRPC, run the jobs the scheduler
dispatches, and settle each one with its result or its error.

It is the other half of the [`flexiq`](../ruby) producer gem. The two are separate gems because
they are separate doors behind separate scopes: an `execute` token cannot enqueue, a `produce`
token cannot attach, and this door has no HTTP binding, so it needs the `grpc` gem's native
extension where the producer gem needs nothing at all.

## Install

Ruby 3.3 or newer. Neither gem is on RubyGems yet; take both from this repository with Bundler:

```ruby
# Gemfile
git "https://github.com/ByteVeda/flexiq", glob: "sdks/{ruby,ruby-executor}/*.gemspec" do
  gem "flexiq"
  gem "flexiq-executor"
end
```

`grpc` and `google-protobuf` ship precompiled for Linux and macOS, so nothing is compiled on
install.

## Quickstart

```ruby
require "flexiq/executor"

worker = FlexiQ::Executor::Worker.new("flexiq.internal:50051",
                                      token: ENV.fetch("FLEXIQ_EXECUTE_TOKEN"),
                                      id: "billing-1",
                                      slots: 8)

worker.handle("billing.charge") do |job|
  order = job.args.first            # positional arguments, decoded from the payload
  job.progress(50)
  job.log("info", "charging", extra: { "order" => order["id"] })
  { "receipt" => Gateway.charge(order) }  # the job's result
end

trap("TERM") { worker.stop }
worker.run
```

`run` blocks. It attaches, dispatches each job to its handler on a thread of its own, and
reconnects when the scheduler rotates the stream, which it does every half hour by default
(`FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE`): a gRPC stream cannot be load-balanced once it has
started. A rotation is not a failure and costs no job in flight. `run` returns when the
scheduler sends `shutdown` or `stop` is called; it raises `FlexiQ::Executor::AttachError` for a
refusal that reconnecting cannot fix, such as a bad token or a protocol version mismatch.

TLS is on by default and uses the system roots. Pass `credentials:` (a
`GRPC::Core::ChannelCredentials`) for a private CA, or `insecure: true` for loopback only.

## What a handler returns

- **A value** is a success. It is encoded with the producer gem's CBOR codec, so a producer reads
  it back with `FlexiQ::Payload.decode_result`. `nil` means the task returned nothing.
- **An exception** is a failure, written to `Job#error` as
  `{"errtype","message","traceback"}` JSON. It is retried while retries remain.
- **`FlexiQ::Executor::Fatal`** fails the job without a retry. Whether to retry is the
  executor's decision: only it can see the exception. Raised inside a `rescue`, the error names
  the original exception.

## Cancellation and timeouts

Ruby cannot stop a thread safely, so both are cooperative. `job.check!` raises
`FlexiQ::Executor::Cancelled` once the scheduler asked for the job to stop, and
`FlexiQ::Executor::TimeoutError` once its timeout has passed; a long handler calls it between
units of work. A handler that lets `Cancelled` propagate settles the job as cancelled. One that
raises after its timeout settles it as a timed-out failure, with the same message the server
writes for a timeout it reaps itself.

`job.cancelled?`, `job.timed_out?` and `job.deadline` (monotonic seconds) are there for handlers
that check on their own terms.

## Settings

| Keyword | Default | |
|---|---|---|
| `token:` | required | carries the `execute` scope |
| `id:` | `ruby-<host>-<pid>` | unique among attached executors; a second attach under one id is refused, and retried with backoff |
| `slots:` | processor count | jobs run at once |
| `credentials:` / `insecure:` | system roots / `false` | transport security |
| `max_message_bytes:` | 68 MiB | the door's own ceiling; gRPC's 4 MiB default fails on the first large job |
| `handshake_timeout:` | 10 | seconds to wait for `hello_ack` |
| `heartbeat_interval:` | 5 | seconds between heartbeats |
| `shutdown_drain:` | 30 | seconds running jobs get to finish when draining |
| `backoff_min:` / `backoff_max:` | 0.25 / 30 | reconnect delay bounds, in seconds |
| `logger:` | `$stderr` at INFO | any `Logger` |

## What this gem does not do

- **Durable steps.** The `steps` capability is not advertised, so the scheduler sends no step
  frames. It fails rather than degrades, so it will land as its own feature.
- **Enqueue.** The executor door has no enqueue-shaped RPC. A task that fans out uses the
  `flexiq` gem with a `produce` token of its own.

The wire contract this implements is
[`contracts/REMOTE_SDK_CONTRACT.md`](../../contracts/REMOTE_SDK_CONTRACT.md).

## Development

```sh
bundle install
bundle exec rake            # RuboCop and the unit suite, against an in-process scheduler
bundle exec rake e2e        # against a real flexiq-server; see test/e2e/support/server.rb
buf generate                # regenerate lib/flexiq/executor/v1 after a contracts/proto edit
```
