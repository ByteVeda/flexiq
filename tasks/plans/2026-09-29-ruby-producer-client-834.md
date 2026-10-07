# Ruby producer client — #834

Branch `feat/ruby-producer-client` off `master`, in worktree `../taskito-ruby`.
Commits authored as kartikeya-27; PR raised as pratyush618.

## Scope

Producer door only, per the issue: `Enqueue`, `EnqueueBatch`, `GetJob`,
`CancelJob`, `QueueStats`. Out: `ListJobs`, workflows, `WatchJobs`, executor
door, admin door, gem publishing.

## Decisions

1. **Transport = JSON facade over `Net::HTTP`, not gRPC.** The contract names
   the facade as a first-class route ("If there is no gRPC library either").
   The `grpc` gem is a native extension plus a protoc codegen step — exactly
   the build weight the issue says this client should not carry. Facade gives
   **zero runtime dependencies**, stdlib only. Cost: no `WatchJobs` (no HTTP
   path) — out of scope anyway.
2. **Payload = `raw` arm, own CBOR codec.** `structured` normalises key order
   and refuses >2^53 ints / bytes, so `auto:` keys would not match peers.
   A ~200-line pure-Ruby encoder/decoder pins the three encoder rules
   (definite length, shortest int, `fb` finite floats) and passes every
   `wire-vectors.json` case, kwargs included (Ruby has keyword args).
   Hash insertion order is preserved → no sorting, `single-object-arg` holds.
3. **TLS by default.** `https://` verifies the peer (`VERIFY_PEER`, optional
   `ca_file`/client cert for mTLS). `http://` refused unless
   `insecure: true` — mirrors the Go client's explicit opt-in.
4. **No automatic retries.** Contract forbids blind write retries.
   `FlexiQ::Error#retryable?` / `#retry_after` expose the decision instead.
5. **Errors branch on `reason`.** `FlexiQ::Error` carries `code`, `status`,
   `reason`, `metadata`, `retry_after`, `index`; `FlexiQ::Reason` constants
   hold the closed list. Unparseable metadata numbers → `nil`.
6. **Unknown `JobStatus` is never terminal**; unknown JSON fields ignored.
7. **Ruby floor 3.3** (3.2 EOL 2026-03). `Data.define` for value objects.
   CI matrix 3.3 / 3.4 / 4.0.

## Layout

```
sdks/ruby/
  flexiq.gemspec  Gemfile  Gemfile.lock  Rakefile  README.md
  .rubocop.yml  .gitignore  bin/console  bin/setup
  lib/flexiq.rb                    single barrel
  lib/flexiq/version.rb            mirrored by scripts/version.mjs
  lib/flexiq/client.rb             public API, five RPCs
  lib/flexiq/transport.rb          Net::HTTP, bearer, TLS, JSON, failure mapping
  lib/flexiq/errors.rb             Error / ConfigurationError / CodecError / TransportError
  lib/flexiq/reason.rb             closed ErrorInfo reason list
  lib/flexiq/rpc_error.rb          google.rpc.Status → RPCError
  lib/flexiq/job.rb  job_status.rb  task_error.rb
  lib/flexiq/debounce.rb  enqueue_options.rb  enqueue_request.rb
  lib/flexiq/enqueue_result.rb  batch_item_result.rb  queue_stats.rb
  lib/flexiq/wire/{bytes,duration,int64,path,timestamp}.rb   proto3 JSON scalars
  lib/flexiq/cbor/{encoder,decoder}.rb
  lib/flexiq/payload.rb            envelope tag byte, call body / result
  test/unit/*_test.rb              incl. wire_vectors_test.rb; client tests via test/support/fake_server.rb
  test/e2e/producer_test.rb        real flexiq-server; test/e2e/support/server.rb drives it
```

## API sketch

```ruby
client = FlexiQ::Client.new("https://flexiq.internal:50051", token: ENV["FLEXIQ_TOKEN"])
res = client.enqueue("send_receipt", args: [{ order_id: "o-1" }], queue: "emails", unique_key: "o-1")
res.job.id; res.deduplicated?
client.enqueue_batch([FlexiQ::EnqueueRequest.new(task_name: "t", args: [1])])
job = client.get_job(id, include_result: true); job.result  # decoded CBOR
client.cancel_job(id)
client.queue_stats("emails")  # or nil → namespace-wide
```

## Tasks

- [x] Local Ruby 3.4.6 (built from source into `~/.rubies`; the prebuilt tarball is not relocatable)
- [x] Gem scaffold (gemspec, Gemfile, Rakefile, rubocop, bin/, barrel, version)
- [x] CBOR codec + wire-vectors conformance test
- [x] Payload envelope (tags 0x00/0x01 rejected by name)
- [x] Errors + Reason list
- [x] Models (Job, JobStatus, QueueStats, TaskError, options/results)
- [x] Transport (TLS, bearer, JSON, status → Error)
- [x] Client five RPCs + fake-server tests
- [x] E2E against `flexiq-server` (cargo build -j1 -p flexiq-server --features grpc)
- [x] `scripts/version.mjs` mirror for `version.rb` and `Gemfile.lock`
- [x] CI: `ci-ruby.yml`, `ci-ruby-e2e.yml`, dispatcher filters, `ci-status.needs`, labeler
- [x] Pre-commit rubocop hook
- [x] Docs: gem README, `server/clients.mdx` callout, root README remote-clients row

## Review

- rubocop clean; unit 81 runs / 230 assertions; e2e 8 runs / 26 assertions against a
  debug `flexiq-server`; `gem build` succeeds; `version.mjs --check` passes.
- E2E pins the stored payload byte-equal to this gem's encoder, so the `raw` arm is untouched.
- Not run locally: the Ruby 3.3 and 4.0 matrix legs (CI only), and the README's Bundler
  `git:` + `glob:` install line, which resolves only once the branch is on `master`.
