# A Go executor client over the dispatch door — implementation plan

Issue [#908](https://github.com/ByteVeda/flexiq/issues/908). Design:
`tasks/specs/2026-09-11-go-executor-client-design.md`.

Branch `feat/go-executor-client`, worktree `.claude/worktrees/go-executor-908`,
off master `755f54b4`.

## Global constraints

- `sdks/go` is a gRPC client and links nothing from `crates/`. No Rust
  toolchain is needed for anything but the e2e suite.
- Every commit must pass `make check` (`build vet lint race`) on its own —
  pre-commit stashes unstaged *tracked* edits, so a split that leaves a package
  uncompilable fails the hook on a tree that looks fine in the editor.
  Untracked files are **not** stashed: park files belonging to later commits in
  `/tmp` or they get vetted against a stashed source.
- `golangci-lint` is on `~/go/bin`, pinned v2.13.2, and must agree with
  `sdks/go/Makefile` and `.github/workflows/ci-go.yml`. `buf` is 1.72.0, pinned
  by `contracts/BUF_VERSION`.
- No `//nolint` in the tree. There are none today; do not be the first.
- `revive`'s `exported` rule is an error, so every exported identifier needs a
  doc comment starting with its own name.
- No sibling SDK is named in this package's code, docs or PR. "Cross-SDK
  contract" is the phrase.

## Build order

Each task ends with a verification command whose output must be read, not
assumed.

### Task 1 — Generate the executor stubs

`sdks/go/buf.gen.yaml`: add `../../contracts/proto/flexiq/executor/v1` to
`inputs[0].paths`. The existing comment says this module does not open that
door; rewrite it to say it now opens both and why they stay one module (one
codec, one credential type, one version).

`paths:` entries resolve against cwd, not against `directory:` — the full
`../../contracts/proto/...` prefix is required or buf reports "is outside the
context directory".

Then `make generate`. Stubs land under
`sdks/go/internal/pb/flexiq/executor/v1/` and are committed.

Verify: `buf generate && git status --short internal/pb` shows the new files and
no change to `flexiq/v1`; `go build ./...`.

### Task 2 — `EncodeResult` and the task-error encoder in package `flexiq`

`wire.go` gains `EncodeResult(v any) ([]byte, error)`: tag byte plus a bare CBOR
value, no array wrapper — the asymmetry with `EncodeCall` is the point and the
doc comment says so.

`taskerror.go` gains the encoder beside `ParseTaskError`. Key order
`errtype`, `message`, `traceback`, no extra whitespace, `traceback` `[]` and
never null. A round-trip test against the contract's byte-exact vector:
`{"errtype":"BoomError","message":"it broke","traceback":["frame1","frame2"]}`.

Verify: `go test ./... -run TaskError -v` and a new wire vector test for
`EncodeResult`/`DecodeResult`.

### Task 3 — `executor` package skeleton: options, errors, doc

`options.go` mirrors the producer client's option set — token, TLS,
transport credentials, insecure, user agent, raw dial options — plus the
executor's own: `WithID`, `WithSlots`, `WithHeartbeatInterval`,
`WithHandshakeTimeout`, `WithShutdownDrain`, `WithReconnectBackoff`,
`WithLogger`, `WithSDK`.

`MaxMessageBytes = 68 * 1024 * 1024`, its own constant with its own comment. Do
not reuse the producer client's 4 MiB.

Defaults copied, not invented: handshake 10s, heartbeat 5s, shutdown drain 30s,
backoff 250ms→30s. The first three are the reference executor's
(`ExecutorConfig::new`).

`errors.go`: `Fatal(err)`, `ErrFatal`, `ErrCancelled`, and the permanent attach
errors so `Run` can return one without reconnecting.

Verify: `go build ./... && golangci-lint run`.

### Task 4 — `Job`, and the payload decode

`job.go`: the struct mirrors `JobFrame` field for field, with `Timeout` a
`time.Duration` and `Metadata` an opaque string.

`Call()` returns `flexiq.Call`. `Bind(v)` decodes the first positional argument
into `v` and **refuses a non-empty kwargs map** naming how many it carried —
the same call the Rust SDK made, for the same reason.

`Progress`, `Log` and `Publish` are the `side_channel` surface. Each is a no-op
when the scheduler did not advertise the capability, and each clamps or bounds
what the frame allows. `Publish` is `Log` at level `result` with the value in
`extra` and an empty message.

Verify: unit tests decoding a payload built by `flexiq.EncodeCall`.

### Task 5 — Slots and the settling frames

`slots.go`: a counting semaphore plus a free-slot gauge the heartbeat reads.

`result.go`: handler outcome to `success`/`failure`/`cancelled`, per the table
in the design. Panic recovery with `runtime/debug.Stack()` split into
`traceback`. Absent vs present-and-empty `result` kept apart.

Verify: table tests over every row of the outcome table.

### Task 6 — `attach.go`, one stream session

The core of the change. A `session` owns one `Attach` stream:

1. Open the stream, read `stream.Header()` for `flexiq-attach-session-bin`.
2. Send `hello`. Never anything before it.
3. Read `hello_ack` under the handshake budget.
4. Compare `protocol_version`; log both and refuse permanently on a mismatch.
5. Intersect capabilities.
6. Start the writer goroutine (sole owner of `SendMsg`), the reader loop, and
   the heartbeat.
7. On any end, drain and report *why* — the cause is what `Run` branches on.

The writer is fed by a channel carrying already-built frames. `send(jobID, …)`
stamps the lease. Progress coalesces latest-per-job; logs drop oldest.

An `AttachResponse` whose `Frame` is nil is an unknown arm: log once per stream,
continue.

Verify: the bufconn suite from Task 8, incrementally.

### Task 7 — `worker.go` and `backoff.go`

`Run` is the reconnect loop around `session`. A clean end reconnects
immediately and does not consume backoff; a transport error backs off; a
`shutdown` frame and the two permanent statuses return.

`Handle` refuses an empty name and a duplicate, and refuses to register after
`Run` has started — `hello.tasks` is fixed for the life of a stream, and a
handler added later would be advertised to nobody.

Verify: `go test -race ./...`.

### Task 8 — The bufconn suite

`sdks/go/tests/executor_*_test.go`, separate package, exported API only. A fake
`ExecutorService` that can be scripted per test. Cases are the list in the
design's Testing section; each one is a named test whose name is the claim.

Verify: `make race`.

### Task 9 — E2E behind `//go:build integration`

Reuse `e2e_harness_test.go`, which already owns a real `flexiq-server` and
reads its port back out of its log. A worker attaches, the producer client
enqueues, the job runs, the producer client reads the result back.

The harness strips `RUST_LOG` and every `FLEXIQ_` var from the inherited env,
and points `FLEXIQ_QUEUES` at a queue the tests do not use — an executor test
needs the queue it *does* use to be drained by its own worker, not by the
server's scheduler.

Verify: `make server && make e2e`.

### Task 10 — Docs

- `sdks/go/README.md` gains an executor section.
- `sdks/go/doc.go` says "nothing in this module opens it" about the executor
  door. That is now false.
- `docs/content/docs/server/custom-executors.mdx` gains a Go callout mirroring
  the one `clients.mdx` already carries for the producer client.

Verify: `pnpm --dir docs build` only if an MDX file changed structurally;
otherwise read the diff.

### Task 11 — Follow-up issues

Three, filed not built:

1. Durable steps in the Go executor client.
2. `crates/flexiq/src/outcome.rs` writes `"traceback": null` where
   `BINDING_CONTRACT.md` requires `[]`.
3. `sdks/go/taskerror.go` rejects a null traceback, so a Rust-SDK failure reads
   as unstructured.

## Commit split

One self-contained change each, subject ≤60 chars, imperative, no `@`, no AI
attribution:

1. `chore: generate the executor door's Go stubs`
2. `feat: encode a result and a task error in the Go client`
3. `feat: a Go executor client over the dispatch door` (the package)
4. `test: drive the Go executor against a scripted door`
5. `test: run a Go worker against a real server`
6. `docs: a Go executor beside the Go producer client`

Task 1 must be its own commit: it touches `buf.gen.yaml` and generated files,
and nothing else compiles against them yet.

## Verification before the PR

```bash
cd sdks/go
make check          # build vet lint race
make server && make e2e
cd ../.. && node scripts/version.mjs --check
```

Read every output. A green `make check` with a skipped suite is not green.
