# #909 — run the Go client against a real flexiq-server

Branch `test/go-client-real-server` off `master` at `f349fae1`. One build-tagged suite in
`sdks/go/tests`, one reusable workflow, and the dispatcher wiring that triggers it.

## The gap

Every Go test today answers a `ProducerService` double on a bufconn. That pins this client's half
of the contract. Nothing pins the *pair*: that a job this client enqueues is a job the Rust server
stores, that the payload it writes is the payload that comes back, and that an error it branches on
is an error the server actually raises.

## Decisions

- **The harness owns the server.** The issue's option 1 (dial `$FLEXIQ_GRPC_ADDR`) runs nowhere by
  default; option 2 (CI starts one) runs nowhere locally. Giving the harness a binary path and
  letting it mint, start, bind and tear down gets both: one `make e2e` locally, one step in CI.
- **`//go:build integration`.** The tag is the gate, so `go test ./...` stays a double-only suite
  needing no Rust. Missing binary is a failure, never a skip — a suite that skips itself in CI is a
  suite that stopped running and said nothing.
- **`127.0.0.1:0`, address read from the server's own log.** The listener already reports what it
  bound rather than what it was asked for. Picking a free port in Go and passing it would race
  another process onto it between the probe and the bind.
- **A separate `ci-go-e2e.yml`, not a job inside `ci-go.yml`.** The issue names the cost: this puts
  a Rust build in a suite that today needs no Rust toolchain. A workflow of its own keeps
  `ci-go.yml` exactly as it is and gives the pair test its own trigger set — `sdks/go/**`,
  `contracts/proto/**` and `crates/flexiq-server/**`, the two halves it is about.
- **Not `*shared`.** An engine-crate change that breaks the door already turns `ci-rust.yml` red
  (`cargo test -p flexiq-server --features grpc`). Same reasoning the `server` filter states.

## Plan

- [x] `sdks/go/tests/e2e_harness_test.go` — locate the binary, mint tokens through
      `flexiq-server token create`, start the server, read the bound address, wait on
      `grpc.health.v1.Health/Check`, tear down, dump the server log on failure
- [x] `sdks/go/tests/e2e_test.go` — the three the issue names, plus the pair facts that cost
      nothing once a server is up
- [x] `make e2e` target, in step with the workflow
- [x] `.github/workflows/ci-go-e2e.yml`
- [x] `.github/workflows/ci.yml` — `go_e2e` filter, job, `ci-status` needs
- [x] `sdks/go/README.md` — the Development section
- [x] Run it locally, green, before claiming any of the above

## Review

Eight tests, all green on the first run against a real server and stable over repeated `-race`
runs. The three the issue named — the payload round trip, a real `JOB_NOT_FOUND`, a real
`UNAUTHENTICATED` from a revoked token — plus five that cost nothing once a server is up and
each pin something a double can only assume: that `include_payload` is honoured rather than
merely sent, that a listing really does omit payloads, that a unique key really does dedupe onto
the original job, that cancel is idempotent, and that an `execute` token really is refused at
the producer door with the `scope` key the client's accessor reads.

Two things the work turned up on the way:

- **`make help` never listed a target with a digit in its name.** Its grep was `[a-zA-Z_-]+`, so
  `e2e` would have been invisible in the one place the Makefile exists to be discovered from.
  Widened to include digits.
- **`go vet` and `golangci-lint` both skip a build-tagged file by default**, which would have made
  the whole suite exempt from the checks every other file here passes. `build-tags: [integration]`
  in `.golangci.yml` and `-tags integration` on the pre-commit hook close that.

Not done, and deliberately: `QUEUE_FULL` against a real admission cap. The issue names its metadata
as an unproven pair, and it is — but provoking one needs a queue cap configured on the server, which
is a setting this door does not expose. It belongs with whatever adds that, not here.
