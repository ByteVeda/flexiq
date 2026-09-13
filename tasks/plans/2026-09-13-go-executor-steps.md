# Durable steps in the Go executor client — implementation plan

Issue: [#929](https://github.com/ByteVeda/flexiq/issues/929)
Design: `tasks/specs/2026-09-13-go-executor-steps-design.md`
Branch: `feat/go-executor-steps`, off `origin/master` at `b448ff74`.

## Global constraints

- Every commit lands `make check` clean — build, `go vet`, `golangci-lint run`,
  `go test -race`. The pure package's identifiers are exported *within* the
  module, so `unused` does not flag them before their consumer exists, which is
  what lets the rules land ahead of the wiring.
- No proto change. The step messages have been generated since #908.
- The Rust original is the oracle for every string the two sides compare:
  `step/key.rs`, `step/sequence.rs`, `worker/protocol.rs`, `step/idempotency.rs`.

## Build order

### Task 1 — `internal/step`: identity and caps

`doc.go`, `key.go`, `limits.go`. `Derive`/`Explicit`/`Abbreviate`, the 128/256
byte bounds, the ban on either separator inside a name, the three caps with
their defaults and hard ceilings.

### Task 2 — `internal/step`: the snapshot codec

`snapshot.go`. `Record`, `Kind` with an `UnmarshalJSON` that refuses a kind this
build does not know, `EncodeSnapshot` and a strict `DecodeSnapshot` — no
metadata line, unreadable metadata, a negative length, truncation and trailing
bytes are all errors.

### Task 3 — `internal/step`: the sequence walk

`sequence.go`, `idempotency.go`. `Sequence` with the gapless check at
construction, the per-name occurrence counter spent only once a step is usable,
keyed lookup by map against unkeyed by cursor, the three sleep states, the caps
check and the windowed divergence message.

### Task 4 — the executor wiring

`steps.go` (the public surface and the per-job state), `steperrors.go` (verdicts
and sentinels), `stepacks.go` (the `(job_id, seq)` registry, the snapshot stash
and the commit round trip), `steplimits.go` (the exported caps and their
option). Then: `attach.go` advertises `CapSteps`, splits the two inbound arms,
abandons waiters when the reader goes and takes the snapshot at dispatch;
`job.go` carries the state; `result.go` grows the superseded, slept and
swallowed branches; `options.go` grows two options; `doc.go` is rewritten.

One existing test flips with the behaviour: the executor now advertises steps.

### Task 5 — the bufconn suite

`tests/executor_steps_test.go`. A scriptable dispatch that answers commits per a
callback, and the eighteen cases in the design's testing section. Mutation-check
the superseded, swallowed, memo-hit, settled-deadline and elapsed-sleep
assertions against the prior behaviour.

### Task 6 — E2E

`tests/e2e_executor_test.go`, behind `//go:build integration`. A step that
survives the attempt that wrote it, and a job with two sleeps.

### Task 7 — Docs

`sdks/go/README.md` gains a durable-steps section and a third capability row;
`docs/content/docs/server/custom-executors.mdx` stops saying the Go client
leaves steps out.

## Commit split

1. `feat: step identity and caps for the Go client`
2. `feat: decode a durable-step snapshot`
3. `feat: walk a job's recorded step sequence`
4. `feat: durable steps in the Go executor client`
5. `test: the durable-step round trip over bufconn`
6. `test: durable steps against a real server`
7. `docs: durable steps in the Go executor client`

## Verification before the PR

- `cd sdks/go && make check`.
- `make server && make e2e` — a real `flexiq-server` on SQLite, which is what
  advertises the `steps` capability back.
- The five mutation checks above, each confirmed red before being confirmed
  green.
