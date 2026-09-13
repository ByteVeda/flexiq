# Durable steps in the Go executor client — design

Issue: [#929](https://github.com/ByteVeda/flexiq/issues/929)
Branch: `feat/go-executor-steps`
Follows: [#908](https://github.com/ByteVeda/flexiq/issues/908) / PR #928, which
shipped `sdks/go/executor` with `lease` and `side_channel`.

## Why this exists

PR #928 deliberately left `steps` out. It is the one capability that **fails
rather than degrades** — a durable step that silently did not commit is a step
that will re-run a charge — so half-present was worse than absent. This is the
other half.

## Scope

The `steps` capability end to end: the snapshot a dispatch carries, the
commit/ack round trip, the two-phase sleep, and the verdict a refusal carries.
Plus the local rules none of that hands over — sequencing, key derivation,
divergence, caps.

Out: nothing. The issue's six bullets are all here.

## Shape

Go is different in kind from the other shells. Node, Java and Python hand the
step *rules* to `flexiq_core::step` across their FFI; their attach code is thin
because the real work is behind the binding. Go links no Rust and speaks gRPC,
so every rule is reimplemented and has to stay byte-compatible with the Rust
original. The wire shapes already existed — `JobStepsFrame`, `StepCommitFrame`
and `StepAckFrame` are generated, and `stampLease` already had a `StepCommit`
arm, written anticipating this.

Two layers:

- **`sdks/go/internal/step`** — the rules, and nothing else. Key derivation,
  the caps, the snapshot codec, the sequence walk, the run key. No gRPC, no
  connection, no clock it is not handed. Internal so the public surface stays
  small, but exported *within* the module so `sdks/go/tests` can drive it
  directly and so each file lints clean before its consumer exists.
- **`sdks/go/executor`** — the surface, the correlation and the settlement.

## Decisions

### Generic package functions, not methods

A Go method cannot have a type parameter, so a typed step result forces a free
function:

```go
func Step[T any](ctx context.Context, job *Job, name string,
    body func(ctx context.Context, key string) (T, error)) (T, error)
func StepKeyed[T any](ctx context.Context, job *Job, name, key string,
    body func(ctx context.Context, key string) (T, error)) (T, error)

func (j *Job) Sleep(ctx context.Context, name string, d time.Duration) error
func (j *Job) SleepUntil(ctx context.Context, name string, at time.Time) error
func (j *Job) RunKey() string
```

The alternative — a handle plus a decode-into-pointer `out any`, mirroring
`job.Bind(&v)` — was weighed and rejected: it costs a type, an argument and the
result's static type, and buys symmetry with a method that decodes a payload
this package did not produce.

`body` is handed the **downstream idempotency key**, `{run}:{step_key}`, rather
than being offered a separate accessor for it. Memoization closes the replay
window and not the crash window; the key is what closes the other, and a
signature that hands it over is one nobody has to be told to reach for.

Sleeps are unkeyed. Core allows a keyed sleep and the Rust SDK shell does not
expose one either; adding it later is two lines, and nothing asks for it now.

### Sentinel errors, not a class hierarchy

`*StepError` with `errors.Is` sentinels — `ErrStepRetryable`,
`ErrStepPermanent`, `ErrStepSuperseded`, `ErrStepUnavailable`,
`ErrStepSwallowed`, `ErrStepSlept`. The FFI shells use an exception tier a
`catch` cannot reach (`BaseException` in Python, `java.lang.Error` in Java);
Go has no such tier, and `errors.go` already committed this package to
`Fatal`/`ErrCancelled`.

`Unwrap() []error` reports several at once, which is what makes a **permanent**
step error also answer to `ErrFatal`: a body that simply returns one settles the
job the way the verdict says, with no translation at the call site and no change
to `settle`'s default branch.

The verdict comes **from the `StepFailure` enum, never the message**. An
unrecognised verdict reads as retryable — nothing confirmed the write landed,
and a scheduler that grows a fourth verdict must not turn into a dead letter
here.

### A latch, because Go cannot stop a body swallowing an error

`_, _ = executor.Step(...)` compiles. Node and Python both hit this and both
answer with a latch checked the instant the handler returns; this is the same
thing.

| Latch state | Frame |
|---|---|
| superseded | **none at all** — another attempt owns this job |
| slept | `slept`, with the deadline the **ack** settled on |
| a refusal the body returned past | `failure`, `should_retry` from the verdict |

Superseded outranks everything, including a sleep: an attempt that lost its
fence writes nothing. A swallowed sleep still ends the attempt, because the
claim is gone either way.

### Where the wait is bounded

`(job_id, seq)` waiter registry on the session, modelled on
`sdks/python/flexiq/prefork/steps.py` — the one existing implementation with no
core to lean on. **The waiter is registered before the commit is sent**, or a
fast scheduler answers before anyone is listening.

Three bounds, whichever comes first: the caller's context, the **job's** context
— which carries the attempt deadline, so a wait never outlives a reap the
scheduler has already decided on — and `WithStepAckTimeout` (30s, the reference
executor's own number). A stream ending closes every waiter at once rather than
leaving each to time out alone, and nils the registry so a commit racing the
teardown is refused rather than booked against a reader that has gone.

### Local caps, and what they are for

`WithStepLimits` refuses an over-cap commit before the round trip. The check
that *holds* is the scheduler's, inside the write's own transaction; this one
buys an error that names the step and the number that failed. Zero fields take
the defaults and anything above the hard ceiling is brought back to it — a cap a
caller can zero by leaving the struct empty is worse than one they can raise.

### The snapshot is decoded on arrival and raised at the first step

`job_steps` arrives immediately before its `job` frame, so the dispatch can
answer a memo hit without a storage read this side has no credentials for. It is
decoded when it lands — a snapshot that will not parse is a fact about the
dispatch — but the error is *kept* and raised at the first step call, because a
job that uses no steps has no reason to fail over it. That is core's own shape
(`Shared::remember_snapshot` / `snapshot_for`).

The refusal is **retryable**, not permanent: a damaged dispatch says nothing
about whether the code and the recorded sequence agree, and the next one may
arrive whole. A hole in the recorded `seq` is the opposite — the rows on disk
will not heal — and is permanent.

## The facts most likely to be got wrong

- **`seq` for new ground is the number of rows already stored, not the cursor.**
  A keyed hit can claim a row out of order and leave the positional walk behind.
  Its own test, and its own mutation check.
- **A refused step does not spend its occurrence.** The counter advances only
  once the step is known to be usable, so a retry derives `charge#0` again
  rather than `charge#1`.
- **Only a fresh sleep advances the sequence.** A resume re-issues at the
  *recorded* position and writes nothing; counting it would put the sequence one
  ahead of storage.
- **The ack echoes the settled deadline, never the candidate.** On a replay they
  are different numbers and the job was rescheduled to the stored one.
- **An elapsed sleep sends no frame at all.** That is what makes a job with
  three sleeps not restart the first one on the third wake.
- **`already: true` is a success.** It is a retransmission after a lost ack.

## Testing

`sdks/go/tests`, the external package the rest of the suite uses.

- The rules directly: key derivation, the caps, the snapshot codec's four
  refusals, and the sequence's memo/divergence/occurrence behaviour.
- The round trip over bufconn: eighteen cases, from a memo hit that sends no
  commit to a superseded attempt that sends no frame at all. Five of them were
  mutation-checked against the prior behaviour before they counted.
- E2E behind `//go:build integration`, against a real `flexiq-server`: a step
  that survives the attempt that wrote it — the scheduler encodes the snapshot
  in Rust and this client decodes it in Go, and a memo that did not survive that
  round trip is a body that runs twice — and a job with two sleeps that does not
  restart the first on the second wake.
