# #720 — the ExecutorService as a fourth Transport

Part of #710, stage two. Depends on #719 (merged, `cfb47e98`), whose lease is on
this package's wire from its first release rather than retrofitted into it.

Governed by `tasks/specs/2026-09-01-flexiq-v1-proto-design.md` §8, D1, D22, D24.
§11's row for #720 lists the seven ways this fails; each is answered below.

## The shape

`RemoteDispatcher::attach(Box<dyn Transport>)` already owns the handshake, the
reader thread, placement, the lease fence, the side channel, registry divergence
and the drain. A gRPC executor must reach *that*, unchanged, or it becomes the
second dispatcher §11 names as the failure.

`Transport` is byte-oriented — `split()` hands back `BufRead`/`Write` halves and
the frame codec sits on top. A gRPC stream carries decoded protobuf messages, so
the fourth transport is the one that turns frames back into the bytes the codec
expects.

```
tonic Streaming<AttachRequest>  ──▶ ExecutorMessage ──▶ FrameEndpoint::send
                                                             │  (encodes)
                                                             ▼
                                        FrameTransport (impl Transport) ──▶ attach()
                                                             ▲
                                                             │  (decodes)
tonic mpsc<AttachResponse>      ◀── SchedulerMessage ◀── FrameEndpoint::recv
```

**Two crates, and the seam is `flexiq_core`'s message types, not tonic's.**
`FrameTransport` lives in core and knows nothing about gRPC; the proto
conversions and the tonic pumps live in `flexiq-server` behind the `grpc`
feature, where D23 puts generated code. The cost is one encode and one decode
per frame beyond what a socket pays — accepted, because the alternative is a
frame-level `Transport` trait that rewrites `remote.rs`, `executor.rs` and the
prefork pool for a memcpy.

**A fourth `Transport` cannot be written outside core today.** `Connection`'s
fields are private and it has no constructor, so `split()` is unimplementable by
any other crate. That is why `FrameTransport` is core's and not the server's.

## Names: an amendment to §8

§8 sketches `ExecutorFrame` and `SchedulerFrame`. `buf lint` at `STANDARD`
refuses them — verified, not predicted:

```
RPC request type "ExecutorFrame" should be named "AttachRequest"
RPC response type "SchedulerFrame" should be named "AttachResponse"
```

`RPC_REQUEST_STANDARD_NAME` has no streaming exemption. So the two messages are
`AttachRequest` and `AttachResponse`, each still exactly one `oneof` over the
frame set — §8's substance, §1.2's spelling. Same correction §1.2 already made
to `service Executor`.

## The contract

`contracts/proto/flexiq/executor/v1/executor_service.proto`, package
`flexiq.executor.v1`, importing nothing from `flexiq.v1`: the dispatch frame and
the producer's `Job` are different shapes and share no field. The reverse import
stays forbidden either way (D1).

```proto
service ExecutorService {
  rpc Attach(stream AttachRequest) returns (stream AttachResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
}
```

`AttachRequest` arms: `hello`, `progress`, `task_log`, `success`, `failure`,
`cancelled`, `step_commit`, `slept`.
`AttachResponse` arms: `hello_ack`, `job`, `job_steps`, `step_ack`, `cancel`,
`shutdown`.

One message per frame, mirroring the `ExecutorMessage` / `SchedulerMessage`
variants field for field, with four rules:

- **`payload_len` becomes `bytes`.** A frame declares a length because its
  payload follows it on a byte stream; a protobuf field carries its own.
- **Explicit presence where absence differs from zero** (D19): `optional bytes
  result` (`None` = returned nothing, `Some(b"")` = returned an empty value),
  `optional bytes extra`, `optional string namespace`, `optional string
  metadata`, `optional int64 wake_at`, `optional bytes lease`.
- **`capabilities` stays `repeated string`** (D24). `StepKind` and `StepFailure`
  become enums with `_UNSPECIFIED = 0`; an `UNSPECIFIED` `StepKind` is refused at
  conversion, because the Rust enum has no such value.
- **`hello` carries no `token` and no fingerprint.** The bearer credential is the
  gRPC one, checked by `AuthLayer` before the RPC is entered; the fingerprint is
  derived from `tasks[]` as #703 established. Neither is a wire field here.

The lease is `optional bytes` on every executor→scheduler frame that settles or
advances an attempt — `success`, `failure`, `cancelled`, `slept`, `progress`,
`task_log`, `step_commit` — and on the `job` frame. `hello` and `heartbeat`
carry none: they are the connection's, not any job's.

`contracts/descriptor.binpb` is regenerated with `scripts/proto-check.sh --fix`
under the pinned buf 1.72.0 (present locally, verified).

## Heartbeat is a delivery, not a new frame

§8: a unary RPC, not a frame on the dispatch stream — Hatchet's `ListenV2`
correction taken for free.

`ExecutorMessage::Heartbeat { free_slots }` already exists and already updates
`last_seen_ms` and shrinks free capacity in `handle_frame`. So `Heartbeat` does
not add a code path: the RPC **injects that frame into the attached session's
inbound endpoint**, and it reaches `handle_frame` exactly as it would have on the
stream. The frames stay the frames; only the delivery moved.

Routing needs the connection's identity. `HeartbeatRequest.session` is an opaque
value **minted by the scheduler** at attach and returned in the `Attach`
response's initial metadata (`flexiq-attach-session`) — the same principle as the
lease: a value a peer can choose is a request to be trusted, not an identifier.
`executor_id` is deliberately *not* it; it is a name the executor picks, so one
authenticated peer could shrink another's capacity by claiming it.

Verified first, before the service is written: if tonic withholds a streaming
response's initial metadata until the first message, the session moves to a
client-minted secret in *request* metadata and the reason is written down.

## Bounded stream lifetime, and the race to close

A stream that never ends cannot be load balanced and pins an executor to one
scheduler replica. So it ends on a timer, and the executor reconnects.

The requirement — "a reconnect must not drop a job in flight" — is what
`drain_and_close` does for the whole dispatcher and nothing does for one
executor. Closing the socket and letting the reaper notice is exactly what the
issue refuses.

**New in core: `RemoteDispatcher::detach(executor_id, drain) -> bool`.** Temporal
ships this as `ShutdownWorker`, and for the same reason.

1. Remove the executor from the registry **under the `executors` lock**, so
   `try_acquire` cannot pick it from that instant. Nothing new is matched to a
   departing stream.
2. Wait until `free == slots`, not until `in_flight` is empty. This is the race
   the issue names: `place()` reserves a slot, then `await`s two storage reads,
   and only then writes the job frame. Between those it is matched but not
   delivered and has *no* `in_flight` entry. `free == slots` covers both states;
   `is_busy()` covers only one.
3. Close the connection. The stream ends `Ok`, and the executor reconnects.

Rotation reuses it: one timer per `Attach` stream, `FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE`
seconds (default 1800, `0` disables), ±10% jitter so a fleet does not rotate in
lockstep. The drain budget is the rotation period itself — one knob, and a job
outliving a whole period is pathological rather than a case to configure for. On
expiry it force-closes and names the jobs left to the reaper, exactly as
`drain_and_close` already does.

**A clean stream end means reconnect; a `shutdown` frame means stop.** The
scheduler going away already has a frame for it, so rotation needs no new one.

The ungraceful path is unchanged and deliberately so: an executor that drops
mid-job gets no synthesized result, and recovery stays with the dead-owner
reaper ([[executor-attach]]'s stated non-goal). The lease is what makes that
retry exactly one execution of one attempt.

## Authentication

`gate.rs` already classifies `/flexiq.executor.v1.` as `Scope::Execute`, and
`limits.rs` already declares `EXECUTOR_MAX_MESSAGE_BYTES` at 68 MiB — both
pre-declared for this PR, both currently unused.

But `RemoteConfig.auth_token` is set from `FLEXIQ_ATTACH_TOKEN`, and a
gRPC-attached executor's `hello` carries no token, so `Shared::attach` would
refuse every one of them. Injecting the configured secret into the frame on the
executor's behalf would be forging a credential.

Instead: **`Transport::is_authenticated() -> bool`, defaulting to `false`.** It
is a property of the transport — "this connection established its peer's
authority before a frame was read" — and `Shared::attach` skips the frame
credential when it holds. Every byte transport keeps the default; a socket
carries no credential of its own. `FrameTransport` takes it as a constructor
argument, and only the gRPC door passes `true`, having been through `AuthLayer`.

## Build order

Each commit compiles and tests on its own: pre-commit stashes unstaged tracked
files, so a commit that needs the next one's hunks fails the hook
([[feedback_precommit_drops_untracked]]).

1. **core — the transport.** `Connection::new`, `Transport::is_authenticated`,
   `FrameTransport`/`FrameEndpoint` over a pipe generalized out of
   `MemoryTransport`'s `Channel` (bounded with a write timeout in the dispatch
   direction, so a stalled peer fails a write rather than growing a buffer —
   the trap #583 fixed for sockets), `Lease` wire-bytes accessors. Unit tests:
   a full frame set round-trips, a bounded write times out, `is_authenticated`
   skips the token check.
2. **core — `detach`.** Plus the regression that pins step 2 above: detach while
   a placement sits between its reservation and its write, and assert the job is
   delivered rather than abandoned.
3. **contract.** The `.proto`, `buf format`/`lint`, the regenerated descriptor.
4. **server — the door.** `pb.rs` gains the second `include!`; `grpc/executor/`
   holds the conversions, the service, and the session registry; `listener.rs`
   registers it at `EXECUTOR_MAX_MESSAGE_BYTES`; `runtime/mod.rs` builds the
   dispatcher and supervisor when *either* `FLEXIQ_LISTEN` or
   `FLEXIQ_GRPC_LISTEN` is set. Conversion tests match exhaustively, so a frame
   added later fails to compile rather than silently losing a field.
5. **server — rotation.** The timer, the jitter, the config knob.
6. **tests.** `crates/flexiq-server/tests/grpc_attach_e2e.rs`.

## Acceptance

The executor in the tests is **hand-rolled against the generated client**, as
`attach_e2e.rs` hand-rolls its socket executor and for the same reason: it proves
the wire contract alone is enough to attach.

- The seven `attach_e2e.rs` scenarios, over gRPC, unchanged in behaviour. The two
  token scenarios become their gRPC equivalents — no bearer, and a `produce`-only
  token — since the credential moved to the layer.
- Killing the stream mid-job retries that job **once, and once only**.
- A TCP executor and a gRPC executor on one scheduler both receive work, and the
  registry divergence warning fires across the pair. This is the test that proves
  one dispatcher, not two.
- A `MAX_PAYLOAD_BYTES` payload round-trips over gRPC, which is what the 68 MiB
  message cap exists for (D22).
- An `AttachRequest` with no arm set is skipped, not fatal (D24).
- A rotation mid-job delivers the result and the executor reattaches.
- A unary `Heartbeat` shrinks the executor's free slots without a stream frame.

## Not in this issue

#721 documents the door. No HTTP binding, ever (D2) — the JSON facade serves
`flexiq.v1` and nothing else. No SDK shell speaks gRPC yet.
