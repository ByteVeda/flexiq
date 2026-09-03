# #720 — the ExecutorService as a fourth Transport

Branch `feat/grpc-executor-transport`, off `master` at `cfb47e98`. Plan:
`tasks/plans/2026-09-03-grpc-executor-transport.md`. **Not pushed.**

## Done

- [x] `worker/transport.rs` — `Connection::new` (public, so a `Transport` can
      live outside the core at all), `Transport::is_authenticated`, and the
      channel generalized to a bounded direction with a write timeout.
- [x] `worker/frame_transport.rs` — `FrameTransport` / `FrameEndpoint`: the
      fourth `Transport`, byte-oriented for `RemoteDispatcher` and
      frame-oriented for a door that speaks messages.
- [x] `worker/remote.rs` — a vouched transport skips the frame credential;
      `RemoteDispatcher::detach` drains one executor to `free == slots`, not to
      an empty in-flight map, and `try_acquire` skips a draining peer while
      still counting it as advertising.
- [x] `lease.rs` — `Lease::as_bytes` / `from_wire`, for a transport that carries
      the token as `bytes`.
- [x] `contracts/proto/flexiq/executor/v1/executor_service.proto` + the
      regenerated descriptor. `AttachRequest`/`AttachResponse`, not
      `ExecutorFrame`/`SchedulerFrame` — `buf lint` STANDARD refuses those.
- [x] `grpc/executor/{frames,session,service}.rs` — the conversions, the session
      registry the unary heartbeat routes through, and the door itself.
- [x] `grpc/listener.rs` registers it at `EXECUTOR_MAX_MESSAGE_BYTES`, and stops
      waiting on open connections after its own grace period.
- [x] `runtime/mod.rs` — the dispatcher exists when *either* door is configured,
      and executors are drained while the listeners wind down rather than after.
- [x] `config/grpc.rs` — `FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE`, `--help`, chart.
- [x] `tests/grpc_attach_e2e.rs` — `attach_e2e.rs`'s scenarios over gRPC, plus
      the killed stream, the rotated stream, the 64 MiB payload, the unknown
      frame, the heartbeat and the mixed socket/gRPC pair.

## Review

Two bugs the tests found, both real and both in the shutdown path:

1. **The listener could not stop while an executor was attached.** An attach
   stream is an in-flight gRPC request, a graceful listener waits for one, and
   the stream only ended when the dispatcher closed it — which happened *after*
   the roles were joined. Fixed by draining the scheduler concurrently with the
   wind-down, and bounded by a grace period so an impolite client cannot hold
   the process open either.
2. **The lifecycle task held a second sender on the response stream.** It was
   there to deliver a refusal, and keeping it past the handshake meant a
   finished stream stayed open until the client hung up.

Both were invisible to the unit tests and to `cargo check`. What found them was
running the acceptance scenarios end to end.
