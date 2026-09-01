# #713 — `FLEXIQ_GRPC_LISTEN` as a fourth server role

## Problem
`flexiq-server` plays three roles in one process — attach, dashboard, admission
webhook — each with its own environment variable and its own listener. Epic
#710 puts a `flexiq.v1` producer service on the network, #711 settled the wire
contract and #712 landed the buf gate and `contracts/descriptor.binpb`. Nothing
yet binds a gRPC port.

## Deliverable
The fourth role and nothing more: the variable, the config, the listener, the
graceful-shutdown path, `grpc.health.v1` and server reflection. Reviewed
against §11 of `tasks/specs/2026-09-01-flexiq-v1-proto-design.md`, which fails
this issue if the role starts without a namespace, reimplements listener
parsing, is not gated by a cargo feature, or skips the "at least one role" and
shutdown paths.

## Scope calls
- **No services.** `ProducerService` is #714 and `ExecutorService` is #720. What
  this listener serves is health and reflection, so it needs no prost codegen
  and no `buf.gen.yaml` — the committed descriptor is embedded as bytes.
- **Health is answered out of storage**, the same question `/readiness` answers.
  That is what uses the shared `StorageBackend` the issue asks the role to take,
  and it is what lets a gRPC-only pod carry a real readiness probe.
- **`grpc` is compiled into the shipped image.** Otherwise the chart would set a
  variable the binary rejects.

## Plan
- [x] `grpc` cargo feature: `tonic`, `tonic-health`, `tonic-reflection`,
      `tokio-stream`, all optional at 0.14/0.1. `tonic-prost` arrives under them.
- [x] `AttachListen` → `ListenAddress`, and `parse`/`resolve` take the variable
      name — two roles share the parser, so its errors must name the right one.
- [x] `config/grpc.rs`: `GrpcConfig { listen, namespace }`. Refuses an unset
      namespace (D11/§5.2), the two unhonoured TLS variables, and the variable
      itself on a build without the feature.
- [x] `config/mod.rs`: gRPC joins the "at least one role" and "DSN required"
      checks.
- [x] `src/grpc/`: `listener.rs` (bind/serve, reusing the attach role's hardened
      `bind_unix`), `health.rs` (storage watcher), `reflection.rs` (v1 +
      v1alpha over `contracts/descriptor.binpb`), `limits.rs` (D22, checked at
      compile time).
- [x] `runtime::run`: the `match (dashboard, webhook)` tuple does not survive a
      third server — replaced with a `JoinSet`, where the first role to stop
      triggers shutdown so the rest drain instead of being dropped.
- [x] `--help`, both READMEs, and the deployment guide.
- [x] Chart: `grpc.{enabled,port,service}`, its own Service with
      `appProtocol: grpc`, validation mirroring the server's refusals, and a
      probe branch — TCP for liveness, `grpc.health.v1` for readiness, because
      health reports storage and must not restart a pod during an outage.
- [x] Image builds `--features postgres,redis,grpc`; `EXPOSE 50051`.
- [x] CI: the role's own test run, the chart's gRPC cases, and
      `contracts/descriptor.binpb` in the server path filter — the binary
      embeds it now.

## Review
Verified end to end against the built binary, not only in tests:

- A process with only `FLEXIQ_GRPC_LISTEN` starts and serves —
  `grpcurl -plaintext localhost:50151 list` returns `grpc.health.v1.Health` and
  `grpc.reflection.v1.ServerReflection`, `Health/Check` returns `SERVING`, and
  `describe flexiq.v1.JobStatus` resolves out of the embedded descriptor with no
  `.proto` on hand.
- All four roles in one process bind their own listeners and exit `0` on
  `SIGTERM`.
- The two refusals fire with the reason: no namespace, and the variable on a
  binary built without the feature.
- `cargo fmt --check`, `cargo clippy --all-targets --all-features -D warnings`,
  `cargo test -p flexiq-server` and `... --features grpc` all clean; the chart
  renders every combination and refuses a gRPC door with no namespace.

What #714 inherits: a bound listener, the message caps already declared, and a
health service to flip when the producer service is registered.
