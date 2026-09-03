# docs: server mode, and when the database is still the better door (#721)

Last sub-issue of epic #710. #712–#720 all merged, all code-side; this is the
reader-facing page plus the two things #720's merge left undocumented.

## Placement

New shared page: `docs/content/docs/shared/modules/server.mdx` — the `modules`
skeleton already had a slot comment naming this exact spot (between `executor`
and `autoscaling`). Nav: `"server"` added to
`docs/scripts/parity/section-skeleton.mjs` and all three SDKs'
`modules/meta.json`, plus a card on `shared/modules/index.mdx`'s Distribution
row.

## What already existed vs. net-new

`docs/content/docs/shared/operate/deployment.mdx`'s `## The gRPC door` section
already covered the producer RPCs, tokens/scopes, `raw`/`structured`, and the
JSON facade — written incrementally by #713–#718. Net-new:

- [x] `modules/server.mdx` — the conceptual page: local vs server mode side by
      side, what server mode costs (no CPU-parallelism story), the two doors as
      genuinely different things, `raw`/`structured` summarized and linked out.
- [x] `deployment.mdx` `### The executor door` — #720 (merged the same day as
      this branch, `0a25b8a0`) shipped `flexiq.executor.v1.ExecutorService`
      with zero docs anywhere. Verified no SDK CLI dials it
      (`crates/flexiq-core/src/worker/dial.rs`'s `AttachAddress::parse` only
      takes `tcp://`/`unix:`/bare `host:port`) — documented as a door for a
      custom executor in any language with a gRPC library, same pitch as the
      producer door.
- [x] The untrusted-network warning the issue asked to remove "not before"
      #717 — already gone; #717 deleted it when scoped tokens shipped
      (`grpc-scoped-tokens` memory). Nothing to do here.

## Polyglot variant — scope decision

User chose: producer over gRPC only, not a full network pipeline. Node/Java
workers keep opening the db file directly; only `producer.py` gets a network-
speaking sibling (`grpc_producer.sh`, `grpcurl` + `jq`, no SDK). Workers gain
optional `FLEXIQ_NAMESPACE` (backward-compatible — unset preserves today's
behavior) because a job enqueued through the gRPC door always carries its
token's namespace, and an unnamespaced worker would never see it.

Full network pipeline (workers as attached executors) filed as follow-ups,
not part of this branch:
- Node: https://github.com/ByteVeda/flexiq/issues/796
- Java: https://github.com/ByteVeda/flexiq/issues/797

## Accuracy notes (verified against source, not inferred)

- `EnqueueRequest`/`EnqueueResponse`/`StructuredArgs` field names from
  `contracts/proto/flexiq/v1/{producer_service,job}.proto` — `task_name`,
  `structured.args`, `options.queue`, response `job.id`.
- `EXECUTOR_MAX_MESSAGE_BYTES` = 68 MiB, `crates/flexiq-server/src/grpc/limits.rs`.
- `FLEXIQ_GRPC_EXECUTOR_STREAM_MAX_AGE` default 1800s (30 min),
  `crates/flexiq-server/src/config/grpc.rs`.
- `flexiq.executor.v1.ExecutorService` has two RPCs — `Attach` (bidi stream)
  and `Heartbeat` (unary, off-stream on purpose) —
  `contracts/proto/flexiq/executor/v1/executor_service.proto`.
- Node `Queue` namespace option: `sdks/node/src/queue.ts:144`. Java
  `FlexiQ.Builder.namespace(String)`: `sdks/java/.../FlexiQ.java:1563`.

## Verify

- `pnpm --dir docs typecheck` / `lint` / parity checks / `build`.
- End-to-end: `flexiq-server` (docker) against a temp SQLite file with the gRPC
  door + namespace set, mint a token, run `grpc_producer.sh`, drain with the
  existing Node worker (namespace set) against the same file.
