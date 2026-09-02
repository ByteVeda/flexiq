# #718 — an in-process JSON facade for the producer RPCs

Branch `feat/grpc-json-facade`. Reviewed against the #711 design doc: **D2, D15,
D22, §4.1, §6**. §11 fails the PR if a route exists for an `flexiq.executor.v1`
RPC, a `GET` serves an RPC that is not `NO_SIDE_EFFECTS`, the body cap and the
gRPC cap disagree, errors are rendered in a shape other than §4.1, or the drift
test checks a hand-written list instead of the package.

## Shape

One listener, two renderings of one service. The axum handlers call the
`ProducerService` trait methods on the same `Producer` value the gRPC codec
calls — no loopback hop, no second implementation of a handler — and the
`AuthLayer` that already wraps the whole router gates them, because it is keyed
on the request path and `/v1/` is classified beside the two proto packages.

| Method | Path | RPC | Idempotency |
|---|---|---|---|
| POST | `/v1/jobs` | `Enqueue` | unset |
| POST | `/v1/jobs:batchEnqueue` | `EnqueueBatch` | unset |
| GET | `/v1/jobs` | `ListJobs` | `NO_SIDE_EFFECTS` |
| GET | `/v1/jobs/{job_id}` | `GetJob` | `NO_SIDE_EFFECTS` |
| POST | `/v1/jobs/{job_id}:cancel` | `CancelJob` | `IDEMPOTENT` |
| GET | `/v1/queues/{queue}/stats` | `QueueStats` | `NO_SIDE_EFFECTS` |
| GET | `/v1/stats` | `QueueStats` | `NO_SIDE_EFFECTS` |

`QueueStatsRequest.queue` is `optional` and unset counts the whole namespace, so
one route cannot express every legal request; two routes for one RPC is what the
message shape asks for. `SubmitWorkflow` is #771 and has no RPC to route yet.

## Decisions taken

1. **Hand-written proto3 JSON, no new codegen.** `pbjson` needs
   `google.protobuf.*` to be `pbjson-types` and cannot serialise
   `tonic_types::pb::Status` at all, so adopting it means rewriting #714/#715's
   conversions *and* generating a second `google.rpc.Status` — the one thing
   `build.rs` says never to do. Requests are serde structs; responses are built
   as `serde_json::Value` so presence and int64-as-string are explicit and the
   drift test can read the emitted keys back.
2. **`chrono`, feature-gated.** Already a workspace dependency (`flexiq-core`
   uses it) so the lock does not move. `SecondsFormat::AutoSi` is exactly
   proto3 JSON's 0/3/6/9 fractional-digit rule; a hand-rolled civil-date
   conversion would be thirty lines of leap-year arithmetic for the same answer.
3. **`POST /v1/jobs/{job_id}:cancel` is registered as `POST /v1/jobs/{job_id}`
   and the verb is split off the captured segment.** matchit 0.8, which axum
   routes with, states outright that "dynamic suffixes are not currently
   supported". The public path in `ROUTES` stays the colon form — it is what the
   client types and what the drift test reads.
4. **One new reason, `NO_SUCH_METHOD`, and a §4.1 amendment.** An unrouted path
   must still answer in the §4.1 shape, and reusing `INVALID_REQUEST` would
   conflate "your debounce block has no window" with "that URL names no RPC" on
   the one field a client is told to branch on. Precedent: #714 added
   `INVALID_REQUEST` the same way.
5. **The HTTP status is a pure function of the `google.rpc.Code`.** A body over
   the cap is `OUT_OF_RANGE` — the code tonic answers for the same mistake — and
   therefore 400 rather than 413. One mapping, no per-case exceptions, is what
   keeps the two renderings of one error model from drifting.

## Steps

- [ ] 1. `facade/json/` — proto3 JSON for the `flexiq.v1` messages (+ `chrono`).
- [ ] 2. `facade/error.rs` — the `google.rpc.Status` JSON body and the code →
      HTTP mapping; `NO_SUCH_METHOD`, `malformed_payload`, `payload_too_large`.
- [ ] 3. `facade/routes.rs` + `listener.rs` + `auth/` — the routes, the
      descriptor-driven drift test, `accept_http1`, and refusals rendered for the
      door the request arrived at.
- [ ] 4. `tests/grpc_facade.rs` — end to end over a real socket.
- [ ] 5. Docs: the design-doc amendment, `deployment.mdx`, the crate README.

## Review

All five steps done. What building it changed from the plan:

1. **`google.protobuf.Duration` is not canonical inside this process.** `convert::duration`
   uses Euclidean division, so −1500 ms arrives as `{seconds: −2, nanos: +500_000_000}` —
   the two halves disagree in sign, which a `Duration` may not do. Formatting that pair
   where it sits moves the value by a second, so `duration_to_json` sums the halves into
   one `i128` before splitting them again. Correct for both spellings, and the round-trip
   test asserts it in **milliseconds**, which is what storage actually holds.
2. **`Authenticated<S>`'s response body had to stop being generic.** A refusal has to be
   *rendered* — trailers for gRPC, a body and an HTTP status for JSON — and a `ResBody:
   Default` bound can only make an empty one. It is `tonic::body::Body` now, which is what
   `Routes` returns anyway, and `facade::refusal` picks the rendering off the content type.
3. **matchit refuses `{job_id}:cancel`.** "Dynamic suffixes are not currently supported",
   in its own README. `Binding::pattern()` is what axum registers and `Binding::path()` is
   what a client types; they differ for exactly one route, and a test says so.
4. **`GET /v1/stats` is a second route for one RPC.** `QueueStatsRequest.queue` is
   `optional` and unset counts the namespace, which no path with a queue in it can
   express — which is why the table is keyed on bindings rather than on RPCs.
5. **The drift test got a sibling.** Beyond "every RPC has a route", `json/response.rs`
   asserts that a fully populated message emits *exactly* the JSON names the descriptor
   gives it. Hand-built response objects have no compiler behind them; this is what
   replaces one. D4 freezes the names for this door's sake, so the check belongs here.

## Verification

Run, all green:

- `cargo clippy --all-targets --all-features -j2 -- -D warnings` — 0
- `cargo test -p flexiq-server --features grpc -j2` — 358 lib + 15 facade + 129 other
- `cargo clippy --workspace --all-targets -j2` (default features: none of this compiles)
- `pnpm --dir docs {lint,typecheck,check:parity,check:search,check:diagrams}`,
  `node scripts/version.mjs --check`
