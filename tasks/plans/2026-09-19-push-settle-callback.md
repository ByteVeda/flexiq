# A settle callback for push dispatch — implementation plan

Issue: [#845](https://github.com/ByteVeda/flexiq/issues/845)
Design: `tasks/specs/2026-09-19-push-settle-callback-design.md`
Branch: `feat/push-settle-callback`, off `origin/master` at `0944b3ac`.
Commits authored as **kartikeya-27**. **Not pushed to the remote.**

## Global constraints

- **One cargo job.** `cargo check/test -j1`, `CARGO_BUILD_JOBS=1 cargo clippy`,
  never two invocations at once — 13 GB RAM.
- **Every commit compiles on its own.** Pre-commit stashes unstaged *tracked*
  files, so a split that leaves a caller behind fails the hook rather than CI.
  Untracked files are **not** stashed — move a new test file aside if it would
  be linted against a stashed source.
- **Three feature combos.** `cargo check --workspace`, `--features postgres`,
  `--features redis`. The settle marker touches all three backends; a default
  build proves nothing about Redis.
- **`http-target` and `grpc` are both cfg-gated**, and this feature needs both.
  Every new item is gated to match, and the `settle_only` door must not make
  `flexiq-server` stop compiling with `--no-default-features`.
- **Proto edits restale `sdks/go/internal/pb`**, which CI diffs — comments
  included. `scripts/proto-check.sh --fix` after any `.proto` touch.
- **Error strings are an interface.** Grep `match=`/`toThrow`/`contains` before
  rewording one; `ACCEPTED_NOT_SETTLED` is pinned by an existing test.
- No `Co-Authored-By`, no AI attribution, no `@` in a commit subject.

## Build order

### Task 1 — the wire

`contracts/proto/flexiq/executor/v1/executor_service.proto`: the four RPCs and
their six messages (§4.1). `scripts/proto-check.sh --fix`, regenerate
`contracts/descriptor.binpb` and `sdks/go/internal/pb`.

`contracts/PUSH_DISPATCH_CONTRACT.md`: the `202` row (§4.3), the credential and
gRPC-role dependency, and the progress/task-log bullet leaves "does not
promise".

Nothing consumes the RPCs yet — this commit is the shape, reviewed on its own.

### Task 2 — storage: the marker

Migration `crates/flexiq-core/migrations/m0019_claim_settle_deadline.rs`, and
its line in `migrations/mod.rs`.

`storage/records.rs`: `SettleGrant`, `SettleClaimant`, `StaleJob`. Root
re-exports in `lib.rs` — #921 put all 25 `storage::records` types on the root
and a test diffs the block against `records.rs`, so three new types mean three
new lines or a red test.

`storage/traits.rs`: `await_settle`, `claim_settle`, `supports_settle`, all
defaulted to their refusals (§3.2). `lease.rs`: `lease_authorizes` beside
`epochs_agree`, each doc naming the other.

### Task 3 — storage: the three backends

`diesel_common/locks.rs` for SQLite and Postgres; `redis_backend/locks.rs` for
the separate `flexiq:claim_settle:{job_id}` key (§3.1) — **not** a fourth field
on the claim string. `storage/mod.rs`'s `delegate!` block. `supports_settle`
returns `true` on all three.

Redis's consume is one script so the test-and-delete cannot be split.

### Task 4 — the reaper and the purge

`reap_stale_jobs` → `Vec<StaleJob>` across the trait, both backends and the
`delegate!`; the second indexed read that filters and flags (§3.3).
`purge_execution_claims` stops taking a claim with a live marker (§3.4).
`scheduler/maintenance.rs` consumes the marker as
`SettleClaimant::Expired` and writes the `push.accepted_not_settled` message.

### Task 5 — the dispatcher accepts

`worker/http_target/`: `Accepted` registry, `run_one`'s long await (§1.1), the
202 arm in `contract.rs` behind the opt-in, `set_claim_owner` un-emptied with
its comment rewritten (§5.5), `set_side_channel`, and
`HttpDispatchTarget::settle` (§5.3). Every emission gated on the consume.

### Task 6 — the door

`grpc/executor/service.rs`: `ExecutorDoor::settle_only`, the four handlers,
`Attach`/`Heartbeat` refusing without a dispatcher (§5.1). `grpc/executor/frames.rs`
gains the decode half for the three outcome frames as RPC bodies — reusing the
existing converters, not a second copy. Status mapping per §2.2.

`runtime/listener.rs` wires the door under `DispatchPath::Push`.

### Task 7 — config and chart

`config/push.rs`: `FLEXIQ_PUSH_TARGET_SETTLE`, the `grpc`-requires-a-listener
check and the `supports_settle` check, both refusing at boot by name.
`deploy/helm/flexiq-server/`: `push.settle`, the render, and the
`settle: grpc` + `grpc.enabled: false` guard.

### Task 8 — the side channel and progress

`worker/side_channel.rs` unchanged in shape — `HttpDispatchTarget` gets one.
`ReportProgress`/`WriteTaskLog` handlers resolve against the accepted registry
(§5.4). `flexiq_push_awaiting_settle` in `metrics.rs`.

### Task 9 — tests

The eight e2e cases and the three wire cases from §8, plus the unit tests and
**both mutation checks**. Written last only because they need the whole path;
each earlier task still lands with its own unit coverage.

### Task 10 — docs

`docs/content/docs/server/operate/push.mdx` — "A job has to finish inside one
HTTP request" is now wrong and is rewritten, not patched.
`docs/content/docs/server/custom-executors.mdx` — the per-topology `cancel()`
table's Push row, and the settle door beside the attach door.
`docs/content/docs/server/operate/grpc.mdx` — the settle-only role.

## Commit split

1. `feat: a settle callback on the executor door`  *(proto + contract)*
2. `feat: record a dispatch awaiting an out-of-band settle`  *(migration + trait)*
3. `feat: the settle marker on all three backends`
4. `fix: keep the epoch fence for a job awaiting a settle`  *(reaper + purge)*
5. `feat: accept a 202 and wait for the settle`  *(dispatcher)*
6. `feat: serve the settle RPCs under push`  *(door)*
7. `feat: opt into push settle callbacks`  *(config + chart)*
8. `feat: progress and task logs from a push target`
9. `test: the push settle callback end to end`
10. `docs: push work that outlives the request deadline`

## Verification before the review

```bash
cargo check --workspace -j1
cargo check --workspace --features postgres -j1
cargo check --workspace --features redis -j1
cargo test --workspace -j1
CARGO_BUILD_JOBS=1 cargo clippy --workspace --all-targets --all-features -- -D warnings
scripts/proto-check.sh
pnpm --dir docs typecheck && pnpm --dir docs lint
```

Plus a grep sweep for `#[allow(dead_code)]` comments naming a commit that has
landed — a stale one was a finding nine times on #843.
