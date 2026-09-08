# An operate section for flexiq-server — `docs/server-operate-section`

Closes #826. Branch off `master` at `8bce1713`.

## What the issue asked for, against what already exists

The issue was filed on 2026-09-06 against a tree that had already moved. Verified
before planning:

- `shared/operate/` has **three** pages, not two — #853 added `kubernetes.mdx`,
  which already covers the chart, the four listener roles, maintenance ownership
  across replicas, sidecar injection, probes and the KEDA manifests.
- `shared/operate/deployment.mdx` already carries ~540 lines of gRPC door, token
  lifecycle, scopes, expiry, revocation, the JSON facade and the TLS refusal —
  landed by #721 and #803, both after the issue's premise was written.

So the gap is not that the content does not exist. It is that the server's
operator content is buried in a 2378-line page whose first half is `myapp.py`,
systemd units and SQLite file permissions, and that the crate README is still
the only place naming `FLEXIQ_WORKERS`, `FLEXIQ_AUTO_MIGRATE` or what
`FLEXIQ_MAINTENANCE=off` actually turns off. Two of the five bullets — scaling a
server deployment, and backup/restore per backend — have no page at all.

## Shape

A `Server` group under `Operate`, plus one top-level `backup` page. The group is
what #825 lifts into the fourth server tier later; `backup` stays where it is,
because an embedded SQLite reader needs it as much as a server operator does.

- [ ] `operate/server/index` — the four roles and their four listeners, the full
      environment table, which roles are cargo-gated, what `FLEXIQ_MAINTENANCE`
      does and does not disable. Absorbs the operator half of the crate README.
- [ ] `operate/server/tokens` — mint, list, rotate, revoke; `produce` vs
      `execute`; expiry warnings. Moved out of `deployment.mdx`.
- [ ] `operate/server/grpc` — bind, the namespace requirement, reflection,
      `raw`/`structured`, the JSON facade, tuning, `/metrics`, the executor
      door, and the TLS gap (#838). Moved out of `deployment.mdx`.
- [ ] `operate/server/scaling` — what to scale on, per role; why the maintenance
      owner is a separate release; that `flexiq scaler` is an SDK command and
      not part of `flexiq-server` (#850).
- [ ] `operate/backup` — SQLite, Postgres and Redis; and what a restore does to
      in-flight leases and durable-step memos.

## Gates this touches

- [ ] `section-skeleton.mjs` — a page the skeleton does not list is an error even
      when the MDX exists, so `server`, `backup` and the new group land there
      first, for all three trees at once.
- [ ] `pnpm check:parity` — section shape, internal links (no link may point at a
      REDIRECTS key), CodeTabs SDK coverage on every shared page.
- [ ] `node scripts/version.mjs --check` — `deployment.mdx` is in the hardcoded
      SNIPPETS list; any new page pinning an image tag joins it.
- [ ] `pnpm typecheck`, `pnpm lint`, `pnpm check:search`, `pnpm build`
      (`NODE_OPTIONS=--max-old-space-size=8192`).

No page is deleted, so no REDIRECTS entry is owed. Five inbound `#anchor` links
in `modules/{server,clients}.mdx` and `python/operate/backends.mdx` do move.

## Commits

Two, not three. The backup page was going to be its own commit, but it shares
nine files with the server section — the skeleton, three `operate/meta.json`,
three `operate/index.mdx`, `deployment.mdx` and `server/index.mdx` — and every
one of those edits is additive to the same list. Splitting them would produce
two commits that each half-configure the same nav, which is the opposite of what
one self-contained change per commit is for. The README repointing does touch
disjoint files, so it is its own.

1. `docs: add an operate section for flexiq-server`
2. `docs: point the server README at the docs site`

## Review

Done. `shared/operate/` went from 3 pages to 8: a `server` group (index, tokens,
grpc, scaling) plus `backup`, and `deployment.mdx` lost 598 lines to the move.

Four things the move corrected rather than relocated:

- **`FLEXIQ_MAINTENANCE=off` does not disable "rescue".** `kubernetes.mdx` said
  it did. `runtime/scheduler.rs` only empties the retention config; dead-worker
  reaping and `recover_orphaned_jobs` stay on in every process, deliberately —
  tying in-flight recovery to the maintenance flag would lose it everywhere but
  on one pod. Fixed in `kubernetes.mdx` and stated in `server/index.mdx`.
- **`deploy/keda/scaled-object-prometheus.yaml` does not work against the
  server.** Its queries name `flexiq_queue_depth` and `flexiq_worker_utilization`,
  which come from an SDK's Prometheus collector; `crates/flexiq-server/src/metrics.rs`
  publishes `flexiq_jobs{queue,status}` and `flexiq_executor_slots{state}`.
  Applied unedited it yields no data, which KEDA reports as a healthy zero.
  `server/scaling.mdx` gives the queries that do match.
- **`flexiq scaler` is not part of `flexiq-server`.** The binary has exactly one
  subcommand, `token`, so the two `metrics-api` manifests need an SDK process
  the deployment may not have. Said plainly on the scaling page.
- **A restore brings revoked tokens back.** Tokens live in the settings KV, which
  is what makes revocation take effect with no restart — and what makes a restore
  predating one restore a working credential. `backup.mdx` treats a restore as a
  credential event.

Verified: `pnpm check:parity` (15 sections × 3 SDKs, 1810 links), `check:search`,
`check:diagrams`, `lint`, `typecheck`, `build` (all four new pages prerender in
all three trees), and `node scripts/version.mjs --check` with the new page added
to SNIPPETS for its pinned image tag.

Left for #825, deliberately: the `operate/server` group is where the server tier
will lift from, not a substitute for it. Nothing here claims to be the fourth
peer beside the three SDKs.
