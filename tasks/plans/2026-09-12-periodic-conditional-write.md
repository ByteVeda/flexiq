# Re-registering a periodic is a lost update (#919)

`register_periodic` is an unconditional replace, so anything that wants a
stored value to survive a worker restart has to read the row first — and that
read-then-write is a lost update. Two values are at stake, and neither belongs
to the declaration:

- **`next_run`** is the scheduler's. Another worker's scheduler advances it
  through `update_periodic_schedule` at any moment; putting a stale one back
  re-fires a job that already ran.
- **`enabled`** is the operator's. `set_periodic_enabled` can land between the
  read and the write; putting `true` back resumes a task somebody paused, so a
  pause would not survive a deploy.

`crates/flexiq::cron::register` sidesteps the steady-state case today by not
writing at all when the stored `cron_expr`, `timezone` and `queue` already
match the declaration. That closes the window on every restart of an unchanged
deployment and on nothing else: a deploy that genuinely changes a cron
expression still reads, decides, and writes, with no fence across the two.

## The fix

A second write on the `Storage` contract, conditional in the backend, so the
caller has nothing to read.

```rust
fn declare_periodic(&self, task: &NewPeriodicTask) -> Result<()>;
```

`register_periodic` stays as it is — Node and Java expose it as an explicit
replace, `enabled` argument included, and the dashboards write through it.
`declare_periodic` is the other half: the write a *code declaration* makes,
which owns some columns and must not touch the rest.

On an existing row it writes `task_name`, `cron_expr`, `args`, `kwargs`,
`queue` and `timezone`, and:

- **never** writes `enabled` — a pause outlives a deploy;
- **never** writes `last_run`, as `register_periodic` already does not;
- writes `next_run` only when `cron_expr` or `timezone` changed, because only
  then was the stored deadline computed from a schedule that no longer exists.
  A queue rename is not a schedule change and keeps the deadline.

On no row it inserts `task` whole, `enabled` and `next_run` included, so a
declaration can still be born paused.

`next_run` is computed by the caller from its own clock, as every other
periodic write is. The backend decides whether to apply it; it does not parse
cron.

## Why not read the row inside a transaction

Because that is not enough on Postgres. `PostgresStorage::write_transaction` is
a plain `BEGIN` at READ COMMITTED, so a `SELECT` inside it takes no row lock and
a concurrent `update_periodic_schedule` still commits between the read and the
`UPDATE`. Locking the read would mean `FOR UPDATE`, which is Diesel's
Postgres-only method, and that reintroduces the per-backend split #933 just
removed. SQLite would be fine either way — `write_transaction` is
`BEGIN IMMEDIATE` — but the shared macro has to be correct on both.

One statement is both simpler and stronger: the row lock the `UPDATE` takes is
the fence, on either backend.

## Backends

**Diesel** — a single UPDATE whose SET list omits `enabled` and passes
`next_run` through `CASE WHEN <schedule changed> THEN ? ELSE next_run END`
(`diesel::dsl::case_when`). The "schedule changed" predicate is built in Rust
because the declared timezone is known there, which is how a nullable column is
compared without a portable `IS DISTINCT FROM`. Zero rows updated means no row,
so INSERT; the existing retry-once-on-unique-violation wrapper covers a
concurrent first registration.

**Redis** — the `WATCH`-guarded `rewrite_periodic` helper already does this
shape. The mutate closure receives the stored entry and carries `enabled`,
`last_run` and (unless the schedule changed) `next_run` forward from it. The
`EXEC` aborts if anything touched the key since the read, so the decision and
the write are one.

## The shell

`cron::register` drops the `list_periodic` read and calls `declare_periodic`.
It computes `next_run` unconditionally now — a cron parse, not a query — and
trades a full-table read per declared schedule per worker start for one UPDATE.

## Out of scope

The Python shell registers its schedules at worker start with a hardcoded
`enabled: true` (`crates/flexiq-python/src/py_queue/mod.rs`), so it has the
operator-pause half of this bug on its own path. Node and Java take an explicit
`enabled` and are an intentional replace. Moving Python onto `declare_periodic`
is a separate change against a separate issue.

## Steps

- [x] 1. `declare_periodic` on the `Storage` contract, delegated to the backends
- [x] 2. Diesel: one conditional UPDATE, else INSERT
- [x] 3. Redis: the same decision inside the `WATCH`
- [x] 4. `cron::register` drops the read
- [x] 5. Tests: the cross-backend contract suite and the Rust shell
- [x] 6. Docs: the two places that promise "writes nothing"
