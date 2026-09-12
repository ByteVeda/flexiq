# Re-registering a periodic is a lost update (#919)

Plan: `tasks/plans/2026-09-12-periodic-conditional-write.md`

`register_periodic` is an unconditional replace, so preserving `next_run` or
`enabled` across a worker restart means reading the row first — and that
read-then-write races the scheduler advancing a deadline and an operator
pausing a task. A conditional write goes on the `Storage` contract instead, and
`crates/flexiq::cron::register` stops reading.

- [x] 1. `declare_periodic` on the `Storage` contract, delegated to the backends
- [x] 2. Diesel: one conditional UPDATE, else INSERT
- [x] 3. Redis: the same decision inside the `WATCH`
- [x] 4. `cron::register` drops the read
- [x] 5. Tests: the cross-backend contract suite and the Rust shell
- [x] 6. Docs: the two places that promise "writes nothing"

## Review

`Storage::declare_periodic` is the write a code declaration makes. It owns
`task_name`, `cron_expr`, `args`, `kwargs`, `queue` and `timezone`; `enabled`
is the operator's and is never written, `last_run` is the scheduler's and was
already never written, and `next_run` is the scheduler's too — it is replaced
only when `cron_expr` or `timezone` changed, because only then was the stored
deadline computed from a schedule that no longer exists. A queue rename keeps
the deadline. With no row at all the declaration is inserted whole, `enabled`
and `next_run` included, so a schedule can still be born paused.

The point is that the condition is evaluated *where the write happens*.
`cron::register` used to read through `list_periodic`, decide, and write
through `register_periodic`, with nothing fencing the two — and it could only
avoid the resulting lost update by declining to write at all, which left the
changed-declaration path uncovered. It now reads nothing.

Reading the row inside `write_transaction` would not have been enough.
`PostgresStorage::write_transaction` is a plain READ COMMITTED transaction, so
a `SELECT` in it takes no row lock and a concurrent `update_periodic_schedule`
still commits in between; locking the read means `FOR UPDATE`, which is
Diesel's Postgres-only method and would have split the shared macro #933 just
unified. One `UPDATE` statement is both simpler and stronger — the row lock it
takes is the fence, and SQL evaluates every SET expression against the
pre-update row, so the `CASE` that decides `next_run` sees the stored schedule
rather than the one it is writing.

The "did the schedule change" predicate is built in Rust rather than as a
symmetric SQL comparison. `timezone` is nullable, Diesel has no portable
`IS DISTINCT FROM`, and `timezone <> 'Europe/Stockholm'` is NULL — not true —
on a row that stores none. With the declared value known on the Rust side each
case is an ordinary predicate, and the contract suite covers both directions:
adding a timezone and dropping one.

Redis needed no new machinery. `rewrite_periodic`'s `WATCH` already turns a
read-modify-write into a retry, so the closure decides from the document it
read and the `EXEC` aborts if anything touched the key since.

`register_periodic` is unchanged and still an unconditional replace: Node and
Java expose it with an explicit `enabled` argument, and the dashboards write
through it.

### Verified

- `cargo check --workspace`, `--features native-async`; `cargo check -p
  flexiq-core --tests` under `postgres` and under `redis`.
- `cargo test -p flexiq-core --test rust` — 100 passed, SQLite contract suite
  included.
- `cargo test -p flexiq --test periodic` — 8 passed.
- Postgres and Redis run the same contract suite in CI; no hosted
  `FLEXIQ_POSTGRES_TEST_URL` / `FLEXIQ_REDIS_TEST_URL` was available locally,
  so those two backends are compile-verified here and asserted on CI.

### Not in this change

The Python shell registers its schedules at worker start with a hardcoded
`enabled: true` (`crates/flexiq-python/src/py_queue/mod.rs`), so it still
resumes a paused schedule on every restart. Node and Java take an explicit
`enabled` and are an intentional replace. Moving Python onto `declare_periodic`
belongs to its own issue.
