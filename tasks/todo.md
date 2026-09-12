# Periodic tasks are keyed by name across every namespace (#918)

Plan: `tasks/plans/2026-09-11-periodic-task-namespace.md`

`periodic_tasks` is the last table in the job model with no namespace, so two
tenants sharing a database overwrite one another's schedules, list one
another's, and can delete a name they do not own. Identity becomes
`(namespace, name)`; `None` is the default namespace, not a wildcard, because a
periodic has no globally unique id to address it by.

- [x] 1. `namespace` on the records, the row structs, the schema and `m0018`
- [x] 2. The `Storage` contract and one shared Diesel implementation
- [x] 3. Redis: namespaced keys, full-key due members, legacy rows inert
- [x] 4. The scheduler scopes its due read and fires into the row's namespace
- [x] 5. The four shells pass their handle's namespace; the Rust refusal lifts
- [x] 6. Tests: the cross-backend contract suite, the scheduler, the Rust shell
- [x] 7. Docs and the changelog

## Review

A periodic task is identified by `(namespace, name)` on every backend. Each
`Storage` member that addresses one takes the namespace, and `None` there names
the **default** namespace — a row, not a wildcard. That is the `NewJob`
/`find_active_by_unique_key` rule rather than `get_job`'s, and it has to be: a
schedule has no id of its own, so a bare name with no namespace beside it would
otherwise reach every tenant's.

`get_due_periodic` is the single exception, and it is not an address. A
scheduler running without a namespace serves the whole cluster, so it reads
every namespace's due rows and each job it mints inherits the **row's**
namespace, not the scheduler's. A namespaced scheduler reads only its own.

### The index, not the column pair

`m0018` rebuilds the table — SQLite cannot drop the `name` PRIMARY KEY — and the
uniqueness that replaces it is an index over *expressions*:

```sql
CREATE UNIQUE INDEX … ON periodic_tasks ((namespace IS NULL), COALESCE(namespace, ''), name)
```

`UNIQUE (namespace, name)` would have silently stopped constraining the default
namespace: both backends treat two NULLs as distinct inside a unique index, and
the default namespace is the common case. The leading discriminator is there
because `COALESCE` alone folds `None` into `Some("")`, which `m0017` already
treats as a different namespace.

Diesel cannot name an expression index as a conflict target, so both Diesel
backends lost their upsert and share one UPDATE-else-INSERT in
`diesel_common/periodic.rs` instead — 170 lines of near-duplicate down to one
macro. That fixed a second bug on the way: SQLite's `REPLACE INTO` deleted the
row before re-inserting it, so **every worker restart forgot `last_run`** while
Postgres kept it. A contract test now pins the surviving behaviour.

### Redis

Keys are `periodic:<ns-segment>:<name>` with the same length-prefixed encoding
the `unique_key` pointer got in #773, and the due sorted set holds whole keys
rather than bare names. Pre-#918 rows are orphaned, not migrated (the #773
precedent), and inert rather than merely unreachable: a legacy due member is not
a key under the periodic root, so nothing reads it — reading one *would* fire it
and then write the advance to the new key, leaving the old `next_run` to fire
again forever. The due read drops such a member as it finds it, because nothing
else ever will: its score does not advance, so it would come back on every pass.

Each write is a read-modify-write of one JSON document, so all three go through
one `WATCH`-guarded helper. Without it a worker re-registering a schedule would
put back the `last_run` a scheduler had just advanced — the Diesel backends have
no such window, because their `AsChangeset` never names `last_run` at all.

### The migration ledger is now the lock

Review caught that `run_generic` read `schema_migrations` *outside* the
per-migration transaction and recorded the version with `ON CONFLICT DO
NOTHING`. Two processes booting at once could therefore both see `0018` as
pending, both rebuild the table, and both commit — the loser copying the rows
through `CARRIED`, which has no `namespace`, and resetting every schedule to the
default namespace. Harmless while every migration was idempotent; not harmless
for a rebuild.

The recording now runs **first**, inside the transaction, without the conflict
clause: the ledger's primary key is what serializes the two, and the loser rolls
back before touching the schema. A unique violation is only treated as a lost
race when the ledger actually holds the version afterwards, so a migration whose
own DDL collides with existing data still fails the run.

### Shells

Scope rides on the handle, which already carried a namespace in all four shells,
so no public signature changed. `crates/flexiq`'s blanket refusal is gone: a
namespaced handle now registers, lists, deletes, pauses and resumes its own
schedules like any other operation. The listing views did not gain a `namespace`
field — under the identity rule a handle only ever sees its own rows, so it
would be a constant.

### Verification

`cargo check --workspace` clean on default, `--features postgres` and
`--features redis` (`--all-targets` for the latter two); `cargo test -p
flexiq-core` and `-p flexiq` green, which covers the SQLite contract suite, the
scheduler cases and the Rust shell's. The Postgres and Redis runs of the same
cross-backend suite need `FLEXIQ_POSTGRES_TEST_URL` / `FLEXIQ_REDIS_TEST_URL`,
which this machine has not got — CI's three Rust jobs are what exercises those.

The changelog is written at a release, not per PR (nothing since the 2.0.0 bump
touches it), so this leaves `CHANGELOG.md` alone and puts the behaviour change
in the docs: the four Rust pages that documented the refusal, the shared
scheduling guide, and `crates/flexiq/README.md`.
