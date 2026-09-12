# Periodic tasks are keyed by name across every namespace (#918)

`periodic_tasks` is the last table in the job model with no namespace. Every
backend keys it by `name` alone, so on a database shared by more than one
namespace two tenants registering the same task name overwrite one another's
row, `list_periodic` returns everyone's schedules, and `delete_periodic` /
`set_periodic_enabled` reach a name the caller does not own. Jobs have been
scoped since `m0017`; this closes the hole.

## Identity

`(namespace, name)`. `namespace` is `Option<String>` — `None` is the default
namespace, the same value `NewJob.namespace` carries — and is **not** folded
into `Some("")`, which `m0017` already treats as a different namespace.

The DB constraint is therefore a unique index over
`((namespace IS NULL), COALESCE(namespace, ''), name)`, not `(namespace, name)`:
both SQLite and Postgres treat two NULLs as distinct inside a unique index, so
the naive form would silently stop constraining the default namespace — the
common case, and the exact trap `m0017` documents.

## What `None` means on a method

A periodic has no globally unique id. Every method addresses it by identity, so
`namespace: None` names the **default namespace** — one row, not a wildcard.
That is the `NewJob.namespace` / `find_active_by_unique_key` rule, not the
`get_job(id, None)` one, which can only be a wildcard because a job id is
unique on its own.

`get_due_periodic` is the single exception: it is the engine's read, not an
address, and a scheduler with no namespace serves the whole cluster, so `None`
there reads every namespace and the fired job inherits the **row's** namespace.

## Backends

**Diesel.** The `name` PRIMARY KEY has to go, and SQLite cannot drop one, so
`m0018` rebuilds the table: create the new shape, copy, drop, rename. A
migration runs inside one transaction and both backends have transactional DDL,
so the rebuild is atomic — there is no partial state to recover from.

The upsert changes with it. SQLite's `REPLACE INTO` and Postgres'
`ON CONFLICT (name)` both targeted the old PK, and Diesel cannot name an
expression index as a conflict target. Both collapse into one
`diesel_common/periodic.rs` macro doing UPDATE-then-INSERT in a transaction,
which also stops SQLite's `REPLACE` from wiping `last_run` on every
re-registration.

**Redis.** Keys become `periodic:<ns-segment>:<name>` using the same
length-prefixed `namespace_segment` encoding `m0017` gave the `unique_key`
pointer, and the `periodic:due` sorted set holds **full keys** as members rather
than bare names. Legacy `periodic:<name>` rows are orphaned, not migrated
(the #773 precedent): a legacy due member is a bare name, which is not a key
under the periodic root, so nothing reads it — no double-fire and no re-fire
loop — and `list_periodic` skips any key that is not the key its own
`(namespace, name)` would compute, which is exactly the set of pre-#918 rows.

## Shells

Scope rides on the handle, which already carries a namespace in all four
shells, so no shell's public signature changes. `crates/flexiq`'s refusal comes
out: a namespaced handle registers, lists, deletes, pauses and resumes its own
schedules like any other operation.

The shells' listing views do not gain a `namespace` field. Under the identity
rule a handle only ever sees rows in its own namespace, so the field would be a
constant.

## Steps

- [x] 1. `namespace` on the records, the row structs, the schema and `m0018`
- [x] 2. The `Storage` contract and one shared Diesel implementation
- [x] 3. Redis: namespaced keys, full-key due members, legacy rows inert
- [x] 4. The scheduler scopes its due read and fires into the row's namespace
- [x] 5. The four shells pass their handle's namespace; the Rust refusal lifts
- [x] 6. Tests: the cross-backend contract suite, the scheduler, the Rust shell
- [x] 7. Docs and the changelog
