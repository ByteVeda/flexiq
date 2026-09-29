# Token grants beyond produce and execute (#839)

## Goal

A token can be narrowed three ways it cannot be today:

- **read-only** on the producer package: see jobs, never submit or cancel;
- **per-queue**: enqueue to `emails`, not to `billing`;
- **per-task**: enqueue exactly `send_receipt`.

"Admin, separately" already shipped with #836 (`inspect` / `admin`).
The namespace binding stays as it is. Everything here is authorization
*inside* one namespace.

## Model: a token holds grants, not only scopes

A **grant** is a scope, optionally narrowed by a queue pattern and a task
pattern:

```
produce                                  every queue, every task (today's meaning)
read                                     read-only producer methods, every queue
produce:queue=emails                     one queue
produce:queue=emails-*,task=send_receipt one task on queues named emails-…
read:queue=emails
```

- A **pattern** is an exact name, a prefix ending in `*`, or `*` on its own.
  `*` may appear only as the last character. A name must not contain `,`, `=`,
  whitespace or control characters, and must be ≤ 200 chars.
- Only `produce` and `read` take qualifiers. `execute`, `inspect` and `admin`
  with a qualifier are refused at mint. See "Out of scope" below.
- A token's grants are a **union**: a call is allowed when **any** grant covers
  it.
- A `produce` grant also covers `read`, because `produce` has always included
  reads. Existing tokens keep exactly what they have.

### Storage: no migration, fail-closed on downgrade

Grants are stored in the row's existing `scopes` array, as their spelled form.

- An unqualified name (`"produce"`) is today's form, so **existing rows read
  back unchanged**.
- A qualified entry (`"produce:queue=emails"`) is a name an older build does not
  know. `NameVisitor` already *drops* unknown names, which narrows. So a downgrade
  can only make a narrowed token open less, never more.
- The new build drops any unparseable entry, with a warning, for the same reason.
  **An unrecognised or unparseable pattern denies.**

Types (in `tokens/scope.rs`, with the grammar in a new `tokens/grant.rs`):

- `ScopeSet` stays as the bitset of **unrestricted** grants.
- `Grant { scope, queue: Pattern, task: Pattern }`.
- `Grants { unrestricted: ScopeSet, restricted: Vec<Grant> }` replaces
  `ApiToken.scopes`. Its serde is the same array of strings.
- `From<ScopeSet> for Grants`. `NewToken::new` takes `impl Into<Grants>`, so
  every existing caller and test that passes a `ScopeSet` compiles unchanged.

## Enforcement

### Gate (`grpc/auth/gate.rs`)

- New `Scope::Read`. Producer methods split the same way admin already does:
  - `READ_METHODS`: GetJob, ListJobs, QueueStats, GetWorkflowRun, WatchJobs.
    A test holds this list to the descriptor's `NO_SIDE_EFFECTS` level, in both
    directions.
  - Every other producer method needs `produce`. A new method fails closed onto
    `produce`.
- On the facade, `GET` under `/v1` (outside `/v1/admin`) is `read`, and any other
  verb is `produce`.

### Layer → `Principal::access`

After the scope check passes, the layer narrows the principal to the scope the
path needs, as `Access`:

- `Unrestricted` if any unrestricted grant covers the scope;
- otherwise `Restricted(Vec<(Pattern, Pattern)>)`.

The facade already clones the principal into the request it hands the service,
so the access travels with it.

- **Admin and executor packages, and `/v1/admin`:** the layer requires
  `Unrestricted`. They have no resource-aware methods, so a restricted grant
  opens none of them.
- **Producer handlers:** `Producer::scope()` requires `Unrestricted` by default,
  and refuses otherwise. **A new RPC is therefore closed to narrowed tokens until
  someone makes it resource-aware on purpose** (D10's property, kept). Only the
  handlers below use the resource-aware entry point:

| RPC | narrowed token |
|---|---|
| Enqueue / EnqueueBatch | check the task and the *resolved* queue (after `prepare`, so the `""` → `default` rewrite is what gets checked). Batch: the first refused item fails the whole batch, reported `.at_index(i)` |
| GetJob | load the row; if refused → `NOT_FOUND`, the same answer as another namespace (no existence oracle) |
| CancelJob | load the row *before* cancelling; if refused → `NOT_FOUND` |
| ListJobs | must name a `queue` filter the token's queue patterns match, and a `task_name` filter if the token is task-narrowed; otherwise `PERMISSION_DENIED` naming the missing filter. No post-filtering, so keyset pages stay honest |
| QueueStats | must name a permitted queue; a task-narrowed token is refused, because the stats aggregate other tasks |
| WatchJobs | queue watch: the queue must be permitted; a task-narrowed grant is refused (as built — simpler than filtering, and closed). Id watch: every id's row is checked up front, and a refused id reads as unknown |
| SubmitWorkflow / GetWorkflowRun | unrestricted only (default path) |

A refusal uses the existing `SCOPE_DENIED` reason and the `scope` metadata key,
plus `queue` / `task` keys and a message naming what was refused. That means no
new reason on the wire contract; an SDK that already handles scope-denied still
does.

## Surfaces

- **CLI** (`tokens/cli.rs`): `--scope` now takes the grant grammar
  (`--scope produce:queue=emails`) and adds `read`. It is a `value_parser` fn
  instead of a `ValueEnum`, so an unparseable grant fails at parse. `token list`
  prints the spelled grants.
- **Dashboard API** (`dashboard/routes/grpc_tokens.rs`):
  - mint `scopes[]` accepts grant strings and refuses unparseable ones with `400`;
  - `GET /api/grpc-tokens/scopes` lists `read` and marks which scopes take
    qualifiers.
- **Dashboard UI** (`dashboard/src/features/grpc-tokens/`):
  - a `read` checkbox;
  - optional "queue pattern" and "task pattern" inputs that narrow the selected
    `produce` / `read` grants;
  - the table renders the grant strings.
- **Docs**: `server/operate/tokens.mdx` gets a "Narrowing a token" section
  covering the grammar, the union rule, the per-RPC table and the downgrade note.
  Also touch the scope mentions in `grpc.mdx` / `cli.mdx`.

## Out of scope (follow-up issues after merge)

- Queue- or task-narrowed `execute` (Hello's task list is the hook) and `admin`
  / `inspect` (per-queue pause, per-queue DLQ).
- Resource-aware workflows (per-node task/queue check on submit).

Until those exist, all of these are refused, not opened.

## Tests

- **Unit:**
  - grant grammar: round trip, and every refusal;
  - pattern matching;
  - `Grants` serde — an old row reads back identical, and an unparseable or
    unknown entry is dropped;
  - `Access` union;
  - gate `READ_METHODS` against the descriptor.
- **Integration** (`tests/grpc_auth.rs` or a new `tests/grpc_grants.rs`):
  - `read` token: reads pass, Enqueue and Cancel are refused;
  - queue-narrowed token: enqueue to its queue passes, to another is refused;
    GetJob/Cancel on another queue's job → `NOT_FOUND`; ListJobs without a
    filter is refused;
  - task-narrowed: enqueue of another task is refused;
  - narrowed token on admin, executor and SubmitWorkflow is refused;
  - the facade gets the same answers;
  - a legacy row (plain `["produce"]` JSON written straight into settings) still
    enqueues everywhere.
- **Existing:** `http_api.rs` mint/refusal cases for grants, and CLI parse tests.

## Build order / commits

1. `feat(tokens): grant grammar and patterns`: `grant.rs` and `Grants`, serde, unit tests
2. `feat(grpc): read scope for producer read methods`: gate, `READ_METHODS` test
3. `feat(grpc): narrow principal access per path`: layer, `Access`, admin/executor refusal
4. `feat(grpc): enforce queue and task grants on producer RPCs`: handlers and integration tests
5. `feat(tokens): mint narrowed grants from CLI and dashboard API`
6. `feat(dashboard): queue and task patterns in token dialog`
7. `docs: narrowing a token`

Verification:
- `cargo test -j1 -p flexiq-server --features grpc` (targeted `--test`)
- clippy with `CARGO_BUILD_JOBS=1`
- `cargo check -j1 --workspace`
- dashboard `pnpm` typecheck/lint/test
- docs typecheck/lint

## Review notes (as built)

- Enqueue also conceals a `unique_key` dedup hit outside the grants, and refuses
  a debounced enqueue from a narrowed token (the keys match namespace-wide).
- CancelJob's dependency cascade reaches dependents in other queues. Kept on
  purpose and documented: the dependent's creator chose the edge, and refusing
  would let another queue veto a narrowed token cancelling its own job.
