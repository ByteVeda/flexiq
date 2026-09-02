# #717 — scoped API tokens replace the gRPC shared secret

Reviewed against design-doc §11's row for #717. It fails if: the namespace is taken from
the request; a token is mintable for a namespace the process does not serve; a `produce`
token can open an executor stream; an unconfigured token store serves anything; the
untrusted-network warning survives the PR.

## The two decisions that shape everything

### 1. The store is the settings KV, not a table

`api_tokens` as a real table costs a `sea-query` migration, `schema.rs`, `models.rs`,
`records.rs`, a `diesel_common` macro, three backend impls, **two** forwarding sites in
`storage/mod.rs`, and contract tests in `tests/rust/storage_tests.rs` — the full eight-layer
pipeline, for six methods.

It buys nothing here. Dashboard users, sessions and webhook subscriptions already persist
as JSON documents through `Storage::set_setting` (`dashboard/stores/kv.rs`), which is what
makes them readable by a SQLite, Postgres *and* Redis deployment without three
implementations. A credential row is the same shape of thing and is read at
admin-frequency plus once per RPC — one point read either way.

Decisive detail: `RESERVED_SETTING_PREFIXES` already carries `"auth:"`, and its comment
already reads *"dashboard sessions, OAuth state, API tokens"*. The prefix this needs is
reserved before it is written, so the generic settings API
(`dashboard/routes/settings.rs:27,39,86,113`) hides these rows with **no core change at
all**. A forged token row cannot be written through the settings surface.

**One setting row per token, keyed by the token's public id**: `auth:grpc_token:<id>`.
Not one document holding every token — a point read on the hot path must not parse the
whole store, and `last_used_at` on one token must not CAS against every other token.

### 2. `FLEXIQ_GRPC_TOKEN` and `Anonymous` are deleted, not demoted

The issue's title is *replace*, and §11 fails the PR if "an unconfigured token store serves
anything". A surviving env-var bypass would be an unrevokable all-scope credential in every
deployment, which is precisely what makes the first acceptance criterion — *a revoked token
fails on the next RPC* — untrue. `Anonymous` goes for the same reason: it hands out a
`Principal` with `ScopeSet::ALL` and no credential behind it, and §5.1 says that under #717
the namespace is *carried by the token*.

Consequences, all simplifications:

- `GrpcConfig.token` disappears; so does the non-loopback-without-token refusal in
  `config/grpc.rs:100-109` and the listener's duplicate guard (`grpc/listener.rs:63-69`).
  Every bind is credentialled now, so there is nothing left for loopback to be an exception
  to.
- `grpc/auth/shared_secret.rs` and `Anonymous` are removed; `auth::authenticator()` stops
  being a match and becomes one constructor.
- `scrub_token()` and the `FLEXIQ_GRPC_TOKEN` block in `main.rs`'s `ENV_HELP` go.

**Nothing is released**: #716 merged today (`8bfb65a1`), after 1.0.0. There is no
compatibility window to keep.

### The bootstrap consequence, and why a CLI

Deleting the env var leaves a gRPC-only deployment with no way to mint its first token —
the chart binds `0.0.0.0` and `_validate.tpl` currently *requires* `grpc.token`. Minting
only from the dashboard would force every gRPC deployment to also expose a dashboard.

So: `flexiq-server token create|list|revoke`, the binary's first subcommand. `clap` with
`derive` is already a dependency and `Cli` is already a `Parser` — it is an empty struct
today. This is what `kubectl exec` reaches for, and it is how Temporal and Windmill mint
theirs. The dashboard CRUD the issue asks for lands too; they write the same rows.

## Layout

`src/tokens/` is compiled unconditionally — the dashboard routes and the CLI need it in a
build with no `grpc` feature.

```
crates/flexiq-server/src/tokens/
  mod.rs      barrel + why the store is the settings KV
  scope.rs    Scope, ScopeSet          — MOVED out of grpc/auth/principal.rs
  secret.rs   generate / parse / hash  — the `fqt_<id>.<secret>` format
  model.rs    ApiToken, TokenStatus    — the stored record
  store.rs    create / list / get / revoke / touch, over kv.rs
  cli.rs      the `token` subcommand
```

`Scope` and `ScopeSet` move because a scope is a property of a token and tokens are always
compiled, while `grpc/auth/` is behind `#[cfg(feature = "grpc")]`. `Principal` stays where
it is and re-exports them, so `gate.rs` and `layer.rs` do not change.

New, feature-gated: `grpc/auth/token_store.rs` — the `Authenticator` impl.
New: `dashboard/routes/tokens.rs`, `dashboard/stores/` untouched (the store is shared).

## The token format

`fqt_<id>.<secret>` — `id` is 16 hex chars (8 random bytes), `secret` is 32 random bytes as
base64url-no-pad (43 chars).

- The **`.` separator is not in the base64url alphabet**, so `split_once('.')` is
  unambiguous. `_` would not be: base64url uses it.
- The id is public. It is the setting key, it is what a listing shows, it is what `revoke`
  takes, and it is what a log line names — which is the audit trail the issue says a shared
  secret does not have. The `fqt_` prefix makes a leaked token greppable and
  secret-scanner-shaped.
- Lookup is a **point read**: parse the id, `get_setting("auth:grpc_token:<id>")`, then
  compare `sha256(secret)` against the stored hash with `constant_time_eq`. No scan, no
  index to keep in step.

**Hashing is plain SHA-256, not PBKDF2**, and this is deliberate. `password.rs` runs 600k
PBKDF2 iterations because a password is low-entropy and an offline attacker guesses it.
This secret is 256 bits from the same generator as session tokens; there is nothing to
guess, and a slow KDF here would add its cost to *every RPC* while buying nothing. It is
also why no salt: the input is already unguessable, so there is no rainbow table to defeat,
and a per-row salt would make the hash unindexable. The comment says so, in the file.

## Expiry

Mandatory. `expires_at` has no `None` arm in the record — "a key with no maximum lifetime
is a permanent credential with extra steps". Default 90 days, capped at 365; both refused
loudly rather than clamped silently.

**The warning path is on use, not on a timer.** On a successful authentication, if the
token expires within 30, 20 or 10 days, log a warning once per (token, threshold) per
process. A sweep would need a new background task and would shout about tokens nobody uses;
this notifies the operator about credentials that are actually carrying traffic, at the
three thresholds the issue names, and costs one comparison on a path that already read the
row.

## The seam becomes async

`Authenticator::authenticate` is a sync `fn` today. The store lives in `Storage`, `Storage`
is blocking, and the layer runs on the runtime — so the trait returns a boxed future
(`async-trait`, already a workspace dependency; `dyn Authenticator` rules out plain AFIT).

`layer.rs::call` then needs the standard tower clone-and-replace so the inner service is
owned by the future:

```rust
let clone = self.inner.clone();
let mut inner = std::mem::replace(&mut self.inner, clone);
```

`Routes` is `Clone`, and the ready service must be the one that is called — replacing with
the clone rather than calling it is what keeps `poll_ready`'s reservation honest.

**No cache.** A cached allow decision is a revocation that does not take effect, and
"a revoked token fails on the next RPC with no restart" is acceptance criterion 1. One
point read per RPC, on a path that is about to do a database write anyway.

`last_used_at` is the exception: writing it per RPC would double the write load for a field
nobody reads in real time. It is coalesced — at most one write per token per 60s, tracked
in a `Mutex<HashMap<id, i64>>` on the authenticator — and written through `kv::update` so
it CASes against a concurrent revoke instead of clobbering it.

## The namespace, in two places

**At mint time** (§5.4): the token's namespace is the process's `FLEXIQ_NAMESPACE`. A
request naming a different one is refused; a process with *no* namespace cannot mint at all,
and the error says to set it. There is no way to express the NULL namespace (D11).

**At authentication time**, again: the settings KV is one global keyspace, so two listeners
serving different namespaces off one database read the same rows. A token whose namespace is
not the listener's is refused — as `UNAUTHENTICATED`, not `PERMISSION_DENIED`, so it is not
an oracle for which ids exist. That is acceptance criterion 3 made structural rather than
checked per handler.

## Commits

1. `refactor(server): move Scope out of the grpc feature gate` — `tokens/scope.rs`, plus
   `parse` and serde; `principal.rs` re-exports. No behaviour change.
2. `feat(server): a hashed, scoped, expiring gRPC token store` — `tokens/{mod,secret,model,
   store}.rs` and their tests.
3. `refactor(server): make the Authenticator seam async` — trait, `layer.rs`, existing impls
   still passing.
4. `feat(server): authenticate gRPC calls against the token store` — `token_store.rs`,
   wiring in `listener.rs`, expiry warnings.
5. `feat(server): drop FLEXIQ_GRPC_TOKEN for the token store` — `config/grpc.rs`,
   `shared_secret.rs` and `Anonymous` deleted, `main.rs`, `grpc_auth.rs` rewritten.
6. `feat(server): dashboard CRUD for gRPC tokens` — `routes/tokens.rs`, router, tests.
7. `feat(server): mint tokens from the command line` — `tokens/cli.rs`, `main.rs`.
8. `feat(dashboard): a gRPC tokens page` — the SPA feature folder.
9. `feat(deploy): the chart stops carrying a gRPC secret` — chart + `ci-chart.yml`.
10. `docs: scoped gRPC tokens, and the warning that outlived its issue` —
    `deployment.mdx`, `crates/flexiq-server/README.md`, design-doc amendment if any.

## Tests

`tests/grpc_auth.rs` is rewritten around the store. The three acceptance criteria are three
named tests:

- `a_revoked_token_fails_on_the_next_rpc` — enqueue, revoke through the store, enqueue
  again on the **same channel and the same server**. No restart, no reconnect.
- `a_produce_token_cannot_open_an_executor_stream` — `PERMISSION_DENIED` + `SCOPE_DENIED`
  + the `scope` metadata key, over the wire rather than in a unit test.
- `a_token_for_another_namespace_is_refused` — minted for `other`, presented to a listener
  serving `prod`.

Kept from #716 and still true: health is public, an unrouted path answers `UNAUTHENTICATED`
rather than `UNIMPLEMENTED` (the property that proves the check precedes routing), and every
way of being wrong is one answer. New: an **empty store serves nothing** — the fail-closed
default, which is the one #716 could not assert because `Anonymous` existed.

`tests/http_api.rs` gets the CRUD lifecycle with the secret asserted present exactly once
and absent from every later read. `gate.rs`'s table test gains the new path.

## Verify

`cargo fmt --check` · `cargo clippy --all-targets --all-features -D warnings` ·
`cargo check --workspace` on default / postgres / redis · `cargo test -p flexiq-server`
**and** `cargo test -p flexiq-server --features grpc` (the default run compiles the role
out) · `cargo test --workspace` · `pnpm --dir dashboard build` then `pnpm --dir dashboard
lint` / `types:check` · `pnpm --dir docs build`. All with `-j2`.

No SDK shell changes: this is a server-side credential and no shell speaks gRPC yet.
