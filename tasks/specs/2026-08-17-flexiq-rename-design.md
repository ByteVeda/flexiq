# Rename Taskito → FlexiQ

**Date:** 2026-08-17
**Status:** Approved, pending implementation plan
**Branch:** `rename/flexiq` off `dev` (created off `master` at `1a4b376f`)

## Goal

Rename the project, repository, and every published artifact from Taskito to
FlexiQ. Clean break: no compatibility shims, no dual-name code paths anywhere.

## Scope

Measured footprint: **10,332 occurrences** across **~1,396 files**
(`taskito` 7,623 / `Taskito` 1,714 / `TASKITO` 995), plus ~1,700 PascalCase
identifiers.

Per-area file counts:

| Area | Files | Area | Files |
|---|---|---|---|
| `sdks/java` | 441 | `crates/taskito-python` | 26 |
| `docs` | 414 | `crates/taskito-node` | 26 |
| `sdks/python` | 301 | `crates/taskito-core` | 22 |
| `sdks/node` | 138 | `dashboard` | 22 |
| `crates/taskito-server` | 61 | `crates/taskito-java` | 19 |
| `.github` | 18 | `crates/taskito-workflows` | 18 |
| `deploy` | 15 | `examples` | 12 |
| `crates/taskito-tui` | 9 | `crates/taskito-mesh` | 8 |
| `crates/taskito` | 3 | `scripts` | 3 |
| `contracts` | 1 | `docker` | 1 |

## Decisions

| # | Decision | Rationale |
|---|---|---|
| D1 | Clean break on published packages; no alias shims | Users reinstall once under the new name. Alias packages on three registries are permanent release plumbing. |
| D2 | `FLEXIQ_*` env vars only, no `TASKITO_*` fallback | A fallback resolver would need four implementations (Rust, Python, Node, Java) and dual-path tests, for a migration users already perform when they reinstall. |
| D3 | Rename persisted/in-flight names; require a drain | Payload markers, the webhook header, the session cookie, and the DB path all change. Release note instructs: drain the queue, move `~/.taskito/taskito.db` → `~/.flexiq/flexiq.db`, update webhook receivers, re-login. |
| D4 | `FlexiQ` casing everywhere, including identifiers | Brand casing is preserved in type names (`FlexiQError`, `InMemoryFlexiQ`). Code and wordmark never diverge. |
| D5 | One branch, ~14 staged commits, one PR | Reviewable by area. Intermediate commits will not build: renaming a crate breaks every dependent at once. |
| D6 | Version `0.23.0` → `1.0.0` in the same branch | The change is breaking by definition, and the semver gate already requires a bump before a breaking core change. |

## Name map

| Surface | Taskito | FlexiQ |
|---|---|---|
| Crates | `taskito-{core,workflows,mesh,python,node,java,server,tui}`, `taskito` | `flexiq-*`, `flexiq` |
| Rust types | `TaskitoError` | `FlexiQError` |
| Python dist / module / bin | `taskito`, `taskito._taskito`, `taskito` | `flexiq`, `flexiq._flexiq`, `flexiq` |
| Python stub | `taskito/_taskito.pyi` | `flexiq/_flexiq.pyi` |
| Node package / bin / binaryName | `@byteveda/taskito`, `taskito`, `taskito` | `@byteveda/flexiq`, `flexiq`, `flexiq` |
| Java package | `org.byteveda.taskito.*` | `org.byteveda.flexiq.*` |
| Java artifact / rootProject | `taskito` | `flexiq` |
| JNI symbols | `Java_org_byteveda_taskito_*` | `Java_org_byteveda_flexiq_*` |
| Env vars (98 distinct) | `TASKITO_*` | `FLEXIQ_*` |
| Payload markers | `__taskito_ref__`, `__taskito_proxy__`, `__taskito_convert__`, `__taskito_redirect__`, `__taskito_cache__` | `__flexiq_*__` |
| HTTP headers | `X-Taskito-Signature`, `X-Taskito-Token` | `X-FlexiQ-Signature`, `X-FlexiQ-Token` |
| Session cookie | `taskito_session` | `flexiq_session` |
| User-Agent | `taskito/<version>` | `flexiq/<version>` |
| Dashboard storage key | `taskito.theme` | `flexiq.theme` |
| Runtime paths | `~/.taskito/taskito.db`, `/run/taskito.sock`, `taskito.log` | `~/.flexiq/flexiq.db`, `/run/flexiq.sock`, `flexiq.log` |
| Redis lock literals (4) | `taskito:reaper`, `taskito:retention`, `taskito:debounce:` | `flexiq:*` |
| Helm chart | `deploy/helm/taskito-server` | `deploy/helm/flexiq-server` |
| Docs path | `docs.byteveda.org/taskito` | `docs.byteveda.org/flexiq` |
| Repository | `ByteVeda/taskito` | `ByteVeda/flexiq` |

### Explicitly unchanged

Database table names (`jobs`, `workers`, `dead_letter`, `archived_jobs`,
`topic_subscriptions`, …) carry no product prefix. The migration ledgers
`schema_migrations` and `workflow_schema_migrations` and the migration file
names `m0001_initial.rs` … `m0010_debounce.rs` are unaffected. The wire
envelope tag bytes (`0x00`/`0x01`/`0x02`) and CBOR encoding rules do not
change; only the marker strings carried inside payloads do.

## Registry availability (verified 2026-08-17)

| Registry | Old name | New name |
|---|---|---|
| crates.io | `taskito`, `taskito-core` published | `flexiq`, `flexiq-core` free (sparse index 404) |
| PyPI | `taskito` published (0.23.0) | `flexiq` free |
| npm | `@byteveda/taskito` published | `@byteveda/flexiq` free |
| Maven Central | `org.byteveda:taskito` | group already ours |

## Preconditions

1. **`gh` token scope.** Active account is `pratyush618`, whose token scopes are
   `admin:public_key, gist, read:org, repo` — no `workflow`. The rename edits
   `.github/workflows/*`, so a push is rejected until the user runs
   `gh auth refresh -h github.com -s workflow`.
2. **Git identity.** The repo-local `user.name` / `user.email` /
   `user.signingkey` overrides were unset so commits inherit the global
   `Pratyush Sharma <56130065+pratyush618@users.noreply.github.com>` and
   `~/.ssh/id_ed25519_signing.pub`. Done.
3. **Untracked files.** Pre-commit hooks drop untracked files.
   `examples/polyglot/java-worker/bin/` is backed up to the session scratchpad.
   Done.
4. **`dev` branch.** Created locally off `master`; must be pushed to `origin`
   before the PR can target it.

## Commit plan

Each commit is one area. Only the branch tip builds.

1. `refactor: rename core crates to flexiq` — `git mv` of `taskito-core`,
   `taskito-workflows`, `taskito-mesh`; workspace `members` and
   `[workspace.dependencies]`; `use taskito_core::` → `use flexiq_core::`;
   `TaskitoError` → `FlexiQError`; crate-level rustdoc; README doctests.
2. `refactor: rename binding crates to flexiq` — `taskito-python`,
   `taskito-node`, `taskito-java`, `taskito-server`, `taskito-tui`, `taskito`.
3. `refactor: rename python sdk to flexiq` — package dir, `_flexiq.pyi`,
   `pyproject.toml` (`name`, `module-name`, `[project.scripts]`, ruff
   `known-first-party`, every mypy override, maturin `include` globs), Django
   template dir and `templatetags/flexiq_admin.py`, static dashboard path,
   imports across source and tests.
4. `refactor: rename node sdk to flexiq` — `package.json` name / `bin` /
   `exports` / `repository.directory` / `napi.binaryName`, the seven platform
   targets, native artifact filename, `src/`, tests.
5. `refactor: rename java sdk to flexiq` — package directories, every
   `package`/`import` declaration, JNI symbol names on both the Rust and Java
   side, `getResourceAsStream` paths, `settings.gradle.kts` `rootProject.name`,
   `build.gradle.kts` coordinates.
6. `refactor: rename env vars to FLEXIQ prefix` — 98 variables across four
   SDKs, the server crate, helm, keda, docker, tests, and docs.
7. `refactor: rename wire and http identifiers` — payload markers, headers,
   cookie, User-Agent, dashboard storage key; regenerate
   `contracts/wire-vectors.json` and re-verify cross-SDK `auto:` keys.
8. `refactor: rename runtime paths to flexiq` — DB path, socket, log file,
   Redis lock literals.
9. `refactor: rename dashboard to flexiq` — `dashboard/src`, titles,
   `api-types.ts`.
10. `ci: rename workflows and deploy manifests for flexiq` — workflow path
    filters, composite actions, helm chart directory, keda manifests, docker.
11. `docs: rename taskito to flexiq` — `docs/`, `README.md`,
    `ARCHITECTURE.md`, `CONTRIBUTING.md`.
12. `chore: bump version to 1.0.0` — `scripts/version.mjs` coordinate regexes
    first, then `node scripts/version.mjs --set 1.0.0`.
13. `docs: add flexiq migration guide` — docs page plus a CHANGELOG entry
    covering the drain requirement from D3.
14. `chore: update claude memory and skills for flexiq` — `.claude/**`,
    `CLAUDE.md`.

## Verification

Run at the branch tip, in order:

1. `cargo check --workspace` and the `postgres`, `redis`, `native-async`
   feature variants.
2. `cargo test --workspace`, then with `--features workflows`.
3. Delete stale native artifacts: `sdks/python/flexiq/_taskito*.so`,
   `sdks/node/native/taskito.*.node`, `sdks/java/build/`.
4. `uv sync --extra dev --extra oauth`, then
   `uv run maturin develop --reinstall-package flexiq`.
5. `uv run python -m pytest tests/ -v` (1007 tests), `uv run ruff check
   flexiq/ tests/`, `uv run mypy flexiq/ --no-incremental`.
6. Node build and test; Gradle test.
7. `pnpm --dir docs types:check`, `lint`, `build`.
8. pyo3-leakage tripwire; `cargo publish --dry-run` per publishable crate.
9. Final gate: `grep -ri taskito` returns hits only in CHANGELOG history and
   the migration guide.

## Traps the compiler will not catch

- **JNI symbol / package mismatch** surfaces only as a runtime
  `UnsatisfiedLinkError`. The Rust `#[no_mangle]` names and the Java package
  must be renamed in the same commit.
- **Java resource paths** — `getResourceAsStream("/org/byteveda/taskito/…")`
  is a string; the dashboard assets directory must move with it.
- **Django** — `{% load taskito_admin %}` in templates and the
  `templates/taskito/` lookup directory are both string-matched at render time.
- **CI path filters** — `crates/taskito-server/**` silently stops matching
  after the directory moves. Jobs disappear rather than fail, so the workflow
  rename must land with the crate rename in mind.
- **maturin `module-name`** must equal the on-disk package directory or the
  extension import fails at runtime.
- **napi `binaryName`** determines the per-platform npm package names for all
  seven targets.
- **`scripts/version.mjs`** hardcodes `taskito` coordinates in its regexes. If
  they are not updated, `--check` matches nothing and passes green.
- **`flexiq-core` README doctests** compile under `cargo test` but not under
  `cargo check`.
- **Stale `.venv` wheel** — `uv run` restores a cached wheel unless
  `--reinstall-package` is passed.

## Post-merge, outside the repository

Owner-driven, in order:

1. Rename the GitHub repository `ByteVeda/taskito` → `ByteVeda/flexiq`
   (GitHub preserves redirects for the old URL), then update the local remote.
2. Publish `flexiq-core` to crates.io before its dependents.
3. Publish `flexiq` to PyPI.
4. `npm deprecate @byteveda/taskito "renamed to @byteveda/flexiq"`.
5. Publish the `flexiq` Maven artifact.
6. Add a `docs.byteveda.org/taskito` → `/flexiq` redirect.

## Out of scope

Any API redesign. This is a rename: no symbol is dropped, added, or given new
behaviour beyond its name. Prefix-shortening of types (`FlexiQError` →
`Error`) was considered and rejected as a separate change.
