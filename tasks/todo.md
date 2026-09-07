# Security alert cleanup — `fix/security-alerts`

Branch off `master` at `e3264d4f`. Closes the open Dependabot + CodeQL alerts:
16 Dependabot, 49 CodeQL.

## Findings after verifying against library sources

Both scanner headlines turned out to be weaker than they read, and the thing
worth fixing was not flagged as severe by either.

- [x] **CVE-2026-25537 (`jsonwebtoken` < 10.3.0) is NOT exploitable here.** The
      advisory is `exp`/`nbf` type confusion: a claim sent with the wrong JSON
      type parses to `FailedToParse`, which `validate()` treats as absent. The
      gate is `required_spec_claims` — and `Validation::new` seeds it with
      `exp`, which `oidc.rs` never overrode. Bumped anyway: real CVE, runtime
      dependency, and an alert nobody can action rots.
- [x] **No OIDC algorithm-confusion bypass either.** `jsonwebtoken` 9.3.1's
      `verify_signature` already rejected `key.family != alg.family()`, so an
      `HS256` token forged against a published RSA JWK could not verify. But
      the algorithm still came out of the token's own header, which is one
      library refactor away from mattering — now pinned to the key.
- [x] **31/31 `rust/hard-coded-cryptographic-value` (critical) are test-only.**
      All sit past the `#[cfg(test)]` line of their file, or in `tests/`.
- [x] **`rust/cleartext-logging` ×4 and `rust/access-invalid-pointer` ×4 are
      false positives.** The former log a username or a `job_id`; the latter all
      land on a `#[napi] pub struct` line, i.e. napi-rs macro output.
- [x] **Real: log injection ×4** — 3 in `oauth/mod.rs`, 1 in `scaler.py`.

## Tasks

- [x] 1. `jsonwebtoken` 9 → 10.3, `rust_crypto` provider (pure Rust, so the
      multi-arch server image needs no C toolchain). `use_pem` moved to
      dev-dependencies — the stub issuer signs from PEM, the binary never does.
- [x] 2. Pin `id_token` verification to the key's algorithm, require
      `exp`/`iss`/`aud`/`sub`, validate `nbf`. Unit tests for the JWK→algorithm
      rules, plus an end-to-end forgery case in `oidc_login.rs`.
- [x] 3. `log_safe::escape` + the four OAuth log sites; the two that logged a
      request-supplied `slot` now log the config-owned `provider.slot`.
- [x] 4. `_log_safe` in `scaler.py` + 4 tests in `test_keda.py`.
- [x] 5. Lockfile bumps: `fast-uri`, `postcss`, `browserslist`,
      `brace-expansion`, `js-yaml` in range; `toml` and `esbuild` needed
      `pnpm.overrides` (both cross a major/0.x boundary).
- [x] 6. CodeQL `paths-ignore` for test trees. The 29 in-`src` `#[cfg(test)]`
      alerts cannot be matched by path and need dismissing alert-by-alert.
- [x] 7. Verify.
- [ ] 8. Open the PR.

One compile job at a time — 13 GB RAM, no concurrent cargo processes.

## Review

Verification run, all green:

| Gate | Result |
|------|--------|
| `cargo test -p flexiq-server` | 350 passed, 20 binaries, exit 0 |
| `cargo clippy -p flexiq-server --all-targets -- -D warnings` | clean |
| `cargo fmt --all --check` | clean |
| `pytest tests/` (python) | see below |
| `ruff check` + `ruff format` (flexiq/ + tests/) | 346 files clean |
| `mypy flexiq/ tests/` | 346 files, no issues |
| node `build:ts` + `vitest` | 779 passed, 6 skipped |
| node `biome ci` + `tsc --noEmit` | clean |
| dashboard `pnpm ci` | 161 passed + build |
| docs `typecheck` + `build` | clean, prerender OK |

The npm overrides are the part worth re-reading at review time: `toml@4.3.0`
and `esbuild@0.28.2` are both forced past a boundary their parents did not ask
for, so the builds passing is the only thing standing behind them.
