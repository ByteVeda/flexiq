# FlexiQ Rename Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the project, repository, and every published artifact from Taskito to FlexiQ, with no compatibility shims anywhere.

**Architecture:** The transform is uniform — three case-sensitive substitutions (`taskito`→`flexiq`, `Taskito`→`FlexiQ`, `TASKITO`→`FLEXIQ`) applied to git-tracked text files, plus `git mv` for directories and files whose names carry the product name. Cross-cutting string contracts (env vars, wire markers, HTTP identifiers, runtime paths) are renamed first as their own reviewable commits, because once an area is swept wholesale those changes are no longer separable. Area sweeps follow. Only the branch tip builds: renaming a crate breaks every dependent at once.

**Tech Stack:** Rust (cargo workspace, 9 crates), Python (maturin/uv/pytest), Node (napi-rs/pnpm/vitest), Java (Gradle/JNI), React dashboard (vite/biome), Fumadocs site (pnpm), GitHub Actions, Helm.

**Spec:** `tasks/specs/2026-08-17-flexiq-rename-design.md`

## Global Constraints

- Casing: `taskito`→`flexiq`, `Taskito`→`FlexiQ`, `TASKITO`→`FLEXIQ`. Brand casing is preserved inside identifiers: `TaskitoError`→`FlexiQError`, `InMemoryTaskito`→`InMemoryFlexiQ`.
- No compatibility shims, no dual-name reads, no deprecation fallbacks — anywhere, in any language.
- `tasks/**` and `CHANGELOG.md` retain the old name deliberately and must never be swept.
- Branch `rename/flexiq`, already created off `dev` (which is off `master` at `280f650c`). Spec commit `fa4f7243` is the branch's first commit.
- Version moves `0.23.0` → `1.0.0`, via `node scripts/version.mjs --set 1.0.0`. Never hand-edit a version literal.
- Commits: conventional prefix, subject ≤60 chars imperative, no `Co-Authored-By`, no AI/assistant attribution, no `@` in the subject.
- Git identity is already `Pratyush Sharma <56130065+pratyush618@users.noreply.github.com>`, signing key `~/.ssh/id_ed25519_signing.pub`. The repo-local overrides were unset; do not re-add them.
- Pushing requires the `workflow` scope on the `pratyush618` token. The user must run `gh auth refresh -h github.com -s workflow` before Task 17.
- Pre-commit hooks drop untracked files. `examples/polyglot/java-worker/bin/` is backed up at `<SCRATCH>/untracked-backup/java-worker-bin`. Never bypass hooks with `--no-verify` or `core.hooksPath`.
- `<SCRATCH>` throughout means `/tmp/claude-1000/-home-ezio-Desktop-Work-personal-taskito/1307f590-f895-4e4c-8887-47dbd53a5cec/scratchpad`.

---

## Task 0: Rename helper

**Files:**
- Create: `<SCRATCH>/rename.sh` (throwaway, never committed)

**Interfaces:**
- Produces: `rename.sh [--dry] <pathspec>...` — applies the three substitutions to git-tracked, non-binary files under the given pathspecs, excluding `tasks/**` and `CHANGELOG.md`. Directory arguments are normalised to `dir/**` because a bare directory pathspec silently matches nothing once an exclude pathspec is present.

- [ ] **Step 1: Export the scratch path**

Every later task refers to `"$SCRATCH/rename.sh"`. Export it once per shell session:

```bash
export SCRATCH=/tmp/claude-1000/-home-ezio-Desktop-Work-personal-taskito/1307f590-f895-4e4c-8887-47dbd53a5cec/scratchpad
mkdir -p "$SCRATCH"
```

- [ ] **Step 2: Write the helper**

```bash
cat > "$SCRATCH/rename.sh" <<'EOF'
#!/usr/bin/env bash
# Case-preserving taskito -> flexiq sweep, scoped to git-tracked text files.
# Usage:  rename.sh [--dry] <pathspec> [<pathspec>...]
#   --dry   list the files that would change, do not edit
# Excludes tasks/ and CHANGELOG.md: both deliberately retain the old name.
set -euo pipefail

DRY=0
if [ "${1:-}" = "--dry" ]; then DRY=1; shift; fi
[ $# -gt 0 ] || { echo "usage: rename.sh [--dry] <pathspec>..." >&2; exit 2; }

# A bare directory pathspec silently matches nothing once an exclude pathspec is
# present, so directories are normalised to their `dir/**` glob form.
SPECS=()
for spec in "$@"; do
  if [ -d "$spec" ]; then SPECS+=("${spec%/}/**"); else SPECS+=("$spec"); fi
done

mapfile -d '' FILES < <(
  git ls-files -z "${SPECS[@]}" ':!:tasks/**' ':!:CHANGELOG.md' \
    | xargs -0 -r grep -IlZ -e taskito -e Taskito -e TASKITO
)

if [ ${#FILES[@]} -eq 0 ]; then echo "no matching files"; exit 0; fi

if [ "$DRY" = "1" ]; then
  printf '%s\n' "${FILES[@]}"
  echo "--- ${#FILES[@]} files would change"
  exit 0
fi

printf '%s\0' "${FILES[@]}" | xargs -0 sed -i \
  -e 's/taskito/flexiq/g' \
  -e 's/Taskito/FlexiQ/g' \
  -e 's/TASKITO/FLEXIQ/g'
echo "rewrote ${#FILES[@]} files"
EOF
chmod +x "$SCRATCH/rename.sh"
```

- [ ] **Step 3: Verify it selects the right files and edits nothing**

Run: `"$SCRATCH/rename.sh" --dry crates/taskito-mesh | tail -2`
Expected: a file list ending in `--- 8 files would change`.

Run: `"$SCRATCH/rename.sh" --dry '*' | tail -1`
Expected: `--- 1393 files would change`.

Run: `git status --short`
Expected: only `?? examples/polyglot/java-worker/bin/` — the dry run must not have modified anything.

- [ ] **Step 4: No commit**

The helper is scratch tooling. Nothing to commit in this task.

---

## Task 1: Environment variables

**Files:**
- Modify: every tracked file containing `TASKITO_` — 98 distinct variables across the four SDKs, `crates/taskito-server`, `deploy/helm`, `deploy/keda`, `docker`, tests, and `docs/`.

**Interfaces:**
- Produces: the `FLEXIQ_` env prefix. Every later task assumes no `TASKITO_` variable survives. Examples: `FLEXIQ_DSN`, `FLEXIQ_BACKEND`, `FLEXIQ_DASHBOARD_AUTH`, `FLEXIQ_ATTACH_TOKEN`, `FLEXIQ_POSTGRES_TEST_URL`, `FLEXIQ_REDIS_TEST_URL`.

- [ ] **Step 1: Record the baseline count**

Run: `git grep -o 'TASKITO_[A-Z0-9_]*' | wc -l`
Expected: a non-zero count. Note it; Step 3 asserts it drops to zero.

- [ ] **Step 2: Rewrite the prefix**

```bash
git ls-files -z '*' ':!:tasks/**' ':!:CHANGELOG.md' \
  | xargs -0 -r grep -IlZ 'TASKITO_' \
  | xargs -0 -r sed -i 's/TASKITO_/FLEXIQ_/g'
```

- [ ] **Step 3: Verify no variable survives**

Run: `git grep -c 'TASKITO_' -- ':!:tasks/**' ':!:CHANGELOG.md' | wc -l`
Expected: `0`.

Run: `git grep -oh 'FLEXIQ_[A-Z0-9_]*' -- ':!:tasks/**' ':!:CHANGELOG.md' | sort -u | grep -vc '_$'`
Expected: `98` — the full variable names. Three further tokens end in `_`; those are concatenation prefixes, not variables.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor: rename env vars to FLEXIQ prefix"
```

---

## Task 2: Wire markers and HTTP identifiers

**Files:**
- Modify: `sdks/python/taskito/interception/{converters,reconstruct,walker}.py`, `sdks/python/taskito/proxies/reconstruct.py`, `sdks/python/tests/resources/{test_interception,test_proxies,test_resource_system_full}.py`, `sdks/node/src/workflows/cache.ts`
- Modify: `crates/taskito-server/src/dashboard/{webhook_sender.rs,auth/context.rs,auth/cookies.rs}`, `crates/taskito-server/tests/{webhook_delivery,auth_flow,github_login,oidc_login}.rs`
- Modify: `sdks/java/src/main/java/org/byteveda/taskito/dashboard/auth/{Cookies,TokenAuth}.java`, `sdks/java/src/main/java/org/byteveda/taskito/webhooks/Deliverer.java`, and the Java dashboard tests
- Modify: `sdks/node/src/dashboard/{server.ts,testing.ts,auth/context.ts,auth/tokenAuth.ts}`, `sdks/node/src/webhooks/{deliverer,types}.ts`, `sdks/node/test/dashboard/{oauthEndpoints,sessionAuth}.test.ts`
- Modify: `sdks/python/taskito/dashboard/_testing.py`
- Modify: `dashboard/public/theme-init.js`, `dashboard/src/providers/theme-provider.tsx`
- Modify: the matching `docs/content/docs/**` pages
- Modify: `contracts/wire-vectors.json` (a path reference in its header comment only)

**Interfaces:**
- Produces: payload markers `__flexiq_ref__`, `__flexiq_proxy__`, `__flexiq_convert__`, `__flexiq_redirect__`, `__flexiq_cache__`; headers `X-Flexiq-Signature`, `X-Flexiq-Token` (and their lowercase forms); session cookie `flexiq_session`; dashboard storage key `flexiq.theme`.

- [ ] **Step 1: Rewrite the contract literals**

```bash
git ls-files -z '*' ':!:tasks/**' ':!:CHANGELOG.md' \
  | xargs -0 -r grep -IlZ -e '__taskito_' -e 'X-Taskito-' -e 'x-taskito-' \
                          -e 'taskito_session' -e 'taskito\.theme' \
  | xargs -0 -r sed -i \
      -e 's/__taskito_/__flexiq_/g' \
      -e 's/X-Taskito-/X-Flexiq-/g' \
      -e 's/x-taskito-/x-flexiq-/g' \
      -e 's/taskito_session/flexiq_session/g' \
      -e 's/taskito\.theme/flexiq.theme/g'
```

- [ ] **Step 2: Verify each literal is gone**

Run: `git grep -i -e '__taskito_' -e 'X-Taskito-' -e 'taskito_session' -e 'taskito\.theme' -- ':!:tasks/**' ':!:CHANGELOG.md'`
Expected: no output.

Run: `git grep -c '__flexiq_' -- sdks/python sdks/node | wc -l`
Expected: `8` (seven Python files plus `sdks/node/src/workflows/cache.ts`).

The two User-Agent strings — `taskito-release/1.0` in `.github/workflows/publish.yml` and `"taskito-server"` in the GitHub OAuth provider — are byte-identical to a workflow label and the server binary name, so they are renamed by their own area sweeps (Tasks 11 and 5) rather than here.

- [ ] **Step 3: Update the wire-vector header comment**

`contracts/wire-vectors.json` line 5 references `crates/taskito-core/BINDING_CONTRACT.md`. The vectors themselves encode no product name, so no regeneration is needed — only the path.

```bash
sed -i 's#crates/taskito-core/BINDING_CONTRACT.md#crates/flexiq-core/BINDING_CONTRACT.md#' contracts/wire-vectors.json
```

- [ ] **Step 4: Confirm the vector payloads are untouched**

Run: `git diff --stat contracts/wire-vectors.json`
Expected: `1 file changed, 1 insertion(+), 1 deletion(-)`. Any larger diff means the golden vectors were altered — revert and redo.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "refactor: rename wire and http identifiers"
```

---

## Task 3: Runtime paths and Redis key literals

**Files:**
- Modify: every tracked file containing `taskito.db` (118 occurrences), `.taskito/`, `taskito.sock`, `taskito.log`, or a `"taskito:` Redis key literal.

**Interfaces:**
- Produces: default DB path `~/.flexiq/flexiq.db`, socket `/run/flexiq.sock`, log file `flexiq.log`, Redis lock keys `flexiq:reaper`, `flexiq:retention`, `flexiq:debounce:`.

- [ ] **Step 1: Rewrite the path literals**

```bash
git ls-files -z '*' ':!:tasks/**' ':!:CHANGELOG.md' \
  | xargs -0 -r grep -IlZ -e 'taskito\.db' -e '\.taskito/' -e 'taskito\.sock' \
                          -e 'taskito\.log' -e '"taskito:' \
  | xargs -0 -r sed -i \
      -e 's/taskito\.db/flexiq.db/g' \
      -e 's#\.taskito/#.flexiq/#g' \
      -e 's/taskito\.sock/flexiq.sock/g' \
      -e 's/taskito\.log/flexiq.log/g' \
      -e 's/"taskito:/"flexiq:/g'
```

Note: the Java logger name `org.byteveda.taskito.log` matches the `taskito.log` rule and becomes `org.byteveda.flexiq.log`. That is the correct end state — the Java package rename in Task 8 lands on the same string.

- [ ] **Step 2: Verify**

Run: `git grep -e 'taskito\.db' -e '\.taskito/' -e 'taskito\.sock' -e '"taskito:' -- ':!:tasks/**' ':!:CHANGELOG.md'`
Expected: no output.

Run: `git grep -c 'flexiq\.db' | wc -l`
Expected: a non-zero file count.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: rename runtime paths to flexiq"
```

---

## Task 4: Core crates

**Files:**
- Move: `crates/taskito-core` → `crates/flexiq-core`, `crates/taskito-workflows` → `crates/flexiq-workflows`, `crates/taskito-mesh` → `crates/flexiq-mesh`
- Modify: the three moved crates in full, plus the three `crates/…` entries in the root `Cargo.toml`

**Interfaces:**
- Produces: crates `flexiq-core`, `flexiq-workflows`, `flexiq-mesh`; Rust module paths `flexiq_core::`, `flexiq_workflows::`, `flexiq_mesh::`; error type `FlexiQError`.
- Consumes: nothing. This task does not build — the binding crates still name the old dependencies until Task 5.

- [ ] **Step 1: Move the directories**

```bash
git mv crates/taskito-core      crates/flexiq-core
git mv crates/taskito-workflows crates/flexiq-workflows
git mv crates/taskito-mesh      crates/flexiq-mesh
```

- [ ] **Step 2: Sweep their contents**

```bash
"$SCRATCH/rename.sh" crates/flexiq-core crates/flexiq-workflows crates/flexiq-mesh
```

- [ ] **Step 3: Retarget the three workspace entries in the root manifest**

The root `Cargo.toml` is swept in full by Task 5; here only the three core coordinates move, so the diff stays scoped to this task.

```bash
sed -i \
  -e 's#crates/taskito-core#crates/flexiq-core#g' \
  -e 's#crates/taskito-workflows#crates/flexiq-workflows#g' \
  -e 's#crates/taskito-mesh#crates/flexiq-mesh#g' \
  -e 's/^taskito-core = /flexiq-core = /' \
  -e 's/^taskito-workflows = /flexiq-workflows = /' \
  -e 's/^taskito-mesh = /flexiq-mesh = /' \
  Cargo.toml
```

- [ ] **Step 4: Verify the manifests name themselves correctly**

Run: `grep -h '^name' crates/flexiq-core/Cargo.toml crates/flexiq-workflows/Cargo.toml crates/flexiq-mesh/Cargo.toml`
Expected:
```
name = "flexiq-core"
name = "flexiq-workflows"
name = "flexiq-mesh"
```

Run: `grep -n 'flexiq-core = \|members =' Cargo.toml`
Expected: `members` lists `crates/flexiq-core`, `crates/flexiq-workflows`, `crates/flexiq-mesh` alongside the still-unrenamed binding crates, and the dependency line reads `flexiq-core = { path = "crates/flexiq-core", version = "0.23.0" }`.

- [ ] **Step 5: Commit (build is expected to be broken)**

```bash
git add -A
git commit -m "refactor: rename core crates to flexiq"
```

Do not run `cargo check` here — the binding crates still depend on `taskito-core`, which no longer exists. Task 5 restores a compiling workspace.

---

## Task 5: Binding crates

**Files:**
- Move: `crates/taskito-python` → `crates/flexiq-python`, `crates/taskito-node` → `crates/flexiq-node`, `crates/taskito-java` → `crates/flexiq-java`, `crates/taskito-server` → `crates/flexiq-server`, `crates/taskito-tui` → `crates/flexiq-tui`, `crates/taskito` → `crates/flexiq`
- Modify: all six moved crates, root `Cargo.toml`, `Cargo.lock`

**Interfaces:**
- Consumes: `flexiq-core`, `flexiq-workflows`, `flexiq-mesh` from Task 4.
- Produces: PyO3 extension module `_flexiq` (`[lib] name = "_flexiq"` in `crates/flexiq-python/Cargo.toml`); server lib `flexiq_server` and binary `flexiq-server`; TUI binary `flexiq-tui`; JNI symbols `Java_org_byteveda_flexiq_*`.

- [ ] **Step 1: Move the directories**

```bash
git mv crates/taskito-python crates/flexiq-python
git mv crates/taskito-node   crates/flexiq-node
git mv crates/taskito-java   crates/flexiq-java
git mv crates/taskito-server crates/flexiq-server
git mv crates/taskito-tui    crates/flexiq-tui
git mv crates/taskito        crates/flexiq
```

- [ ] **Step 2: Sweep the crates and the root manifest**

```bash
"$SCRATCH/rename.sh" crates Cargo.toml
```

- [ ] **Step 3: Verify the JNI symbols moved with the package**

Run: `grep -rho 'Java_org_byteveda_[a-z]*_' crates/flexiq-java/src | sort -u`
Expected: `Java_org_byteveda_flexiq_` and nothing else. A surviving `Java_org_byteveda_taskito_` here produces a runtime `UnsatisfiedLinkError` that no compiler catches.

- [ ] **Step 4: Verify the extension module name**

Run: `grep -A2 '^\[lib\]' crates/flexiq-python/Cargo.toml`
Expected: `name = "_flexiq"`.

- [ ] **Step 5: Build the workspace**

Run: `cargo check --workspace`
Expected: success. `Cargo.lock` is rewritten with the new crate names.

- [ ] **Step 6: Build every feature combination**

Run each and expect success:
```bash
cargo check --workspace --features postgres
cargo check --workspace --features redis
cargo check --workspace --features native-async
```

- [ ] **Step 7: Run the Rust tests**

Run: `cargo test --workspace`
Expected: 72 tests pass.

Run: `cargo test --workspace --features workflows`
Expected: 110 tests pass.

The `flexiq-core` README doctests compile only under `cargo test`, not `cargo check` — this step is what proves they were renamed correctly.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "refactor: rename binding crates to flexiq"
```

---

## Task 6: Python SDK

**Files:**
- Move: `sdks/python/taskito` → `sdks/python/flexiq`
- Move: `sdks/python/flexiq/_taskito.pyi` → `sdks/python/flexiq/_flexiq.pyi`
- Move: `sdks/python/flexiq/contrib/django/templates/taskito` → `.../templates/flexiq`
- Move: `sdks/python/flexiq/contrib/django/templatetags/taskito_admin.py` → `.../flexiq_admin.py`
- Move: `sdks/python/flexiq/contrib/django/management/commands/taskito_{dashboard,worker,info}.py` → `flexiq_{dashboard,worker,info}.py`
- Modify: `sdks/python/pyproject.toml`, all of `sdks/python/`, `.pre-commit-config.yaml`
- Delete: the stale compiled extension `sdks/python/flexiq/_taskito.cpython-*.so`

**Interfaces:**
- Consumes: `crates/flexiq-python` with `[lib] name = "_flexiq"` from Task 5.
- Produces: distribution `flexiq`, import root `flexiq`, native module `flexiq._flexiq`, console script `flexiq`, Django template tag library `flexiq_admin`, management commands `flexiq_worker` / `flexiq_dashboard` / `flexiq_info`.

- [ ] **Step 1: Move the package and its name-carrying files**

```bash
git mv sdks/python/taskito sdks/python/flexiq
git mv sdks/python/flexiq/_taskito.pyi sdks/python/flexiq/_flexiq.pyi
git mv sdks/python/flexiq/contrib/django/templates/taskito \
       sdks/python/flexiq/contrib/django/templates/flexiq
git mv sdks/python/flexiq/contrib/django/templatetags/taskito_admin.py \
       sdks/python/flexiq/contrib/django/templatetags/flexiq_admin.py
for cmd in dashboard worker info; do
  git mv "sdks/python/flexiq/contrib/django/management/commands/taskito_$cmd.py" \
         "sdks/python/flexiq/contrib/django/management/commands/flexiq_$cmd.py"
done
```

- [ ] **Step 2: Sweep the SDK and the hook config**

```bash
"$SCRATCH/rename.sh" sdks/python .pre-commit-config.yaml
```

- [ ] **Step 3: Verify the maturin module name matches the package directory**

Run: `grep -nE 'name = "flexiq"|module-name|^flexiq = |manifest-path' sdks/python/pyproject.toml`
Expected: `name = "flexiq"`, `manifest-path = "../../crates/flexiq-python/Cargo.toml"`, `module-name = "flexiq._flexiq"`, and under `[project.scripts]` a `flexiq = "flexiq.cli:main"` entry. A mismatch between `module-name` and the on-disk directory fails only at import time.

- [ ] **Step 4: Verify the Django string-matched references**

Run: `grep -rn 'load flexiq_admin' sdks/python/flexiq/contrib/django/templates/`
Expected: every template that previously loaded `taskito_admin` now loads `flexiq_admin`.

Run: `git grep -n 'templates/taskito\|taskito_admin\|taskito_worker' -- sdks/python`
Expected: no output.

- [ ] **Step 5: Drop stale build products**

```bash
rm -f sdks/python/flexiq/_taskito.cpython-*.so
find sdks/python -name '__pycache__' -prune -exec rm -rf {} + 2>/dev/null || true
rm -rf sdks/python/.mypy_cache .mypy_cache
```

- [ ] **Step 6: Rebuild the wheel**

```bash
cd sdks/python
uv sync --extra dev --extra oauth
uv run maturin develop --reinstall-package flexiq
```

Expected: maturin reports building `flexiq`. Never plain `uv sync` — the extras are required.

- [ ] **Step 7: Verify the extension imports**

Run: `cd sdks/python && uv run python -c "import flexiq, flexiq._flexiq; print(flexiq.__version__)"`
Expected: `0.23.0`. An `ImportError` here means `module-name` and the package directory disagree.

- [ ] **Step 8: Run the Python suite and linters**

Run: `cd sdks/python && uv run python -m pytest tests/ -q`
Expected: 1007 passed.

Run: `cd sdks/python && uv run ruff check flexiq/ tests/`
Expected: `All checks passed!`

Run: `cd sdks/python && uv run mypy flexiq/ tests/ --no-incremental`
Expected: no errors.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor: rename python sdk to flexiq"
```

---

## Task 7: Node SDK

**Files:**
- Modify: `sdks/node/package.json`, all of `sdks/node/src`, `sdks/node/test`, `sdks/node/README.md`, `sdks/node/tsup.config.ts`
- Delete: `sdks/node/native/taskito.*.node`, `sdks/node/dist`

**Interfaces:**
- Consumes: `crates/flexiq-node` from Task 5.
- Produces: npm package `@byteveda/flexiq`, console binary `flexiq`, napi `binaryName: "flexiq"`, native artifacts `flexiq.<target>.node` across all seven targets.

- [ ] **Step 1: Sweep the SDK**

```bash
"$SCRATCH/rename.sh" sdks/node
```

- [ ] **Step 2: Verify the package identity and napi binary name**

Run: `grep -nE '"name"|"bin"|binaryName|manifest-path' sdks/node/package.json`
Expected: `"name": "@byteveda/flexiq"`, a `bin` entry mapping `flexiq`, `"binaryName": "flexiq"`, and the native build script pointing at `../../crates/flexiq-node/Cargo.toml`. `binaryName` determines the per-platform npm package names for all seven targets.

- [ ] **Step 3: Drop stale native artifacts**

```bash
rm -f sdks/node/native/taskito.*.node
rm -rf sdks/node/dist
```

- [ ] **Step 4: Build**

```bash
cd sdks/node
pnpm install --frozen-lockfile
pnpm run build:native
pnpm run build
```

Expected: the native artifact is written as `native/flexiq.<target>.node`.

- [ ] **Step 5: Test and lint**

Run: `cd sdks/node && pnpm test`
Expected: all tests pass.

Run: `cd sdks/node && pnpm exec biome check src test`
Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: rename node sdk to flexiq"
```

---

## Task 8: Java SDK

**Files:**
- Move: `sdks/java/src/main/java/org/byteveda/taskito` → `.../flexiq`; the same rename under `src/main/java22`, `src/test/java`, and `src/main/resources/org/byteveda`
- Move: `sdks/java/src/main/resources/META-INF/native-image/org.byteveda/taskito` → `.../flexiq`
- Modify: all of `sdks/java`, including `settings.gradle.kts` and `build.gradle.kts`
- Delete: `sdks/java/build`

**Interfaces:**
- Consumes: JNI symbols `Java_org_byteveda_flexiq_*` from Task 5.
- Produces: Java package `org.byteveda.flexiq.*`, Maven coordinates `org.byteveda:flexiq`, `rootProject.name = "flexiq"`, dashboard resources under `/org/byteveda/flexiq/dashboard/`.

- [ ] **Step 1: Move the package directories**

```bash
git mv sdks/java/src/main/java/org/byteveda/taskito      sdks/java/src/main/java/org/byteveda/flexiq
git mv sdks/java/src/main/java22/org/byteveda/taskito    sdks/java/src/main/java22/org/byteveda/flexiq
git mv sdks/java/src/test/java/org/byteveda/taskito      sdks/java/src/test/java/org/byteveda/flexiq
git mv sdks/java/src/main/resources/org/byteveda/taskito sdks/java/src/main/resources/org/byteveda/flexiq
git mv sdks/java/src/main/resources/META-INF/native-image/org.byteveda/taskito \
       sdks/java/src/main/resources/META-INF/native-image/org.byteveda/flexiq
```

- [ ] **Step 2: Sweep the SDK**

```bash
"$SCRATCH/rename.sh" sdks/java
```

- [ ] **Step 3: Verify package declarations and coordinates**

Run: `grep -rho '^package org\.[a-z.]*' sdks/java/src | sort -u | head -3`
Expected: every line begins `package org.byteveda.flexiq`.

Run: `grep -n 'rootProject.name' sdks/java/settings.gradle.kts; grep -n 'coordinates(' sdks/java/build.gradle.kts`
Expected: `rootProject.name = "flexiq"` and `coordinates(group.toString(), "flexiq", version.toString())`.

- [ ] **Step 4: Verify the string-matched resource paths**

Run: `git grep -n 'org/byteveda/taskito\|org\.byteveda\.taskito' -- sdks/java crates/flexiq-java`
Expected: no output. `getResourceAsStream("/org/byteveda/flexiq/dashboard/…")` is a runtime lookup; a stale path fails only when the dashboard is served.

- [ ] **Step 5: Drop stale build output**

```bash
rm -rf sdks/java/build sdks/java/.gradle
```

- [ ] **Step 6: Build and test**

Run: `cd sdks/java && ./gradlew build test`
Expected: build succeeds, tests pass. `WorkflowCacheTest` failing on a 20-second await deadline under JDK 25 is a known flake, not a rename fault — rerun it alone to confirm.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "refactor: rename java sdk to flexiq"
```

---

## Task 9: Dashboard

**Files:**
- Modify: `dashboard/src`, `dashboard/public`, `dashboard/package.json`, `dashboard/index.html`

**Interfaces:**
- Consumes: storage key `flexiq.theme` from Task 2 (already applied there).
- Produces: dashboard title and package name under the FlexiQ brand.

- [ ] **Step 1: Sweep**

```bash
"$SCRATCH/rename.sh" dashboard
```

- [ ] **Step 2: Lint, typecheck, test**

Run: `cd dashboard && pnpm install --frozen-lockfile && pnpm exec biome check src`
Expected: no errors.

Run: `cd dashboard && pnpm run typecheck`
Expected: no errors.

Run: `cd dashboard && pnpm test`
Expected: all tests pass. A dashboard build break also reds the `Server image` job and the Node jobs, so this gate must be green before moving on.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: rename dashboard to flexiq"
```

---

## Task 10: Examples, contracts, scripts

**Files:**
- Move: `examples/polyglot/java-worker/build.gradle.kts` artifact name and `examples/polyglot/*/` manifests (in-file only; no directory carries the product name)
- Modify: `examples/polyglot`, `scripts/polyglot_e2e.py`, `scripts/sync-changelog.mjs`, `contracts/`

**Interfaces:**
- Consumes: the renamed SDK package names from Tasks 6-8 — `flexiq` on PyPI, `@byteveda/flexiq` on npm, `org.byteveda:flexiq` on Maven.
- Produces: a polyglot example that installs the renamed packages.

- [ ] **Step 1: Sweep**

```bash
"$SCRATCH/rename.sh" examples scripts contracts
```

- [ ] **Step 2: Verify the example manifests name the new packages**

Run: `grep -rn 'flexiq' examples/polyglot/node-worker/package.json examples/polyglot/java-worker/build.gradle.kts`
Expected: `@byteveda/flexiq` in the npm manifest and `org.byteveda:flexiq` in the Gradle build.

- [ ] **Step 3: Verify the e2e driver**

Run: `python -m py_compile scripts/polyglot_e2e.py`
Expected: no output (compiles clean).

Run: `git grep -n taskito -- examples scripts contracts`
Expected: no output.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor: rename examples and scripts to flexiq"
```

---

## Task 11: CI workflows and composite actions

**Files:**
- Modify: `.github/workflows/*.yml` (21 files), `.github/actions/*/action.yml` (8 composite actions), `.github/labeler.yml`

**Interfaces:**
- Consumes: the crate and SDK paths established in Tasks 4-8.
- Produces: path filters and job definitions that match the renamed tree.

- [ ] **Step 1: Sweep**

```bash
"$SCRATCH/rename.sh" .github
```

- [ ] **Step 2: Verify every path filter resolves to a directory that exists**

Run:
```bash
grep -rhoE "'(crates|sdks|deploy|dashboard|docs)/[^']*'" .github/workflows .github/actions \
  | tr -d "'" | sed 's/\*.*//' | sort -u \
  | while read -r p; do [ -e "${p%/}" ] || echo "MISSING: $p"; done
```
Expected: no `MISSING:` lines. A stale filter such as `crates/taskito-server/**` does not fail — the job silently stops running.

- [ ] **Step 3: Lint the workflows**

Run: `actionlint` if installed, otherwise `python -c "import yaml,glob,sys; [yaml.safe_load(open(f)) for f in glob.glob('.github/workflows/*.yml')]"`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "ci: rename workflows and actions for flexiq"
```

---

## Task 12: Deploy manifests and Docker

**Files:**
- Move: `deploy/helm/taskito-server` → `deploy/helm/flexiq-server`
- Modify: `deploy/helm/flexiq-server/**`, `deploy/keda/*.yaml`, `docker/scheduler.Dockerfile`, `docker/scheduler.Dockerfile.dockerignore`

**Interfaces:**
- Consumes: binary name `flexiq-server` from Task 5 and the `FLEXIQ_` env prefix from Task 1.
- Produces: Helm chart `flexiq-server`.

- [ ] **Step 1: Move the chart**

```bash
git mv deploy/helm/taskito-server deploy/helm/flexiq-server
```

- [ ] **Step 2: Sweep**

```bash
"$SCRATCH/rename.sh" deploy docker
```

- [ ] **Step 3: Verify the chart**

Run: `grep -n '^name:\|^version:\|^appVersion:' deploy/helm/flexiq-server/Chart.yaml`
Expected: `name: flexiq-server`, `version: 0.23.0`, `appVersion: "0.23.0"` — the version moves in Task 14, not here.

Run: `helm lint deploy/helm/flexiq-server` if `helm` is installed
Expected: no failures. If `helm` is unavailable, note it and rely on the `ci-chart.yml` job.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "chore: rename deploy manifests to flexiq"
```

---

## Task 13: Documentation

**Files:**
- Modify: `docs/**` (content, app, public, scripts, package.json), `README.md`, `ARCHITECTURE.md`, `CONTRIBUTING.md`, and every remaining tracked file still holding the old name

**Interfaces:**
- Consumes: every name established in Tasks 1-12.
- Produces: documentation whose install snippets, env tables, and code samples all name FlexiQ.

- [ ] **Step 1: Sweep the docs and root files**

```bash
"$SCRATCH/rename.sh" docs README.md ARCHITECTURE.md CONTRIBUTING.md
```

- [ ] **Step 2: Sweep whatever remains**

```bash
"$SCRATCH/rename.sh" --dry '*'
```
Expected: `no matching files`. If files are listed, sweep them:
```bash
"$SCRATCH/rename.sh" '*'
```

- [ ] **Step 3: Verify the tree is clean**

Run: `git grep -i taskito -- ':!:tasks/**' ':!:CHANGELOG.md'`
Expected: no output.

- [ ] **Step 4: Build the docs site**

Run: `pnpm --dir docs install --frozen-lockfile && pnpm --dir docs types:check && pnpm --dir docs lint && pnpm --dir docs build`
Expected: all four succeed. If the build runs out of memory, raise the Node heap: `NODE_OPTIONS=--max-old-space-size=8192 pnpm --dir docs build`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: rename taskito to flexiq"
```

---

## Task 14: Version script and 1.0.0 bump

**Files:**
- Modify: `scripts/version.mjs` (already swept in Task 10 — this task verifies and then bumps)
- Modify: everything `version.mjs --set` rewrites

**Interfaces:**
- Consumes: the renamed coordinates from every prior task.
- Produces: version `1.0.0` across the workspace, the npm manifest, `gradle.properties`, `sdks/python/flexiq/__init__.py`, and the Helm chart.

- [ ] **Step 1: Verify the mirrors and snippet patterns point at real paths**

Run: `grep -n 'file:\|flexiq' scripts/version.mjs | head -40`
Expected: mirrors reference `crates/flexiq-*` coordinates, `sdks/python/flexiq/__init__.py`, and `deploy/helm/flexiq-server/Chart.yaml`; the snippet regexes match `org\.byteveda:flexiq`, `<artifactId>flexiq`, `@byteveda\/flexiq`, and `\bflexiq[\w-]*==`. Every regex that still says `taskito` would match nothing and let `--check` pass green on a drifted repo.

- [ ] **Step 2: Confirm the current state validates**

Run: `node scripts/version.mjs --check`
Expected: passes, reporting `0.23.0`.

- [ ] **Step 3: Bump**

Run: `node scripts/version.mjs --set 1.0.0`
Expected: reports each rewritten mirror.

- [ ] **Step 4: Add the CHANGELOG section the check requires**

`version.mjs` fails if the newest `CHANGELOG.md` section does not match the declared version. Add a `## 1.0.0` heading at the top of the existing sections with a one-line entry; the full release notes land in Task 15.

- [ ] **Step 5: Verify**

Run: `node scripts/version.mjs --check`
Expected: passes, reporting `1.0.0`.

Run: `cargo check --workspace`
Expected: success — `Cargo.lock` picks up the new version.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "chore: bump version to 1.0.0"
```

---

## Task 15: Migration guide and release notes

**Files:**
- Create: `docs/content/docs/shared/guides/migrating-to-flexiq.mdx`
- Modify: `CHANGELOG.md`, and the docs navigation entry that lists shared guides

**Interfaces:**
- Consumes: every renamed surface, as the guide enumerates them.
- Produces: the user-facing migration path for the clean break.

- [ ] **Step 1: Write the guide**

It must cover, each with a before/after example:
1. Package installs — `pip install flexiq`, `npm i @byteveda/flexiq`, `org.byteveda:flexiq`, `cargo add flexiq`.
2. Imports — `import taskito` → `import flexiq`; `org.byteveda.taskito.*` → `org.byteveda.flexiq.*`; `use taskito_core::` → `use flexiq_core::`.
3. Environment variables — the `TASKITO_` → `FLEXIQ_` prefix, noting there is no fallback.
4. **Drain requirement** — jobs enqueued by 0.23.x carry `__taskito_*` payload markers that 1.0.0 does not understand. Drain the queue to empty before upgrading; do not run mixed-version workers against one database.
5. Data path — move `~/.taskito/taskito.db` to `~/.flexiq/flexiq.db` (or set `FLEXIQ_DB_PATH`).
6. Webhook receivers — `X-Taskito-Signature` becomes `X-Flexiq-Signature`.
7. Dashboard sessions — the cookie changed name, so every session is invalidated and users re-login once.
8. Django — `manage.py taskito_worker` becomes `manage.py flexiq_worker`; templates load `flexiq_admin`.
9. Helm — the chart is `flexiq-server`; release names and value keys move with it.

- [ ] **Step 2: Expand the CHANGELOG entry**

Replace the placeholder line added in Task 14 with a `## 1.0.0` section stating the rename, the clean break, and a link to the migration guide. Keep every historical section's old name intact.

- [ ] **Step 3: Verify the docs still build**

Run: `pnpm --dir docs types:check && pnpm --dir docs build`
Expected: both succeed and the new page appears in the generated manifest.

Run: `node scripts/version.mjs --check`
Expected: passes — the CHANGELOG edit must keep `## 1.0.0` as the newest section.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "docs: add FlexiQ migration guide"
```

---

## Task 16: Project memory and skills (untracked)

**Files:**
- Modify: `.claude/memory/*.md`, `.claude/skills/**`, `.claude/agents/**`, `.claude/commands/**`, `.claude/soul.md`, `CLAUDE.md`

`.gitignore` lines 18-19 exclude `.claude/` and `CLAUDE.md`, so these files are invisible to git. The helper only walks tracked files and would report `no matching files`; this task edits the filesystem directly and produces **no commit**.

**Interfaces:**
- Consumes: the final layout from every prior task.
- Produces: agent-facing docs that name the renamed paths and commands.

- [ ] **Step 1: Sweep the untracked agent docs directly**

```bash
grep -rIl -e taskito -e Taskito -e TASKITO .claude CLAUDE.md \
  | xargs -r sed -i \
      -e 's/taskito/flexiq/g' \
      -e 's/Taskito/FlexiQ/g' \
      -e 's/TASKITO/FLEXIQ/g'
```

- [ ] **Step 2: Correct the paths the sweep cannot infer**

Read `CLAUDE.md` and each file under `.claude/skills/` and confirm every path it names exists:

Run:
```bash
grep -rhoE '(crates|sdks|dashboard|docs|deploy)/[A-Za-z0-9_./-]+' CLAUDE.md .claude \
  | sed 's/[.,)]$//' | sort -u \
  | while read -r p; do [ -e "$p" ] || echo "MISSING: $p"; done
```
Expected: no `MISSING:` lines. Fix any that appear.

- [ ] **Step 3: Update the counts and commands that changed**

In `CLAUDE.md`: the build commands now read `uv run ruff check flexiq/`, `uv run mypy flexiq/ --no-incremental`, and the layout section names `sdks/python/flexiq/`, `crates/flexiq-core/src/`, `crates/flexiq-python/src/`, `sdks/python/flexiq/_flexiq.pyi`.

- [ ] **Step 4: Confirm nothing entered the index**

Run: `git status --short`
Expected: only `?? examples/polyglot/java-worker/bin/`. These files are gitignored — if any `.claude/` path appears as staged or untracked, stop: `.gitignore` was altered by an earlier sweep and must be restored.

---

## Task 17: Full verification, push, PR

**Files:** none modified — this task only verifies and publishes.

**Interfaces:**
- Consumes: the complete branch.
- Produces: `origin/dev`, `origin/rename/flexiq`, and the PR.

- [ ] **Step 1: Confirm no occurrence survives**

Run: `git grep -i taskito -- ':!:tasks/**' ':!:CHANGELOG.md'`
Expected: no output.

Run: `git grep -ci taskito -- CHANGELOG.md`
Expected: non-zero — release history keeps the old name on purpose.

Run: `git ls-files | grep -i taskito`
Expected: no output — no tracked path still carries the old name.

- [ ] **Step 2: Full Rust gate**

```bash
cargo check --workspace
cargo check --workspace --features postgres
cargo check --workspace --features redis
cargo check --workspace --features native-async
cargo test --workspace
cargo test --workspace --features workflows
```
Expected: all succeed; 72 and 110 tests respectively.

- [ ] **Step 3: pyo3 leakage tripwire**

```bash
for c in flexiq-core flexiq-workflows flexiq-mesh; do
  echo "== $c"; cargo tree -p $c --all-features | grep pyo3 || echo "clean"
done
```
Expected: `clean` for all three.

- [ ] **Step 4: Publish dry-run**

```bash
cargo publish --dry-run -p flexiq-core --allow-dirty
```
Expected: packaging succeeds. `flexiq-core` has no crates.io baseline, so `cargo-semver-checks` has nothing to compare — that is expected for a newly named crate, not a failure.

- [ ] **Step 5: Full SDK gate**

```bash
cd sdks/python && uv run python -m pytest tests/ -q && uv run ruff check flexiq/ tests/ && uv run mypy flexiq/ tests/ --no-incremental
cd sdks/node   && pnpm test
cd sdks/java   && ./gradlew build test
cd dashboard   && pnpm run typecheck && pnpm test
pnpm --dir docs types:check && pnpm --dir docs lint && pnpm --dir docs build
```
Expected: 1007 Python tests pass; every other gate green.

- [ ] **Step 6: Version gate**

Run: `node scripts/version.mjs --check`
Expected: passes, reporting `1.0.0`.

- [ ] **Step 7: Confirm the token scope, then push**

The `pratyush618` token needs the `workflow` scope or the push of `.github/workflows/**` is rejected. Ask the user to run `gh auth refresh -h github.com -s workflow` and confirm before continuing.

```bash
git push -u origin dev
git push -u origin rename/flexiq
```

- [ ] **Step 8: Open the PR against dev**

```bash
gh pr create --base dev --head rename/flexiq \
  --title "refactor: rename Taskito to FlexiQ" \
  --body-file <(cat <<'EOF'
Renames the project, repository, and every published artifact from Taskito to FlexiQ.

Clean break: no compatibility shims. Published package names change on all four registries, the `TASKITO_` env prefix becomes `FLEXIQ_`, and payload markers, HTTP headers, the session cookie, and the default data path all move.

Commits are ordered so the cross-cutting string contracts (env vars, wire identifiers, runtime paths) are reviewable on their own before the per-area sweeps. Only the branch tip builds — renaming a crate breaks every dependent at once.

Version moves to 1.0.0. Migration guide: `docs/content/docs/shared/guides/migrating-to-flexiq.mdx`.
EOF
)
```

- [ ] **Step 9: Restore the untracked backup**

```bash
cp -r "$SCRATCH/untracked-backup/java-worker-bin" examples/polyglot/java-worker/bin
```
Expected: `git status --short` shows only `?? examples/polyglot/java-worker/bin/`, as at the start.

---

## Deviation from the spec

The spec listed 14 commits with env vars, wire identifiers, and runtime paths as commits 6-8, after the area sweeps. That ordering cannot produce those commits: a per-area sweep renames everything in its path at once, so by commit 6 there would be nothing left to change. They are moved to Tasks 1-3, ahead of the area sweeps, which preserves the spec's intent — each breaking contract gets its own reviewable commit — and makes each later sweep smaller. The examples, CI, and deploy sweeps are also split into their own commits rather than folded together.

The spec's fourteenth commit, "update claude memory and skills", does not exist: `.gitignore` excludes `.claude/` and `CLAUDE.md`, so Task 16 edits them without committing. The branch therefore carries 16 commits — the spec commit plus the fifteen rename commits of Tasks 1-15.

## Post-merge, outside the repository

Owner-driven, in order, as recorded in the spec:

1. Rename the GitHub repository `ByteVeda/taskito` → `ByteVeda/flexiq`, then update the local remote.
2. Publish `flexiq-core` to crates.io before its dependents.
3. Publish `flexiq` to PyPI.
4. `npm deprecate @byteveda/taskito "renamed to @byteveda/flexiq"`.
5. Publish the `flexiq` Maven artifact.
6. Add a `docs.byteveda.org/taskito` → `/flexiq` redirect.
