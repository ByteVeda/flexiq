# Contributing to flexiq

Thanks for your interest in contributing! flexiq is a hybrid Rust + Python project, so the dev setup involves both ecosystems.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Taking part in the issue
tracker, pull requests, or any other project space means agreeing to it. Report unacceptable
behaviour to conduct@byteveda.org.

## Development Setup

### Prerequisites

- Python 3.9+
- Rust (stable) — install via [rustup](https://rustup.rs/)
- [maturin](https://github.com/PyO3/maturin) — builds the Rust extension
- Go 1.25+ — only for `sdks/go`, which builds nothing from `crates/`. The `go-fmt` and `go-vet`
  pre-commit hooks need it on `PATH` when a `.go` file is staged.

### Clone and Install

```bash
git clone https://github.com/ByteVeda/flexiq.git
cd flexiq/sdks/python   # the Python SDK lives here; node/, java/ and go/ are peers

# Create a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install in development mode (compiles Rust + installs Python package)
pip install maturin
maturin develop

# Install dev dependencies
pip install -e ".[dev]"
```

### Rebuilding After Rust Changes

If you modify any `.rs` file, re-run:

```bash
maturin develop
```

Python-only changes don't require a rebuild.

## Running Tests

### Python Tests

```bash
pytest tests/
```

### Rust Tests

```bash
cargo test --manifest-path crates/flexiq-core/Cargo.toml
```

## Code Style

### Python

flexiq uses [ruff](https://github.com/astral-sh/ruff) for linting and formatting:

```bash
# Lint
ruff check flexiq/

# Format
ruff format flexiq/

# Auto-fix
ruff check --fix flexiq/
```

Type checking with [mypy](https://mypy-lang.org/):

```bash
mypy flexiq/
```

### Rust

```bash
cargo fmt --manifest-path crates/flexiq-core/Cargo.toml
cargo clippy --manifest-path crates/flexiq-core/Cargo.toml
```

## Making Changes

New to the code? Read [`ARCHITECTURE.md`](ARCHITECTURE.md) first — it maps the
layers, boundary rules, and the ordered touch-points for common changes (adding a
storage method, a Queue method, a contrib integration).

1. Fork the repo and create a branch from `master`
2. Make your changes
3. Add or update tests as needed
4. Run `ruff check`, `mypy`, and `pytest` to verify
5. Run `cargo test` and `cargo clippy` if you changed Rust code
6. Open a pull request against `master`

## Documentation

Docs are a [Fumadocs](https://fumadocs.dev) site (Next.js + MDX) under `docs/`. To preview locally:

```bash
pnpm --dir docs install
pnpm --dir docs dev
```

Then open http://localhost:3000. To validate before opening a PR:

```bash
pnpm --dir docs typecheck
pnpm --dir docs lint
pnpm --dir docs build
```

## Releasing

All SDKs ship in lock-step off one version, held in `[workspace.package]` of the root
`Cargo.toml`. Never hand-edit a version literal — `node scripts/version.mjs --set X.Y.Z` rewrites
the source and every mirror, and `--check` gates CI.

The standard publishing path is tag-driven: five tag namespaces, one per registry, each with its
own workflow. Every one of them also accepts `workflow_dispatch` with a `version` input, which is
the fallback when a tag has already been cut or a single registry needs a retry — crates.io 1.0.0
went up that way.

| Registry | Tag | Workflow |
| --- | --- | --- |
| PyPI | `X.Y.Z` | `publish-py.yml` |
| crates.io | `crates-vX.Y.Z` | `publish-crates.yml` |
| npm | `node-vX.Y.Z` | `publish-node.yml` |
| Maven Central | `java-vX.Y.Z` | `publish-java.yml` |
| GHCR (server image and Helm chart) | `server-vX.Y.Z` | `publish-server.yml` |
| Go module proxy | `sdks/go/vX.Y.Z` | none — see below |

The Go client has no publish workflow because a Go module has no registry to push to: the proxy
fetches it from the tag. Neither half of that tag is a style choice — a module in a subdirectory is
only resolvable at `<subdir>/vX.Y.Z`, so it is `sdks/go/v2.0.0` and never `go-v2.0.0`, and a module
released above v1 carries the major in its own path, which is why the import path is
`github.com/ByteVeda/flexiq/sdks/go/v2`. Moving to 3.0.0 means editing that path in `go.mod` as
well as cutting the tag.

The server workflow packages the Helm chart during preflight, then publishes it
after the image, tag, release, and wire-contract assets succeed. On the first
chart release, an organization owner must make both GHCR packages public.

Both halves are gated, and neither gate can be satisfied from CI — GitHub
exposes no API for package visibility, so a failure here is a console action:

- Preflight probes `ghcr.io/byteveda/flexiq-server:latest` anonymously and stops
  the run before anything is published, in dry runs too.
- The chart publish probes its own package immediately after `helm push`. The
  chart cannot be checked in preflight because it does not exist until that
  push, and an anonymous probe cannot tell a missing package from a private one.

To clear either failure, open the package under
[the organization's packages](https://github.com/orgs/ByteVeda/packages), then
**Package settings → Change visibility → Public**, and rerun. If *Public* and
*Internal* are greyed out with "Setting is disabled by organization
administrators", the block is the organization policy rather than the package:
an owner enables public packages under **Organization settings → Packages →
Package creation** first. Neither setting is exposed in the REST API.

Verify from a machine with no GHCR credentials:

```bash
docker manifest inspect ghcr.io/byteveda/flexiq-server:X.Y.Z
helm show chart oci://ghcr.io/byteveda/charts/flexiq-server --version X.Y.Z
```

**The first chart release fails once, by construction.** The chart package does
not exist until `helm push` creates it, so it cannot be made public in advance,
and the probe that follows the push is what creates the failure. Expect:

1. Tag `server-vX.Y.Z`. The image, git tag, GitHub release and wire-contract
   assets all publish; the chart pushes; the anonymous probe fails.
2. Make `charts/flexiq-server` public, as above.
3. Rerun the failed job.

A rerun replays the whole `manifest` job, not just the failed step, so every
step in it is written to be repeatable: `imagetools create` rebuilds `:VERSION`
and `:latest` from the per-architecture tags still in the registry, the git tag
is skipped on a tag push and otherwise re-pushed at the same commit, the release
is created only if `gh release view` misses and its assets upload with
`--clobber`, and the chart is packaged and pushed again. OCI chart tags are
mutable, so that last push replaces the version already there — which is also
why the published chart version is worth verifying as part of the release check
rather than assumed from a green run.

One `git tag` per tag — it takes a single name plus an optional commit, so passing all five at
once is `fatal: too many arguments` and creates none of them:

```bash
git tag X.Y.Z
git tag crates-vX.Y.Z
git tag node-vX.Y.Z
git tag java-vX.Y.Z
git tag server-vX.Y.Z
git tag -l          # all five there? a failed tag is silent otherwise
```

Push them by name, and push `crates-v*` on its own first. `git push origin --tags` is the wrong
instrument twice over: it sends every local tag, including any stale one that never belonged to
this release, and it starts all five workflows at once — which throws away the crates.io-first
ordering below.

```bash
git push origin crates-vX.Y.Z
# wait for publish-crates.yml to go green
git push origin X.Y.Z node-vX.Y.Z java-vX.Y.Z server-vX.Y.Z
```

Four things are easy to get wrong:

- **The tag patterns are exact-match globs.** A near miss like `crates-X.Y.Z` matches no workflow
  and fails silently — no run, no error, nothing published.
- **`publish-py.yml` also matches `vX.Y.Z`**, but every historical Python tag is bare. Keep it bare.
- **A crates.io version can be yanked but never replaced**, and PyPI is the same. That is why
  `crates-v*` goes first and alone: if it fails, the registries that can still be redone are ahead
  of you rather than behind. A bad `X.Y.Z` becomes `X.Y.Z+1`; there is no re-push.
- **Creating the GitHub Release by hand first is fine.** Each workflow guards its own
  `gh release create` behind a `gh release view` check and skips when one exists, and its
  `Create git tag` step only runs for `workflow_dispatch`.

Each workflow re-verifies the tag against `scripts/version.mjs --current` in preflight, so a
mistagged release fails before it builds rather than shipping something wrong.

## Questions?

Open an issue on GitHub if you have questions or want to discuss a feature before implementing it.

One exception: a suspected vulnerability never goes in an issue. [`SECURITY.md`](SECURITY.md) has
the private channel, the supported versions, and what is in scope.
