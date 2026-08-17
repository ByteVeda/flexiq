# Contributing to flexiq

Thanks for your interest in contributing! flexiq is a hybrid Rust + Python project, so the dev setup involves both ecosystems.

## Development Setup

### Prerequisites

- Python 3.9+
- Rust (stable) — install via [rustup](https://rustup.rs/)
- [maturin](https://github.com/PyO3/maturin) — builds the Rust extension

### Clone and Install

```bash
git clone https://github.com/ByteVeda/flexiq.git
cd flexiq/sdks/python   # the Python SDK lives here; node/ and java/ are peers

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
pnpm --dir docs types:check
pnpm --dir docs lint
pnpm --dir docs build
```

## Questions?

Open an issue on GitHub if you have questions or want to discuss a feature before implementing it.
