## What changed and why?

<!-- Describe the problem and the resulting behavior. Put "Closes #123" in a commit subject or the PR title; squash merges do not read the PR body. -->

## Validation

Check the suites you ran locally, or explain why they do not apply.

- [ ] Rust lint: `cargo fmt --all --check && cargo clippy --all-targets --all-features -- -D warnings`
- [ ] Python lint: `cd sdks/python && uv run ruff check flexiq/ tests/ && uv run mypy flexiq/ tests/ --no-incremental`
- [ ] Rust / SQLite: `cargo test --workspace`
- [ ] Rust / PostgreSQL: `cargo test --workspace --exclude flexiq-python --features postgres,workflows`
- [ ] Rust / Redis: `cargo test --workspace --features redis,workflows`
- [ ] Python: `cd sdks/python && uv sync --extra dev --extra oauth && uv run python -m pytest tests/`
- [ ] Node.js: `pnpm -C sdks/node build && pnpm -C sdks/node test`
- [ ] Java: `cd sdks/java && ./gradlew build --no-daemon`
- [ ] Docs: `pnpm --dir docs typecheck && pnpm --dir docs lint && NODE_OPTIONS=--max-old-space-size=8192 pnpm --dir docs build`
- [ ] Not applicable (explain below)

The PostgreSQL and Redis suites skip unless `FLEXIQ_POSTGRES_TEST_URL` and `FLEXIQ_REDIS_TEST_URL` are set.

<!-- If crates/ changed, rebuild each shell's native artifact and rerun the Python, Node.js, and Java suites. Gitignored native artifacts survive branch switches. -->

PR titles use a conventional lowercase prefix such as `feat:`, `fix:`, `docs:`,
`test:`, `refactor:`, `perf:`, `chore:`, `ci:`, `build:`, `style:`, or `revert:`.
