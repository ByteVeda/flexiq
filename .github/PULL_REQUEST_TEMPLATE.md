## What changed and why?

<!-- Describe the problem and the resulting behavior. Link the issue with "Closes #123". -->

## Validation

Check the suites you ran locally, or explain why they do not apply.

- [ ] Rust / SQLite: `cargo test --workspace`
- [ ] Rust / PostgreSQL: `cargo test --workspace --exclude flexiq-python --features postgres,workflows`
- [ ] Rust / Redis: `cargo test --workspace --features redis,workflows`
- [ ] Python: `cd sdks/python && uv run python -m pytest tests/`
- [ ] Node.js: `pnpm -C sdks/node test`
- [ ] Java: `cd sdks/java && ./gradlew build --no-daemon`
- [ ] Docs: `pnpm --dir docs typecheck && pnpm --dir docs lint && pnpm --dir docs build`
- [ ] Not applicable (explain below)

<!-- If crates/ changed, rebuild each shell's native artifact and rerun the Python, Node.js, and Java suites. Gitignored native artifacts survive branch switches. -->

PR titles use a conventional lowercase prefix such as `feat:`, `fix:`, `docs:`,
`test:`, `refactor:`, `perf:`, `chore:`, `ci:`, `build:`, `style:`, or `revert:`.
