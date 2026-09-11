# `fq` — a standalone CLI over the producer door (#832)

Spec: `tasks/specs/2026-09-10-fq-cli-design.md`
Plan: `tasks/plans/2026-09-10-fq-cli.md`

- [x] 1. Crate skeleton and generated client
- [x] 2. Endpoint parsing, transport and the bearer credential
- [x] 3. Shell tokens to `StructuredArgs`
- [x] 4. Table and proto3 JSON rendering
- [x] 5. Errors that name the wire's reason
- [x] 6. `fq enqueue`
- [x] 7. `fq jobs list`, `get` and `cancel`
- [x] 8. `fq queues`
- [x] 9. End to end against a real listener
- [x] 10. Ship it in the server's release
- [x] 11. Document it, and the holes
- [x] 12. Whole-repository verification

## Review

**What shipped.** `crates/flexiq-cli`, a library plus a binary named `fq`. It
generates its tonic client from `contracts/descriptor.binpb` and depends on no
member of this workspace — no `flexiq-core`, so no Diesel, no bundled SQLite, no
C toolchain. `fq enqueue`, `fq jobs list|get|cancel` and `fq queues`, with
`--json` emitting the same proto3 JSON the server's `/v1` facade returns.

**Decisions worth remembering.**

- The binary is `fq`, not `flexiq`: that name belongs to three SDK console
  scripts already.
- Enqueue sends the wire's `structured` arm, so the payload envelope is built by
  the one in-tree CBOR encoder. The cost — sorted kwargs, a 2⁵³ integer cap — is
  documented beside the flags, and `--unique-key` is the answer.
- The endpoint's scheme picks the transport (`http://`, `https://`, `unix:`).
  Nothing is guessed, because the server terminates no TLS of its own.
- The credential is `$FLEXIQ_TOKEN` only. There is no `--namespace`: a namespace
  is a property of the token.
- The end-to-end suite lives in `crates/flexiq-server/tests/grpc_cli.rs`, not in
  the CLI crate. A dev-dependency the other way would unify the `grpc` feature
  into every `cargo check --workspace`. `cargo tree -p flexiq-server -e normal
  -i tonic` confirms it has not.
- `output.rs` is a second implementation of the server's own JSON rendering. The
  e2e suite re-encodes a `Job` across the two generated types and asserts the
  renders are equal, so a new field that only one side learns fails a test.

**Verified.** 43 CLI unit tests; 6 end-to-end tests against a real listener;
`cargo test --workspace`; `cargo test -p flexiq-server --features grpc`;
`cargo clippy --all-targets --all-features -D warnings`; `cargo fmt --check`;
`cargo check --workspace` under `postgres` and under `redis`; the rustdoc gate
with CI's exact `RUSTDOCFLAGS`; `node scripts/version.mjs --check`;
`scripts/proto-check.sh`; docs lint, typecheck and build. The container image
was built locally: both binaries are present, both report `2.0.0`, and `fq`
extracts from it as a `static-pie` executable — which is what the new
`ci-server-image.yml` assertion and the new release step do. Every command block
in `docs/content/docs/server/operate/cli.mdx` was run against a live server and
its output pasted from that run.

**Not done, on purpose.**

- `fq tail` — no `WatchJobs` on the wire (#837). A poll loop behind a subcommand
  is the cost that issue exists to remove.
- `fq dlq`, queue pause/resume, worker list/drain, periodic tasks, rate limits,
  queue enumeration, queue rates — none are on the wire (#836).
- `fq enqueue --from-file` over `EnqueueBatch`, and `fq workflows` — filed, not
  built. A `WorkflowGraph` is a document, not a command line.

**Follow-ups to file.** Comments on #836 and #837 naming the eight blocked
subcommands, so the proto work has the CLI's requirements in front of it. The
table in `docs/content/docs/server/operate/cli.mdx` is the source.
