# `fq`, a standalone CLI over the producer door — design

Issue [#832](https://github.com/ByteVeda/flexiq/issues/832). Milestone: Opportunistic.

A decision record for `crates/flexiq-cli`, not a contract. The contract is
[`contracts/REMOTE_SDK_CONTRACT.md`](../../contracts/REMOTE_SDK_CONTRACT.md); where the two
disagree, that one is right and this one is out of date.

## Why this exists

There are three CLIs today and each one ships inside an SDK. Reaching any of them means
installing a language runtime and a native artifact for that language: `pip install flexiq`
brings a maturin wheel, `npm i flexiq` brings a napi `.node`, the Java one is a class inside a
library jar with no launcher at all. An operator who runs `flexiq-server` and writes their
application in Go, or Ruby, or nothing at all, has no command line.

All three also talk to the **database**, not to the server. They open a `StorageBackend` off a
DSN and issue queries. That is the wrong door for an operator holding a scoped API token and no
database credential, and it is a door the wire deliberately does not expose.

So: one binary, over gRPC, linking no SDK and no storage layer.

The second reason is diagnostic. Every subcommand the issue asks for that cannot be written is a
hole in the proto, and writing the ones that can be is the cheapest way to enumerate them. That
list is a deliverable of this work, not a footnote — see [Holes found](#holes-found).

## Shape

One crate at `crates/flexiq-cli`, `publish = false`, `version.workspace = true`. A library
(`flexiq_cli`) plus one binary target named **`fq`**.

`fq`, not `flexiq`: the bare name is already the console script of the Python SDK
(`sdks/python/pyproject.toml` `[project.scripts]`), the `bin` entry of the Node package, and the
picocli `@Command(name = "flexiq")` of the Java one. A fourth thing called `flexiq` on a `PATH`
that has any of them is a collision the user hits, not one the build catches.

| File | Holds |
| --- | --- |
| `build.rs` | Codegen off `contracts/descriptor.binpb`, client only |
| `src/lib.rs` | Barrel: the modules, and `run(Cli)` |
| `src/pb.rs` | `include!` of the generated `flexiq.v1` types |
| `src/cli.rs` | The whole clap tree, and nothing else |
| `src/connect.rs` | Endpoint parsing, transport, the bearer interceptor |
| `src/args.rs` | Shell tokens → `StructuredArgs` |
| `src/output.rs` | Table and proto3-JSON rendering |
| `src/error.rs` | `tonic::Status` → an operator-readable message and an exit code |
| `src/commands/` | One file per subcommand group: `enqueue`, `jobs`, `queues` |
| `src/main.rs` | Argument parse, runtime, exit code |

It links `tonic`, `prost`, `clap`, `serde_json`, `chrono`, `anyhow` — and no member of this
workspace. In particular it does not link `flexiq-core`, so it needs no Diesel, no bundled
SQLite, no libpq and no C toolchain, and a `cargo build -p flexiq-cli` on a clean machine
touches none of them.

### Codegen

`build.rs` is `crates/flexiq-server/build.rs`'s `generate_proto_types` with `build_server(false)`:
it reads the committed `FileDescriptorSet` rather than running `protoc`, so the CLI's types come
from the exact artifact `scripts/proto-check.sh` gates, `buf breaking` protects and the server
serves over reflection. Only the `flexiq.v1` module is `include!`d; `flexiq.executor.v1` is a
different door with a different scope and this binary never opens it.

The two `configure()` settings the server sets are set here for the same reasons: `.extern_path(".google.rpc", "::tonic_types::pb")`, because `google.rpc.Status` must be the type
whose `StatusExt` reads the error details, and `.btree_map(".flexiq.v1.StructuredArgs.kwargs")`,
because that map is encoded into CBOR whose key order is part of the bytes.

## The command tree

```
fq [--endpoint URL] [--json] <command>

fq enqueue <task> [arg…] [--kw KEY=VALUE]…
      -q, --queue        --priority        --max-retries
      --delay-ms         --timeout-ms      --expires-in-ms    --result-ttl-ms
      --unique-key       --metadata JSON   --notes            --depends-on ID…
fq jobs list     [--status S] [-q QUEUE] [-t TASK] [--limit N] [--page-token T]
fq jobs get <id> [--payload] [--result]
fq jobs cancel <id>
fq queues [<name>]
```

Flag names track the Node CLI (`sdks/node/src/cli/`) wherever the two overlap — `-q/--queue`,
`--priority`, `--max-retries`, `--delay-ms`, `--unique-key`, `--json` — so an operator reading
one surface can guess the other. Durations are `*-ms` integers rather than a `30s` grammar for
the same reason: Node already made that choice and a second spelling buys nothing.

`fq jobs list` and `fq queues` are the two commands whose shape the wire dictated rather than the
Node CLI: pagination is `--page-token`, an opaque server-issued string, because `ListJobs` has no
offset; and `fq queues` takes an optional name because `QueueStats` is the only stats RPC and it
answers for one queue or for the namespace, with no way to enumerate.

## Decisions

### The namespace is not a flag

Every other CLI has one. This one must not. The namespace is fixed at token-mint time and the
gRPC role serves exactly one; a flag would be a control the server ignores. `Job.namespace` comes
back on reads and is rendered, so an operator can still see which one they are in.

### The token comes from the environment only

`$FLEXIQ_TOKEN`, never an argument. Python, Node and Java all take this position for attach
tokens and all three say so in a comment; a secret in `argv` is a secret in `ps` and in shell
history. No default and no prompt: an unset variable is an error naming the variable.

### The endpoint scheme picks the transport

`--endpoint` / `$FLEXIQ_ENDPOINT`, default `http://127.0.0.1:50051` — the address every example
in `docs/content/docs/server/operate/grpc.mdx` uses.

- `http://host:port` — plaintext
- `https://host:port` — TLS, verified against the platform roots
- `unix:/path/to.sock` — Unix domain socket

The Go client defaults to TLS and needs `WithInsecureTransport()` to go plaintext. That is right
for a library, where the caller who says nothing should get the safe thing. For a CLI it would
mean `fq --endpoint 127.0.0.1:50051` silently attempting TLS against a server that
[terminates none](../../contracts/REMOTE_SDK_CONTRACT.md), and failing with a handshake error
that names nothing an operator can act on. A scheme in the URL is unambiguous and it is what they
already typed into `grpcurl`.

### `enqueue` sends `structured`, not `raw`

`EnqueueRequest.body` has two arms. `raw` is the tag byte plus CBOR an SDK sends; `structured` is
`repeated google.protobuf.Value` the server encodes with `flexiq_core::wire::encode_call`, the
one in-tree encoder.

`fq` sends `structured`. A CLI's arguments arrive as shell text and JSON is the natural model for
them, so nothing is lost in the conversion that was ever expressible; and the alternative is a
second CBOR encoder in this repository, independently responsible for the definite-length and
shortest-form rules, held to the contract by nothing but its own tests. Reusing the canonical
encoder instead would mean linking `flexiq-core`, and therefore Diesel, into a binary whose
entire premise is linking no SDK.

Two costs, both documented on the CLI's docs page:

- a protobuf map decodes into a `BTreeMap` server-side, so **kwargs are encoded in sorted key
  order** regardless of the order typed;
- `structured` refuses integers beyond ±(2⁵³−1) and non-finite floats, because
  `google.protobuf.Value` cannot hold them.

Either can make an `auto:` idempotency key computed from an `fq` enqueue differ from an SDK's for
the same logical call. `--unique-key` is the answer, and the docs page says so next to the flag.

### Positional arguments are JSON, falling back to string

`fq enqueue send_email a@b.c 3 true '{"x":1}'` sends the string `"a@b.c"`, the number `3`, the
boolean `true` and an object. Each token is handed to `serde_json::from_str`; on a parse error it
becomes a JSON string. Bare words are the overwhelmingly common case and quoting every one of
them would be miserable, while `3` meaning the string `"3"` would be a bug the user cannot see
until the task raises a `TypeError`.

`--force-string`-style escapes are not offered. `'"3"'` is already the JSON spelling of the
string `3`, and a second mechanism for the same thing is a second thing to document.

Keyword arguments are `--kw name=value`, repeatable, splitting on the **first** `=`; the value
side goes through the same JSON-then-string rule. A repeated flag rather than a positional
`name=value` convention because a positional one cannot express a positional argument that
happens to contain `=`.

### `--json` is proto3 JSON, and matches the facade byte for byte

The server already speaks JSON: `crates/flexiq-server/src/grpc/facade` serves `/v1/jobs` and
friends with proto3 JSON semantics — `lowerCamelCase` field names, RFC 3339 timestamps, enums as
their names, absent fields absent rather than zero-valued. `fq --json` emits the same, so one
`jq` expression works against `fq jobs get X --json` and `curl .../v1/jobs/X` alike, and an
operator moving between them learns one shape.

This is hand-rendered in `output.rs` rather than derived: prost types carry no serde
implementation, and pulling in a JSON-mapping crate to produce twenty-five fields would be more
machinery than the fields.

It is therefore a second implementation of a rendering the server already has in
`facade::json::response`, and a second implementation of a shared format drifts unless something
holds it. The end-to-end test does: it takes a `Job` off the wire, re-encodes it with prost,
decodes it into the server's own generated type, and asserts `flexiq_cli`'s render equals
`flexiq_server::grpc::facade::json::response::job`'s, field for field. A field added to `Job`
that only one side learns to render fails that test.

Without `--json`, output is an aligned table with the same construction as
`sdks/node/src/cli/output.ts` — column widths from the widest cell, two spaces between columns, a
dashed rule, and `(none)` for an empty result.

### Errors name the wire's reason

Every non-OK response from the door carries a `google.rpc.ErrorInfo` with a `reason` from a
closed list, and sometimes `RetryInfo`. `error.rs` prints the gRPC code, the message, and the
reason with its metadata when present, because `PERMISSION_DENIED: scope denied` is not
actionable and `PERMISSION_DENIED (SCOPE_DENIED, scope=produce)` is.

`UNAUTHENTICATED` gets one extra line naming `$FLEXIQ_TOKEN`, since it is the same status for a
missing header, an unknown token, a revoked one, an expired one and a token minted for another
namespace — deliberately, so as not to be an existence oracle — and the operator's first question
is always "which of those is it".

Exit codes: `0` success, `2` usage (clap's own), `1` everything else.

## Testing

Three layers, no new CI job.

**Unit, in-crate.** Argument tokenisation (`args.rs`): bare word, number, boolean, object, a
value containing `=`, a kwarg whose value contains `=`, an integer past 2⁵³ rejected with a
message naming the limit. Endpoint parsing (`connect.rs`): each of the three schemes, a bare
`host:port` rejected with a message that names the schemes. Rendering (`output.rs`): the empty
table, column alignment, and that a JSON render omits an absent optional rather than emitting
`null`.

**End-to-end, in `crates/flexiq-server/tests/grpc_cli.rs`.** The CLI's command functions, run
against a real `Listener` on a real socket with a real minted token — the `Harness` in
`grpc_producer.rs`, unchanged in shape. It asserts what only a round trip can: that a job
enqueued by `fq enqueue` with positional and keyword arguments comes back from `fq jobs get` with
the payload the wire encoded, that `fq jobs list --status` filters, that `fq jobs cancel` is
idempotent, and that a wrong-scope token produces the `SCOPE_DENIED` line rather than a panic.

It lives in the server's test directory, not the CLI's, on purpose. A dev-dependency from
`flexiq-cli` on `flexiq-server` would unify the `grpc` feature into every `cargo check
--workspace`, which would put tonic and a code generator into the default build of a crate that
gates them behind a feature precisely so a non-gRPC build does not compile them. Inverting it
costs nothing: `flexiq-server` already has the feature, the harness and the CI step
(`cargo test -p flexiq-server --features grpc`, `ci-rust.yml`), and it gains a dev-dependency on
a crate with no runtime weight.

**The build itself.** `cargo test --workspace` compiles the CLI and runs its unit tests with no
feature flags, because it has none.

## Shipping

The issue's constraint is that the CLI's version and the server's cannot drift. They cannot,
because there is one `[workspace.package].version` and `scripts/version.mjs`'s `guards()` walks
`crates/*/Cargo.toml` and fails `--check` on any member that hardcodes a literal instead of
`version.workspace = true`. No new entry in `MIRRORS` or `SNIPPETS`: nothing pins this crate's
coordinate, because it is never published.

Four edits carry it into the release:

1. `Cargo.toml` — `crates/flexiq-cli` in `members`.
2. `.github/workflows/ci.yml` — `crates/flexiq-cli/**` in the `server:` path filter, so a change
   here rebuilds the image that ships it.
3. `docker/scheduler.Dockerfile` — one `cargo build` invocation building both binaries, and a
   second `COPY --from=builder`. `fq` gets the same `readelf -l | grep INTERP` assertion the
   server gets, for the same reason: `distroless/static` has no dynamic loader.
   `ci-server-image.yml` asserts `fq --version` in the built image beside the server's.
4. `.github/workflows/publish-server.yml` — the `manifest` job extracts `/usr/local/bin/fq` from
   each per-architecture image it has just verified and attaches the two as release assets.

The image is the primary artifact and carries both binaries at one version, which is the
guarantee the issue asked for. The loose binaries exist because the audience for this CLI is an
operator whose machine has no Rust toolchain and who should not have to `docker run` to enqueue
a job. They are extracted from the already-pushed images rather than built again, so a release
cannot ship an `fq` that differs from the one in its own image; `docker create` + `docker cp`
needs no emulation, because nothing in the arm64 container is executed.

## Holes found

The issue predicted that every subcommand which cannot be written is a hole in the proto. Five
of the six subcommand groups it lists are affected. Written to
`docs/content/docs/server/operate/cli.mdx` under "What the wire cannot do yet", and to the PR
body:

| Asked for | Blocked on | Detail |
| --- | --- | --- |
| `fq tail` | [#837](https://github.com/ByteVeda/flexiq/issues/837) | No `WatchJobs`. The only way to follow a job is `GetJob` in a loop, which is the poll loop #837 exists to delete — shipping it inside `fq` would bake the cost in and make it harder to remove. |
| `fq dlq list` / `replay` | [#836](https://github.com/ByteVeda/flexiq/issues/836) | Nothing dead-letter-shaped is on the wire. All three SDK CLIs have `dlq list`/`retry`/`delete`; the remote tier has none of it. |
| `fq queues` — rates | [#836](https://github.com/ByteVeda/flexiq/issues/836) | `QueueStats` returns six counts and no rate. Shipping counts only. |
| `fq queues` — enumeration | [#836](https://github.com/ByteVeda/flexiq/issues/836) | `QueueStatsRequest.queue` is optional, but "unset" means *aggregate the namespace*, not *list the queues*. A caller cannot discover a queue name it was not told. |
| queue pause / resume | [#836](https://github.com/ByteVeda/flexiq/issues/836) | In all three SDK CLIs. Not on the wire. |
| worker list / drain / heartbeats | [#836](https://github.com/ByteVeda/flexiq/issues/836) | Dashboard-only. |
| periodic task CRUD and trigger | [#836](https://github.com/ByteVeda/flexiq/issues/836) | Dashboard-only. |
| rate limits, concurrency caps | [#836](https://github.com/ByteVeda/flexiq/issues/836) | Dashboard-only. |

One further gap, not in the issue and not blocking it: `EnqueueBatch` exists on the door and `fq`
exposes no way to reach it, because a batch of enqueues is a file of them and a `--from-file`
argument is a format decision of its own. Filed, not built.

## Filed, not built

- `fq tail`, once #837 lands.
- `fq dlq`, and the operator verbs above, once #836 lands.
- `fq enqueue --from-file` over `EnqueueBatch`.
- `fq workflows submit` / `get`. `SubmitWorkflow` takes a `WorkflowGraph`, which is a document,
  not a command line.
