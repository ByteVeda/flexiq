# #833 — the descriptor as a supported interface

Branch `feat/descriptor-release-asset` off `master` at `f349fae1`. Docs only: no Rust, no proto,
no workflow change, no new page.

## What the issue asked for, and what is already true

The issue has two bullets. The first is **already shipped**, and the second is two-thirds shipped.

- **"Publish the descriptor as a release asset."** Done in #803. `publish-server.yml:332-364`
  stages `contracts/descriptor.binpb` as `flexiq-descriptor-<v>.binpb` beside
  `flexiq-proto-<v>.tar.gz` and `gh release upload --clobber`s both. Verified live:
  `server-v2.0.0` carries the descriptor at 88467 bytes. Nothing to add.
- **"Document the reflection-only path."** `clients.mdx` has reflection as one of three routes
  to the contract with the `list`/`describe` commands and the two asset `curl` lines;
  `grpc.mdx` has `list` and a JSON-body `Enqueue`; `contract.mdx` explains why reflection
  cannot describe a contract the server does not implement.

## What was actually missing

1. **No page showed what `list` and `describe` return.** Every one showed the command and
   stopped. That is the concrete thing the issue names and the thing a reader cannot supply
   themselves.
2. **The security caveat was the wrong shape.** The issue says reflection "exposes the schema to
   anyone who can reach the port". False here — `grpc/auth/gate.rs:80-86` classifies reflection
   `Requirement::Authenticated` and `tests/grpc_auth.rs:426` pins that an anonymous
   `ListServices` is refused `Unauthenticated`. The true caveat is sharper and matches the
   issue's own "matters once auth scopes are more than binary": reflection is authenticated but
   **unscoped**, so a `produce` token reads the whole schema, `flexiq.executor.v1` included.
3. **`contract.mdx` claimed "every `server-v*` release attaches the same descriptor".** False for
   `server-v1.0.0`, which carries zero assets — the gRPC role shipped in 2.0.0.

## Plan

- [x] Build `flexiq-server --features grpc -j2`, run it on a temp SQLite DSN with a namespace,
      mint a token, and capture real `grpcurl` output. Nothing goes in the docs unrun.
- [x] `grpc.mdx`: turn the one reflection paragraph into a section that shows what `list` and
      `describe` return, verbatim, and ties the credential-free `-proto` flag to the JSON-body
      call already above it
- [x] `grpc.mdx`: state the caveat accurately — authenticated, unscoped, and what that means for
      revocation vs scoping
- [x] `contract.mdx` + `clients.mdx`: pin the asset to releases from 2.0.0
- [x] `pnpm --dir docs check:parity`, `lint`, `typecheck`, `build`

## Not doing

- **No new page.** Three pages already assert facts about reflection; a fourth becomes the fourth
  source of truth on one claim, which is the failure mode this tier is warned about.
- **No extra release asset.** Attaching the descriptor to the PyPI, npm, Maven and crates
  releases would put a server artifact on four releases whose door is not the server.

## Review

Three commits, all docs.

- `docs: show what gRPC reflection returns` — a `## Reflection, and what it hands out` section in
  `grpc.mdx` carrying the real output of `list`, `list flexiq.v1.ProducerService` and
  `describe flexiq.v1.EnqueueRequest`, and a pointer to it from `clients.mdx`.
- `docs: name what a token buys on the reflection door` — the unscoped-reflection callout, a
  `## Security` bullet, and the same fact under `tokens.mdx`'s scope table, which is about calls
  and was silent about the schema.
- `docs: pin the descriptor asset to releases from 2.0.0`.

**A bug the verification turned up, fixed in the first commit.** `grpc.mdx` carried

```bash
grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check
```

under the comment "Health is the exception, and needs no credential." Run against a real server
it fails: `Unauthenticated: failed to query for service descriptor "grpc.health.v1.Health"`.
The health *call* is public — `-import-path . -proto health.proto` returns `SERVING` with no
credential — but grpcurl resolves a method through **reflection** before invoking it, and
reflection is gated. The line now carries the token, and says why.

**Verified against a running server**, not from the descriptor: `flexiq-server 2.0.0` built with
`--features grpc`, on a temp SQLite DSN with `FLEXIQ_NAMESPACE=prod` and
`FLEXIQ_GRPC_LISTEN=127.0.0.1:50051`. Every code block on the new section is pasted from that
run. The caveat was demonstrated both ways: a `produce` token `list`s
`flexiq.executor.v1.ExecutorService` and its two methods, then gets
`PermissionDenied: this credential does not carry the `execute` scope` on
`ExecutorService/Attach`. An `execute` token reaches the handler instead
(`NotFound: no attached stream for this session`), so the executor door *is* routed here — the
listing is not of a service this build fails to serve.

`pnpm --dir docs check:parity` (tier shape and internal links included), `lint`, `typecheck` and
`build` all green; the new heading's anchor and both inbound links were checked in the
prerendered HTML, since the link checker strips anchors rather than resolving them.
