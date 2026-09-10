# #831 — an OpenAPI document generated from the proto

Branch `feat/openapi-from-proto` off `master` at `5aeccb98`. Design:
`tasks/specs/2026-09-10-openapi-from-proto-design.md`.

## Plan

- [x] 1. Annotate the eight `flexiq.v1` RPCs with `google.api.http`; regenerate
      `contracts/descriptor.binpb` via `scripts/proto-check.sh --fix`.
- [x] 2. `crates/flexiq-openapi` — descriptor reader (incl. the `HttpRule`
      extension prost drops), proto3-JSON schema mapping, document assembly,
      binary that writes to stdout or `--out`.
- [x] 3. Generate `contracts/openapi.json`; gate it in `scripts/proto-check.sh`
      beside the descriptor compare.
- [x] 4. Drift test in `facade/routes.rs`: every `Binding` ⟺ exactly one
      annotation, same verb, same path.
- [x] 5. `GET /v1/stats` reads `?queue=`, so the annotation's query parameter is
      real (design D5).
- [x] 6. `ci.yml` path filters gain the generator crate; `ci-proto.yml` gains a
      toolchain.
- [x] 7. Docs: link the document from the `/server/grpc` page, the client guide
      and `contracts/REMOTE_SDK_CONTRACT.md`.

## Review

Nine operations over eight RPCs, 29 component schemas, 52 KB. OpenAPI 3.1,
validated by `openapi-spec-validator`.

**What the live check found.** Driving every documented operation against a
running `flexiq-server` — the thing the issue asks for in its second bullet —
turned up a bug that has nothing to do with generation. `facade/error.rs`'s
`rich()` decoded `status.details()` unconditionally, and prost decodes an
*empty* buffer into a default `google.rpc.Status`. Every error carrying no
`ErrorInfo` therefore rendered as `{"code": 200, "status": "OK"}` under a
non-2xx HTTP status: `GET /v1/workflows/no-such-run` answered 404 with a body
that said the call succeeded. A client branching on `status`, as
`REMOTE_SDK_CONTRACT.md` tells it to, read a failure as a success. Fixed in its
own commit with a unit test per arm and an assertion on the live path.

**One deliberate behaviour change** (design D5). `GET /v1/stats` now reads
`?queue=`, because under `google.api.http` a request field the path does not
bind is a query parameter, and the alternative was a document advertising a
filter the server ignored.

**What the descriptor cost.** `google/api/annotations.proto` pulls in
`google/protobuf/descriptor.proto`, so `contracts/descriptor.binpb` went from
88 KB to 182 KB. It is embedded in the binary and served over reflection, which
is correct — a client resolving the annotations needs those files — but it is
the one number in this change that got twice as big.

**Not done, and why.** The listener does not serve the document, and there is no
test sweeping the spec against a live server on every run. The annotation-to-
`Binding` drift test is what holds the document to the router; the live sweep
was run by hand, once, before this landed.

## Verification

- `scripts/proto-check.sh` clean; staleness proven by hand (a path renamed in
  the `.proto`, descriptor refreshed alone, gate failed on the document).
- `cargo test -p flexiq-openapi` (17), `cargo test -p flexiq-server --features
  grpc` (422 unit + every integration suite).
- `cargo clippy --all-targets -- -D warnings` on both crates.
- Every one of the nine documented operations driven against a running server:
  all routed, every error body's `code` agreeing with its HTTP status.
