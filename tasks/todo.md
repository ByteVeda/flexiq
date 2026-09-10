# #829 — a Go client over the producer door

Branch `feat/go-producer-client` off `master` at `739923da`. One new module at `sdks/go`, its CI
suite, and the prose that stops being true once it exists. No Rust, no proto, no schema.

Design record: [`tasks/specs/2026-09-10-go-producer-client-design.md`](specs/2026-09-10-go-producer-client-design.md).

## Scope

Six of the eight producer RPCs — everything the issue names, plus `ListJobs` because paging is
cheap once the client exists. `SubmitWorkflow` and `GetWorkflowRun` are filed, not built. The
executor door is out of scope by the issue's own words.

## Plan

- [x] `sdks/go` module at `github.com/ByteVeda/flexiq/sdks/go/v2`, `buf generate` wired to
      `contracts/proto`, stubs committed under `internal/pb`
- [x] The payload envelope — tag byte, definite-length CBOR `[args, kwargs]`, bare-value results
- [x] Conformance test against `contracts/wire-vectors.json`: 9 encode, 12 decode, 2 round trips
- [x] `Client`, dial options, bearer credential, TLS by default, both message-size directions
- [x] `Enqueue` / `EnqueueBatch`, options and debounce
- [x] `GetJob` / `ListJobs` / `AllJobs` iterator / `QueueStats` / `CancelJob`
- [x] The error model: closed `Reason` list, `errors.Is`, metadata accessors, `RetryAfter`
- [x] `TaskError`, including the unstructured fallback
- [x] A `ProducerService` double on bufconn, and the behaviour tests over it
- [x] `ci-go.yml` + dispatcher wiring, labeler entry, pre-commit hooks
- [x] `version.mjs` mirror so `--check` covers Go
- [x] README, root README, CONTRIBUTING, and the docs page that called Go the fourth
- [x] File the gaps as issues — #907 workflow RPCs, #908 the executor door, #909 a live end-to-end
      test against a real server

## Review

**What shipped.** A `sdks/go` module of about 1,100 lines of client and 1,000 of test, with no
dependency on `crates/`. `go test -race ./...` is green on go1.27; `go vet` and `gofmt` are clean;
`go mod tidy` is a no-op. `node scripts/version.mjs --check` passes with the Go mirror in place.
Docs `lint` and `typecheck` both pass.

**Three things worth knowing later.**

1. **The module path carries `/v2`.** Go requires it of any module released above v1, and the
   client ships at the repo's version. The tag is `sdks/go/v2.0.0`; there is no publish workflow
   because the proxy fetches from the tag.
2. **The CBOR encoder does not sort.** It cannot: `single-object-arg` pins `order_id` before
   `amount_cents`, and a sorting encoder fails that vector. The cost is that a Go `map` argument
   encodes to different bytes on different runs, so the README tells a caller to pass a struct
   where the bytes matter.
3. **The `polyglot` CI filter no longer watches `sdks/**`.** It names the three shells one by one
   instead, so a Go-only change stops triggering a three-runtime example build it has nothing to
   do with.

**Not done, deliberately.** No live end-to-end test against a running `flexiq-server`: the double
proves this client's half of the contract, and only a real server proves the pair. Filed.
