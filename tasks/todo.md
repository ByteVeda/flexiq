# #716 — gRPC: a shared-secret interceptor behind an Authenticator seam

Epic #710, stage 1. Reviewed against design doc §5.1, D10, §11.
**Fails if:** the namespace is read from a request body · the check is per-RPC
rather than one interceptor · `Principal` lacks a namespace and scope from the
first commit · a non-loopback bind with no token starts.

Branch: `feat/grpc-shared-secret`. Commit as pratyush. **Do not push.**

## The shape

One tower `Layer` over the whole `Routes` — not `InterceptedService` per
service, not a check per RPC. `Server::layer` takes `L: Layer<Routes>`, so the
gate sits where `dashboard/auth/middleware.rs::gate_request` sits for the
dashboard: a place a new RPC cannot land outside of.

`tonic::service::interceptor` is *not* enough: an `Interceptor` sees
`Request<()>` and therefore no URI path, and the gate needs the path to keep
`grpc.health.v1` public (kubelet's `grpc:` probe carries no metadata).

The layer inserts a `Principal` into the request extensions. `Producer` reads
its namespace from there and holds none of its own, so the swap in #717 is a
change to what the authenticator returns and to nothing else. A `Producer`
served *without* the layer fails every call — fail closed.

## Tasks

### 1. Share the token parser
- [ ] `config/listen.rs`: `token()` → `pub(crate) fn secret(env, var)`, message
      names `var`. `MIN_TOKEN_LEN` stays the one number.
- [ ] Grep `match=`/`assert` for the old message before rewording.

### 2. `FLEXIQ_GRPC_TOKEN`
- [ ] `config/grpc.rs`: `GrpcConfig.token: Option<Secret>`, `TOKEN_VAR`.
- [ ] Non-loopback **with no token** refuses; non-loopback **with** a token is
      allowed. Unix socket unchanged — the filesystem mode is the boundary.
- [ ] `grpc::scrub_token()`, called from `main` beside `scrub_attach_token`.
- [ ] `main.rs` ENV_HELP: the new variable, and the old
      "authenticates no caller" line goes.
- [ ] `grpc/listener.rs`: the post-bind guard takes the token into account.

### 3. The seam
- [ ] `grpc/auth/principal.rs` — `Scope::{Produce,Execute}` (one per proto
      package, per D1), `ScopeSet`, `Principal { namespace, scopes }`.
- [ ] `grpc/auth/authenticator.rs` — `Authenticator::authenticate(&MetadataMap)
      -> Result<Principal, Status>`; `Anonymous` (loopback, no token
      configured).
- [ ] `grpc/auth/shared_secret.rs` — `authorization: Bearer <token>`,
      constant-time. Wrong and missing produce the *same* status.
- [ ] `grpc/auth/gate.rs` — path → `Public | Authenticated | Scoped(Scope)`.
      `/grpc.health.v1.Health/` public; `/flexiq.v1.` → Produce;
      `/flexiq.executor.v1.` → Execute; **anything else authenticated**, so an
      unknown path is not an unauthenticated service-existence oracle.
- [ ] `grpc/auth/layer.rs` — `AuthLayer`/`Authenticated<S>`. Splits the request
      into parts so the `MetadataMap` is moved, not cloned; rejects with
      `Status::into_http::<ResBody>()`.

### 4. Wire it
- [ ] `status/reason.rs`: `UNAUTHENTICATED`, `SCOPE_DENIED`.
- [ ] `status/mod.rs`: `WireError::unauthenticated()` (one constructor, one
      message — that is what makes wrong and missing indistinguishable),
      `WireError::scope_denied()`.
- [ ] `grpc/listener.rs`: build the authenticator from the config, `.layer(...)`.

### 5. The namespace comes off the principal
- [ ] `producer/mod.rs`: `Producer::new(storage)` — no namespace field.
      `Scoped<'_> { storage, namespace }` built once per RPC in the trait impl,
      the one place all six are joined. Missing principal → `INTERNAL`.
- [ ] `producer/{enqueue,reads,cancel}.rs` take `&Scoped`.

### 6. Tests
- [ ] `config/grpc.rs`: short token refused · non-loopback + token accepted ·
      non-loopback without token refused, naming `FLEXIQ_GRPC_TOKEN` · unix
      without token accepted.
- [ ] `auth/gate.rs`: the four classifications · a `Produce`-only principal is
      refused an executor path.
- [ ] `auth/shared_secret.rs`: match · mismatch · absent · wrong scheme, and
      that the three failures are byte-identical.
- [ ] `tests/grpc_auth.rs` (feature `grpc`): no credential → `UNAUTHENTICATED` ·
      wrong credential → identical code/message/reason · right credential →
      the enqueue lands · health answers `SERVING` with no credential · an
      unimplemented path answers `UNAUTHENTICATED`, not `UNIMPLEMENTED` · with
      no token configured the job still lands in the configured namespace.
- [ ] `tests/grpc_role.rs`, `tests/grpc_producer.rs`: the new config field.

### 7. Docs
- [ ] `crates/flexiq-server/README.md` — env table + the gRPC section.
- [ ] `docs/.../operations/deployment.mdx` — the credential, the lifted
      non-loopback refusal, the untrusted-network warning that stands until
      #717, `grpcurl -H 'authorization: Bearer …'`.

### 8. Chart (scope decision below)
- [ ] `values.yaml`: `grpc.token` / `grpc.existingSecret` /
      `grpc.existingSecretKey`.
- [ ] `_validate.tpl`: the "not available yet" fail becomes "requires a token".
- [ ] `secret.yaml` + `deployment.yaml`: `FLEXIQ_GRPC_TOKEN`.
- [ ] `ci-chart.yml`: the gRPC-only probe assertions return.

## Verify
- `cargo fmt` · `cargo clippy -p flexiq-server --features grpc -j2 -- -D warnings`
- `cargo test -p flexiq-server --features grpc -j2`
- `cargo test -p flexiq-server -j2` (the role compiled out)
- `cargo check --workspace -j2`
- `helm template` a gRPC-only release, with and without a token

## Review

**Done.** All eight groups, on `feat/grpc-shared-secret`. Not pushed.

### What the shape turned out to be

- **`Server::layer`, not an interceptor.** `tonic::service::interceptor` hands
  the callback a `Request<()>`, which carries metadata and extensions but **no
  URI** — so an interceptor cannot allowlist `grpc.health.v1`, and gating health
  would cost a gRPC-only pod its readiness probe (kubelet's `grpc:` probe sends
  no metadata). `grpc/auth/layer.rs` is therefore a hand-written
  `tower_layer::Layer` over `Routes`; that needed `http`, `tower-layer` and
  `tower-service` as named optional deps rather than reaching through
  `tonic::codegen`, which is codegen's namespace and not an API.
- **The layer moves the headers rather than cloning them** — into a
  `MetadataMap` and back through `into_headers`, the same way tonic's own
  `InterceptedService` takes a request apart.
- **`Producer` lost its namespace field entirely.** It reads the principal out
  of the request extensions in `Producer::scope`, called once per RPC in the
  trait impl. A request with no principal is `INTERNAL`, so registering the
  service *without* the layer serves nothing rather than serving everything
  unauthenticated — the seam is load-bearing, not notional. Handlers take
  `&Scoped<'_>` and became `pub(crate)`.
- **Scope enforcement shipped too**, not just the field. `Scope::{Produce,
  Execute}` is one per proto package (D1), the gate maps a path prefix to the
  scope its package needs, and `SCOPE_DENIED` /`PERMISSION_DENIED` exists with
  the `scope` metadata key. Both #716 authenticators grant `ScopeSet::ALL`, so
  it is unreachable over the wire today and pinned by a unit test — which is
  what makes #717 a change to the authenticator and to nothing else.
- **The default is closed.** An unrouted path is authenticated before it is
  routed, so an anonymous caller gets `UNAUTHENTICATED` and not the
  `UNIMPLEMENTED` that would enumerate the build's services. That assertion is
  the e2e proof that the check is one interceptor: a per-RPC check cannot
  produce it.

### Amendments

`tasks/specs/2026-09-01-flexiq-v1-proto-design.md` §5.1 gained the public-path
rule — health only, reflection gated, unrouted paths gated — because #717
inherits that list and the doc did not name it.

### Verified

- `cargo test -p flexiq-server --features grpc -j2` — 260 unit + 106
  integration, all green, `grpc_auth` 9/9.
- `cargo test -p flexiq-server -j2` (role compiled out) — 200 unit + the rest.
- `cargo clippy -p flexiq-server --features grpc --all-targets -- -D warnings`,
  and the pre-commit form `--all-targets --all-features`.
- `cargo check --workspace -j2`.
- `helm lint` on three role combinations; a gRPC-only render carrying
  `FLEXIQ_GRPC_TOKEN` from the chart Secret and from `existingSecret`; the three
  refusals (no token, short token, no namespace) each failing with their own
  message; and the probe wiring the restored `ci-chart.yml` assertion checks —
  `['tcpSocket'] ['grpc'] True`, falling back to `['tcpSocket'] ['tcpSocket']`
  under `grpc.healthProbe=false`.

### Not done here

- The docs site build was not run (content-only MDX, and the build OOMs on this
  machine); the `<Callout>` matches the 17 already in the file.
- #717 remains the real credential. The untrusted-network warning stays in
  `deployment.mdx` and the chart README until it lands.
