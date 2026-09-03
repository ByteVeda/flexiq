# flexiq-server

The FlexiQ scheduler, executor attach listener, dashboard, and gRPC door in one
binary, with no language runtime.

Task bodies stay in the app's own container: executors dial in over the worker
frame protocol and run them there, so this image is small and identical for
every SDK. It also serves the dashboard SPA, which the SDK packages otherwise
each ship a copy of.

```bash
FLEXIQ_DSN=/var/lib/flexiq/app.db \
FLEXIQ_LISTEN=127.0.0.1:7777 \
FLEXIQ_DASHBOARD=127.0.0.1:8080 \
flexiq-server
```

## Configuration

Environment only — the binary takes no flags beyond `--help` and `--version`.
Run `flexiq-server --help` for the full list; the essentials:

| Variable | Meaning |
|---|---|
| `FLEXIQ_DSN` | Storage connection string (required) |
| `FLEXIQ_BACKEND` | `sqlite` \| `postgres` \| `redis`; defaults to the DSN's scheme |
| `FLEXIQ_QUEUES` | Comma-separated queues (default `default`) |
| `FLEXIQ_LISTEN` | Executor attach address, or `unix:/run/flexiq.sock` |
| `FLEXIQ_DASHBOARD` | Dashboard address |
| `FLEXIQ_DASHBOARD_AUTH` | `off` (default) or `session` |
| `FLEXIQ_MAINTENANCE` | `off` to leave retention to another replica |
| `FLEXIQ_GRPC_LISTEN` | gRPC address, or `unix:/run/flexiq-grpc.sock` |

At least one of `FLEXIQ_LISTEN`, `FLEXIQ_DASHBOARD`, `FLEXIQ_WEBHOOK_LISTEN` or
`FLEXIQ_GRPC_LISTEN` must be set.

Postgres, Redis and gRPC are cargo features:

```bash
cargo build -p flexiq-server --features postgres
cargo build -p flexiq-server --features redis
cargo build -p flexiq-server --features grpc
```

## The gRPC door

`FLEXIQ_GRPC_LISTEN` binds a fourth listener beside the other three, serving
`flexiq.v1.ProducerService` — enqueue, read, cancel, count — over gRPC and over
plain HTTP with JSON bodies, alongside `grpc.health.v1` and server reflection.
Reflection is seeded from `contracts/descriptor.binpb`, the descriptor the buf
gate builds and commits, so a client needs no `.proto` on hand:

```bash
FLEXIQ_DSN=/var/lib/flexiq/app.db \
FLEXIQ_NAMESPACE=prod \
FLEXIQ_GRPC_LISTEN=127.0.0.1:50051 \
flexiq-server

grpcurl -plaintext -H "authorization: Bearer $FLEXIQ_TOKEN" localhost:50051 list
grpcurl -plaintext -H "authorization: Bearer $FLEXIQ_TOKEN" \
  -d '{"task_name":"send_email","raw":"","options":{"queue":"emails"}}' \
  localhost:50051 flexiq.v1.ProducerService/Enqueue
```

### The same RPCs over plain HTTP

The producer service is also served as JSON on that same listener, so a client
with no protobuf toolchain — a shell script, `curl`, a language with no codec —
can enqueue:

```bash
curl -X POST http://localhost:50051/v1/jobs \
  -H "authorization: Bearer $FLEXIQ_TOKEN" -H "content-type: application/json" \
  -d '{"taskName":"send_email","structured":{"args":["a@b.c"]},"options":{"queue":"emails"}}'
```

`POST /v1/jobs`, `POST /v1/jobs:batchEnqueue`, `GET /v1/jobs`,
`GET /v1/jobs/{job_id}`, `POST /v1/jobs/{job_id}:cancel`,
`GET /v1/queues/{queue}/stats` and `GET /v1/stats` — the six producer RPCs, with
`GET` served for the read-only ones and nothing else. Same credential, same
handlers, same 4 MiB cap; failures carry the same `reason` in the standard
`google.rpc` JSON body. The executor API is not exposed this way, and there is
no stream to transcode.

### The credential

Callers present a **scoped API token**, as `authorization: Bearer <token>`. It
gates everything on the listener except the two `grpc.health.v1` RPCs, which stay
open because a Kubernetes `grpc:` probe has no way to send metadata; reflection
is gated with the rest.

Tokens live in the database, not in the environment. Mint one wherever the server
can reach its DSN — no listener has to be running:

```bash
export FLEXIQ_DSN=postgres://user:pass@host/db
export FLEXIQ_NAMESPACE=prod

# The token goes to stdout and the summary to stderr, so a command substitution
# captures the credential alone while the confirmation still reaches your
# terminal.
export FLEXIQ_TOKEN=$(flexiq-server token create --name my-producer --scope produce)
# fqt_9f2c1ab74e05d366.mZ1qgWx8yQ4nR7t0KcV2sJdH6bPfLuA3eXyN5rTiOk

flexiq-server token list
flexiq-server token revoke 9f2c1ab74e05d366
```

The token is printed once; only a SHA-256 digest of it is stored, so it is not
recoverable from the row. The dashboard does the same three things under
Configuration → gRPC tokens, behind its admin role. **A revoked token fails on
its next call, with no restart** — the door reads the row per call rather than
caching a verdict.

`--scope produce` opens `flexiq.v1` (submit, read, cancel) and `--scope execute`
opens `flexiq.executor.v1` (claim work, report on it). They are not a hierarchy:
a token that can poll for work must not be able to enqueue it. Every token
expires — 90 days by default, 365 at most — and the server warns on use at 30, 20
and 10 days remaining.

Three things it refuses. **Every call on a door with no token minted**, loopback
and Unix sockets included, because a listener with no credential provisioned is a
misconfiguration rather than a permission grant. **A missing `FLEXIQ_NAMESPACE`**,
because an unset namespace means "every namespace" to an id-addressed read and
"only the unnamespaced rows" to a dequeue, and a wire that can express that is one
bug away from a cross-tenant read — a token is bound at mint time to the namespace
the minting process serves, and refused by a listener serving another. And
**`FLEXIQ_GRPC_LISTEN` itself** on a binary built without the `grpc` feature, so a
misconfigured deployment fails at boot instead of serving nothing on the port its
clients dial.

Health is answered out of storage — the same question `/readiness` answers — so
an orchestrator can take a replica that cannot reach its database out of
rotation. TLS is not terminated here; a bearer token proves who is calling and
does not encrypt the connection, so that belongs to a sidecar proxy or a service
mesh.

## Container image

`docker/scheduler.Dockerfile` builds a distroless image around a static binary —
no libc, no interpreter, nothing to match against the app's runtime — for
`linux/amd64` and `linux/arm64`. Postgres and Redis are compiled in, so one
image covers every backend and the DSN picks at runtime.

```bash
docker build -f docker/scheduler.Dockerfile -t flexiq-server .
docker run --rm -p 7777:7777 \
  -e FLEXIQ_DSN=postgres://user:pass@host/db \
  -e FLEXIQ_LISTEN=0.0.0.0:7777 \
  -e FLEXIQ_ATTACH_TOKEN="$ATTACH_TOKEN" \
  flexiq-server
```

Releases publish the same build as a multiarch manifest at
`ghcr.io/byteveda/flexiq-server:<version>`.

## Behaviour worth knowing

- **The scheduler starts on the first attach.** With no executor attached there
  is nothing to dispatch to, and claiming jobs anyway would fail them retryably
  once placement timed out.
- **The attach listener defaults to loopback.** An attach connection dispatches
  code, so a non-loopback bind refuses to start unless `FLEXIQ_ATTACH_TOKEN`
  is set. A Unix socket skips the check — the filesystem is the boundary.
- **An unauthenticated dashboard may not be reachable off-host** unless
  `FLEXIQ_ALLOW_INSECURE=1` says so deliberately.
- **The dashboard SPA is embedded at build time** when one has been built
  (`pnpm --dir dashboard build`). `FLEXIQ_DASHBOARD_ASSETS=/path` overrides it
  at runtime; with neither, the dashboard serves a page saying so.
- **Dashboard state is shared with every SDK dashboard.** Users, sessions,
  webhooks, and overrides live under the same settings keys, so a session
  created by one is accepted by the others against the same backend.
