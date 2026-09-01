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
| `FLEXIQ_GRPC_TOKEN` | Shared secret gRPC callers present; required off loopback |

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
`flexiq.v1.ProducerService` — enqueue, read, cancel, count — alongside
`grpc.health.v1` and server reflection. Reflection is seeded from
`contracts/descriptor.binpb`, the descriptor the buf gate builds and commits, so
a client needs no `.proto` on hand:

```bash
FLEXIQ_DSN=/var/lib/flexiq/app.db \
FLEXIQ_NAMESPACE=prod \
FLEXIQ_GRPC_LISTEN=127.0.0.1:50051 \
flexiq-server

grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -d '{"task_name":"send_email","raw":"","options":{"queue":"emails"}}' \
  localhost:50051 flexiq.v1.ProducerService/Enqueue
```

### The credential

`FLEXIQ_GRPC_TOKEN` is the secret a caller presents, as
`authorization: Bearer <token>`. It gates everything on the listener except
`grpc.health.v1`, which stays open because a Kubernetes `grpc:` probe has no way
to send metadata; reflection is gated with the rest.

```bash
# Exported, not prefixed: a `VAR=x cmd` assignment reaches that one command,
# and the client below needs the same value.
export FLEXIQ_GRPC_TOKEN=$(openssl rand -base64 32)

FLEXIQ_GRPC_LISTEN=0.0.0.0:50051 ... flexiq-server

grpcurl -plaintext -H "authorization: Bearer $FLEXIQ_GRPC_TOKEN" \
  localhost:50051 list
```

It is a **shared secret**, so it cannot be revoked for one client, carries no
scope and leaves no audit trail. Treat the listener as reachable only from a
trusted network until scoped tokens land, and terminate TLS in front of it.

Three things it refuses. **A non-loopback bind with no `FLEXIQ_GRPC_TOKEN`**,
because a port that accepts `Enqueue` does not serve an unauthenticated network
— set the token, use loopback, or use `unix:/run/flexiq-grpc.sock`, where the
socket's `0660` mode is the boundary. **A missing `FLEXIQ_NAMESPACE`**, because
an unset namespace means "every namespace" to an id-addressed read and "only the
unnamespaced rows" to a dequeue, and a wire that can express that is one bug
away from a cross-tenant read. And **`FLEXIQ_GRPC_LISTEN` itself** on a binary
built without the `grpc` feature, so a misconfigured deployment fails at boot
instead of serving nothing on the port its clients dial.

Health is answered out of storage — the same question `/readiness` answers — so
an orchestrator can take a replica that cannot reach its database out of
rotation. TLS is not terminated here either; that belongs to a sidecar proxy or
a service mesh.

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
