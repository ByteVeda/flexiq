# taskito-server

The Taskito scheduler, executor attach listener, and dashboard in one binary,
with no language runtime.

Task bodies stay in the app's own container: executors dial in over the worker
frame protocol and run them there, so this image is small and identical for
every SDK. It also serves the dashboard SPA, which the SDK packages otherwise
each ship a copy of.

```bash
FLEXIQ_DSN=/var/lib/taskito/app.db \
FLEXIQ_LISTEN=127.0.0.1:7777 \
FLEXIQ_DASHBOARD=127.0.0.1:8080 \
taskito-server
```

## Configuration

Environment only — the binary takes no flags beyond `--help` and `--version`.
Run `taskito-server --help` for the full list; the essentials:

| Variable | Meaning |
|---|---|
| `FLEXIQ_DSN` | Storage connection string (required) |
| `FLEXIQ_BACKEND` | `sqlite` \| `postgres` \| `redis`; defaults to the DSN's scheme |
| `FLEXIQ_QUEUES` | Comma-separated queues (default `default`) |
| `FLEXIQ_LISTEN` | Executor attach address, or `unix:/run/flexiq.sock` |
| `FLEXIQ_DASHBOARD` | Dashboard address |
| `FLEXIQ_DASHBOARD_AUTH` | `off` (default) or `session` |
| `FLEXIQ_MAINTENANCE` | `off` to leave retention to another replica |

At least one of `FLEXIQ_LISTEN` or `FLEXIQ_DASHBOARD` must be set.

Postgres and Redis are cargo features:

```bash
cargo build -p taskito-server --features postgres
cargo build -p taskito-server --features redis
```

## Container image

`docker/scheduler.Dockerfile` builds a distroless image around a static binary —
no libc, no interpreter, nothing to match against the app's runtime — for
`linux/amd64` and `linux/arm64`. Postgres and Redis are compiled in, so one
image covers every backend and the DSN picks at runtime.

```bash
docker build -f docker/scheduler.Dockerfile -t taskito-server .
docker run --rm -p 7777:7777 \
  -e FLEXIQ_DSN=postgres://user:pass@host/db \
  -e FLEXIQ_LISTEN=0.0.0.0:7777 \
  -e FLEXIQ_ATTACH_TOKEN="$ATTACH_TOKEN" \
  taskito-server
```

Releases publish the same build as a multiarch manifest at
`ghcr.io/byteveda/taskito-server:<version>`.

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
