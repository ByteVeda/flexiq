# taskito-server

The Taskito scheduler, executor attach listener, and dashboard in one binary,
with no language runtime.

Task bodies stay in the app's own container: executors dial in over the worker
frame protocol and run them there, so this image is small and identical for
every SDK. It also serves the dashboard SPA, which the SDK packages otherwise
each ship a copy of.

```bash
TASKITO_DSN=/var/lib/taskito/app.db \
TASKITO_LISTEN=127.0.0.1:7777 \
TASKITO_DASHBOARD=127.0.0.1:8080 \
taskito-server
```

## Configuration

Environment only — the binary takes no flags beyond `--help` and `--version`.
Run `taskito-server --help` for the full list; the essentials:

| Variable | Meaning |
|---|---|
| `TASKITO_DSN` | Storage connection string (required) |
| `TASKITO_BACKEND` | `sqlite` \| `postgres` \| `redis`; defaults to the DSN's scheme |
| `TASKITO_QUEUES` | Comma-separated queues (default `default`) |
| `TASKITO_LISTEN` | Executor attach address, or `unix:/run/taskito.sock` |
| `TASKITO_DASHBOARD` | Dashboard address |
| `TASKITO_DASHBOARD_AUTH` | `off` (default) or `session` |
| `TASKITO_MAINTENANCE` | `off` to leave retention to another replica |

At least one of `TASKITO_LISTEN` or `TASKITO_DASHBOARD` must be set.

Postgres and Redis are cargo features:

```bash
cargo build -p taskito-server --features postgres
cargo build -p taskito-server --features redis
```

## Behaviour worth knowing

- **The scheduler starts on the first attach.** With no executor attached there
  is nothing to dispatch to, and claiming jobs anyway would fail them retryably
  once placement timed out.
- **The attach listener is loopback-only.** An attach connection dispatches
  code, and the handshake does not carry a credential yet, so a non-loopback
  bind refuses to start. Reach it over a Unix socket or the pod network.
- **An unauthenticated dashboard may not be reachable off-host** unless
  `TASKITO_ALLOW_INSECURE=1` says so deliberately.
- **The dashboard SPA is embedded at build time** when one has been built
  (`pnpm --dir dashboard build`). `TASKITO_DASHBOARD_ASSETS=/path` overrides it
  at runtime; with neither, the dashboard serves a page saying so.
- **Dashboard state is shared with every SDK dashboard.** Users, sessions,
  webhooks, and overrides live under the same settings keys, so a session
  created by one is accepted by the others against the same backend.
