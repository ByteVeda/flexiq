# flexiq-server

The FlexiQ scheduler, executor attach listener, dashboard, admission webhook,
gRPC door, push dispatcher and trigger listener in one binary, with no language
runtime.

Task bodies stay in the app's own container: executors dial in over the worker
frame protocol and run them there, so this image is small and identical for
every SDK. It also serves the dashboard SPA, which the SDK packages otherwise
each ship a copy of. Push dispatch (`FLEXIQ_PUSH_TARGET_URL`) is the exception
to "executors dial in": the scheduler POSTs a claimed job straight to an
operator-configured URL instead, for a platform that starts a container from
an inbound request and has nothing that could hold an attach connection open.

```bash
FLEXIQ_DSN=/var/lib/flexiq/app.db \
FLEXIQ_LISTEN=127.0.0.1:7777 \
FLEXIQ_DASHBOARD=127.0.0.1:8080 \
cargo run -p flexiq-server
```

## Operator documentation lives on the docs site

A crate README is not where someone running the binary looks, so the operator
half of this one moved. The canonical guides:

| | |
|---|---|
| Roles, every environment variable, the image | **[Server](https://docs.byteveda.org/flexiq/python/operate/server)** |
| Minting, scoping, rotating and revoking API tokens | **[API tokens](https://docs.byteveda.org/flexiq/python/operate/server/tokens)** |
| The producer and executor doors, the JSON facade, TLS | **[The gRPC door](https://docs.byteveda.org/flexiq/python/operate/server/grpc)** |
| Queues, dead letters, workers, periodic tasks and overrides over the wire | **[The admin door](https://docs.byteveda.org/flexiq/python/operate/server/admin)** |
| Env vars, the egress guard, the limits worth knowing first | **[Push dispatch](https://docs.byteveda.org/flexiq/python/operate/server/push)** |
| Webhooks and object-store events as enqueue sources | **[Triggers](https://docs.byteveda.org/flexiq/python/operate/server/triggers)** |
| What to scale on, per role | **[Scaling](https://docs.byteveda.org/flexiq/python/operate/server/scaling)** |
| Per-backend backup, and what a restore replays | **[Backup and restore](https://docs.byteveda.org/flexiq/python/operate/backup)** |
| The Helm chart, listener roles, KEDA | **[Kubernetes](https://docs.byteveda.org/flexiq/python/operate/kubernetes)** |

The container image's tags, platforms and provenance are in
[`docker/README.md`](../../docker/README.md).

## Building it

Environment only — the binary takes no flags beyond `--help`, `--version` and
the `token` subcommand. `flexiq-server --help` prints every variable it reads.

At least one of `FLEXIQ_LISTEN`, `FLEXIQ_DASHBOARD`, `FLEXIQ_WEBHOOK_LISTEN`,
`FLEXIQ_GRPC_LISTEN`, `FLEXIQ_PUSH_TARGET_URL` or `FLEXIQ_TRIGGER_LISTEN` must
be set, and every role
but the webhook needs `FLEXIQ_DSN`. `FLEXIQ_PUSH_TARGET_URL` and
`FLEXIQ_LISTEN` are mutually exclusive — a `Worker` holds exactly one
dispatcher.

Postgres, Redis, gRPC and push dispatch (`http-target`) are cargo features.
**The published image compiles in all four**, so the DSN picks the backend and
the environment picks the roles with nothing to rebuild. A local build enables
what it needs:

```bash
cargo build -p flexiq-server --features postgres
cargo build -p flexiq-server --features redis
cargo build -p flexiq-server --features grpc
cargo build -p flexiq-server --features http-target
```

`FLEXIQ_GRPC_LISTEN` and `FLEXIQ_PUSH_TARGET_URL` are each rejected at boot on
a build without the matching feature, rather than ignored — a deployment that
looks configured and serves (or dials) nothing is the worse failure.

The dashboard SPA is embedded at build time when one has been built
(`pnpm --dir dashboard build`); `FLEXIQ_DASHBOARD_ASSETS=/path` overrides it at
runtime, and with neither the dashboard serves a page saying so.

## Behaviour worth knowing before reading the source

- **The scheduler starts on the first executor attach**, over either door —
  except under push dispatch, which starts eagerly at boot: a push target is a
  URL, not a connection, so there is no "first attach" to wait for. With
  nothing attached and no push target, there is nowhere to dispatch to, and
  claiming jobs anyway would fail them retryably once placement timed out.
- **The attach listener defaults to loopback.** An attach connection dispatches
  code, so a non-loopback bind refuses to start unless `FLEXIQ_ATTACH_TOKEN` is
  set. A Unix socket skips the check — the filesystem is the boundary.
- **Push dispatch has its own egress guard**: a deny-by-default allowlist,
  DNS pinned at connect, loopback/link-local/cloud-metadata refused whatever
  the allowlist says, no redirects. A job dispatched this way has to finish
  inside one HTTP request — Cloud Run caps that at 60 minutes, Lambda at
  15 — and `cancel()` stops FlexiQ waiting on it, not the target's own work.
- **`FLEXIQ_MAINTENANCE=off` turns off retention sweeps only.** Dead-worker
  reaping and in-flight recovery stay on in every process.
- **Dashboard state is shared with every SDK dashboard.** Users, sessions,
  webhooks and overrides live under the same settings keys, so a session created
  by one is accepted by the others against the same backend.
