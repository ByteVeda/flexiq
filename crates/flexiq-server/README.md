# flexiq-server

The FlexiQ scheduler, executor attach listener, dashboard, admission webhook and
gRPC door in one binary, with no language runtime.

Task bodies stay in the app's own container: executors dial in over the worker
frame protocol and run them there, so this image is small and identical for
every SDK. It also serves the dashboard SPA, which the SDK packages otherwise
each ship a copy of.

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
| What to scale on, per role | **[Scaling](https://docs.byteveda.org/flexiq/python/operate/server/scaling)** |
| Per-backend backup, and what a restore replays | **[Backup and restore](https://docs.byteveda.org/flexiq/python/operate/backup)** |
| The Helm chart, listener roles, KEDA | **[Kubernetes](https://docs.byteveda.org/flexiq/python/operate/kubernetes)** |

The container image's tags, platforms and provenance are in
[`docker/README.md`](../../docker/README.md).

## Building it

Environment only — the binary takes no flags beyond `--help`, `--version` and
the `token` subcommand. `flexiq-server --help` prints every variable it reads.

At least one of `FLEXIQ_LISTEN`, `FLEXIQ_DASHBOARD`, `FLEXIQ_WEBHOOK_LISTEN` or
`FLEXIQ_GRPC_LISTEN` must be set, and every role but the webhook needs
`FLEXIQ_DSN`.

Postgres, Redis and gRPC are cargo features. The published image has all three
compiled in; a local build enables what it needs:

```bash
cargo build -p flexiq-server --features postgres
cargo build -p flexiq-server --features redis
cargo build -p flexiq-server --features grpc
```

`FLEXIQ_GRPC_LISTEN` is rejected at boot on a build without `--features grpc`,
rather than ignored — a deployment that looks configured and serves nothing on
the port its clients dial is the worse failure.

The dashboard SPA is embedded at build time when one has been built
(`pnpm --dir dashboard build`); `FLEXIQ_DASHBOARD_ASSETS=/path` overrides it at
runtime, and with neither the dashboard serves a page saying so.

## Behaviour worth knowing before reading the source

- **The scheduler starts on the first executor attach**, over either door. With
  nothing attached there is nowhere to dispatch to, and claiming jobs anyway
  would fail them retryably once placement timed out.
- **The attach listener defaults to loopback.** An attach connection dispatches
  code, so a non-loopback bind refuses to start unless `FLEXIQ_ATTACH_TOKEN` is
  set. A Unix socket skips the check — the filesystem is the boundary.
- **`FLEXIQ_MAINTENANCE=off` turns off retention sweeps only.** Dead-worker
  reaping and in-flight recovery stay on in every process.
- **Dashboard state is shared with every SDK dashboard.** Users, sessions,
  webhooks and overrides live under the same settings keys, so a session created
  by one is accepted by the others against the same backend.
