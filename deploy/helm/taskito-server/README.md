# taskito-server

The Taskito scheduler, dashboard, and executor sidecar injector — one binary,
one image, up to three roles.

```bash
helm install taskito ./deploy/helm/taskito-server \
  --set storage.dsn='postgres://taskito:secret@postgres:5432/myapp' \
  --set attach.token="$(openssl rand -base64 32)"
```

Task bodies stay in your app's container. This chart deploys the half that holds
the database credentials; executors dial in from your own pods and run the
tasks, so the app image needs no DSN and no inbound port.

## Roles

| Value | Default | What it runs |
|---|---|---|
| `attach.enabled` | `true` | The attach listener and, once an executor attaches, the scheduler |
| `dashboard.enabled` | `true` | The dashboard SPA and its JSON API |
| `webhook.enabled` | `false` | The mutating admission webhook that injects executor sidecars |

At least one must be on. `storage.dsn` is required unless the release runs the
webhook alone — that role rewrites pod specs and reads no jobs.

Storage must be Postgres or Redis. SQLite is a local file with a single writer,
this chart mounts no volume for it, and a second replica would not see the first
one's jobs; the chart refuses the DSN rather than shipping a database that
disappears with the pod.

The chart refuses to render on combinations the server would reject at boot: an
attach listener with no token, an unauthenticated dashboard without
`dashboard.allowInsecure`, cert-manager without the CRDs. The failure names the
value to change.

## Sidecar injection

With `webhook.enabled=true`, annotating a pod template is all it takes. This
example points at the attach listener the same release runs, so it assumes
`attach.enabled=true` (the default):

```yaml
metadata:
  annotations:
    taskito.dev/inject: "true"
    taskito.dev/attach: "taskito-taskito-server-attach.default.svc:7777"
    taskito.dev/command: "taskito executor --app myapp:queue"
    taskito.dev/slots: "4"
    taskito.dev/token-secret: "taskito-taskito-server-config"
    taskito.dev/token-key: "attach-token"
```

An injector can also run on its own — no listener, no dashboard, no database —
alongside schedulers deployed elsewhere:

```bash
helm install taskito-injector ./deploy/helm/taskito-server \
  --set attach.enabled=false --set dashboard.enabled=false \
  --set webhook.enabled=true
```

Annotations then name whichever scheduler the workload should reach, and the
Secret holding its token:

```yaml
metadata:
  annotations:
    taskito.dev/inject: "true"
    taskito.dev/attach: "taskito-eu.platform.svc:7777"
    taskito.dev/command: "taskito executor --app myapp:queue"
    taskito.dev/token-secret: "taskito-eu-attach"
    taskito.dev/token-key: "token"
```

The injected container reuses **the pod's own image reference**, so the image is
already on the node and nothing new is pulled — the same trick OpenTelemetry
auto-instrumentation uses, and the reason this works for any language.

| Annotation | Required | Meaning |
|---|---|---|
| `taskito.dev/inject` | yes | `true` opts the pod in |
| `taskito.dev/attach` | yes | Scheduler address: `host:port`, `:port`, or `unix:/path` |
| `taskito.dev/command` | yes | Argv that runs an executor. A JSON array when an argument contains spaces, otherwise whitespace-split |
| `taskito.dev/slots` | no | Concurrent jobs (default `1`) |
| `taskito.dev/container` | no | Container to copy the image and environment from (default: the first) |
| `taskito.dev/token-secret` | no | Secret holding the attach token |
| `taskito.dev/token-key` | no | Key within it (default `token`) |
| `taskito.dev/socket-volume` | no | Volume carrying the socket; required for a `unix:` address |
| `taskito.dev/inherit-env` | no | `false` to skip copying the source container's `env`/`envFrom` |

Notes worth knowing:

- The sidecar **inherits the app container's environment** by default, so a
  handler reading the same config as the app keeps working. `FLEXIQ_ATTACH`,
  `FLEXIQ_SLOTS` and `FLEXIQ_ATTACH_TOKEN` are always the injector's, never an
  inherited value.
- Injection is idempotent. A pod that already has a `taskito-executor` container
  is admitted unchanged — a second sidecar would silently double its slots.
- A pod that opts in and gets an annotation wrong is **rejected**, with the
  annotation named in the `kubectl` error. Admitting it would produce a
  deployment that looks healthy and never runs a job.
- `failurePolicy` defaults to `Ignore` so a wedged injector cannot stop pod
  creation cluster-wide. Move to `Fail` once a workload depends on the sidecar.

## Certificates

The webhook is HTTPS-only, because that is the only way the API server will call
it.

- **Default** — the chart generates a self-signed CA and leaf and writes the CA
  into the `MutatingWebhookConfiguration`. No external dependency. The pair is
  read back from the existing Secret on upgrade, so the API server never ends up
  trusting a CA the pod stopped serving.
- **cert-manager** — set `webhook.certManager.enabled=true`. The chart ships a
  `Certificate` (and a self-signed `Issuer`, unless `webhook.certManager.issuerRef`
  names one) and lets the `inject-ca-from` annotation fill in the bundle.

A rotated certificate takes effect on restart: the server reads the PEM files
once at boot, and the pod template carries a checksum of them so a changed pair
rolls the Deployment.

## Probes

Liveness uses `/health`, which is never gated and reports only that the process
is up. Readiness uses `/readiness`, which checks that storage answers.

`/readiness` is otherwise gated alongside `/metrics`, so a kubelet probe — which
carries no credential, and cannot be given one, since a probe header is a
literal string in the manifest — would get `401` and the pod would never go
Ready. `dashboard.publicReadiness` (on by default) sets
`FLEXIQ_DASHBOARD_PUBLIC_READINESS`, which answers that one route without a
credential. `/metrics` stays gated either way.

What that publishes to anything that can reach the port: whether storage
answers, and how many workers are registered. Turn it off with
`--set dashboard.publicReadiness=false` and readiness falls back to `/health`,
which always passes while the process is alive.

## Multiple replicas

Replicas coordinate through storage: a job is claimed by exactly one, retention
sweeps under a lease only one holds, and a dead worker's in-flight jobs are
rescued by exactly one survivor. Set `maintenance: false` on extra replicas if
you would rather one release own the sweeps outright.
