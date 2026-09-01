# flexiq-server

The FlexiQ scheduler, dashboard, gRPC door, and executor sidecar injector — one
binary, one image, up to four roles.

```bash
helm install flexiq ./deploy/helm/flexiq-server \
  --set storage.dsn='postgres://flexiq:secret@postgres:5432/myapp' \
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
| `grpc.enabled` | `false` | The gRPC producer door, on its own Service. Needs a credential and `namespace` |

At least one must be on. `storage.dsn` is required unless the release runs the
webhook alone — that role rewrites pod specs and reads no jobs.

Storage must be Postgres or Redis. SQLite is a local file with a single writer,
this chart mounts no volume for it, and a second replica would not see the first
one's jobs; the chart refuses the DSN rather than shipping a database that
disappears with the pod.

`grpc.enabled` requires a credential: either `grpc.token`, which the chart puts
in the Secret it generates under the key `grpc-token`, or `grpc.existingSecret`
naming a Secret you already manage. The key read from that Secret is
`grpc.existingSecretKey`, which defaults to `grpc-token` — set it if yours is
named something else. The door serves the `flexiq.v1` producer service —
enqueue, read, cancel — and the chart binds it `0.0.0.0`, because kubelet dials
the pod IP for every probe type and a loopback listener would never pass
readiness. The server refuses that bind without a credential, so the chart
refuses it a step earlier, where the message can name the value to set.

**Rotating a token needs the pods to restart.** A `secretKeyRef` is resolved
into the container's environment once, at pod start. The chart annotates the pod
template with a checksum of the Secret it generates, so changing `grpc.token` or
`attach.token` rolls the Deployment on the next `helm upgrade`. A Secret the
chart does not own cannot be hashed at render time, so after rotating an
`existingSecret` run `kubectl rollout restart deploy/<release>-flexiq-server`
yourself.

It is a **shared secret**: one string every client presents, granting the whole
producer surface, revocable only by rotating it everywhere. Keep the
`-grpc` Service reachable only from workloads that should be enqueueing.

`grpc.enabled` also requires `namespace`. The gRPC door serves exactly
one namespace, because an unset one means "every namespace" to a read and "only
the unnamespaced rows" to a dequeue — neither is a thing to put on a network
port. The cost is worth naming: jobs written without a namespace are invisible
over gRPC, so producers must be configured with the same value.

The chart refuses to render on combinations the server would reject at boot: an
attach listener with no token, an unauthenticated dashboard without
`dashboard.allowInsecure`, a gRPC door with no namespace or no credential,
cert-manager without the CRDs. The failure names the value to change.

## Sidecar injection

With `webhook.enabled=true`, annotating a pod template is all it takes. This
example points at the attach listener the same release runs, so it assumes
`attach.enabled=true` (the default):

```yaml
metadata:
  annotations:
    flexiq.dev/inject: "true"
    flexiq.dev/attach: "flexiq-flexiq-server-attach.default.svc:7777"
    flexiq.dev/command: "flexiq executor --app myapp:queue"
    flexiq.dev/slots: "4"
    flexiq.dev/token-secret: "flexiq-flexiq-server-config"
    flexiq.dev/token-key: "attach-token"
```

An injector can also run on its own — no listener, no dashboard, no database —
alongside schedulers deployed elsewhere:

```bash
helm install flexiq-injector ./deploy/helm/flexiq-server \
  --set attach.enabled=false --set dashboard.enabled=false \
  --set webhook.enabled=true
```

Annotations then name whichever scheduler the workload should reach, and the
Secret holding its token:

```yaml
metadata:
  annotations:
    flexiq.dev/inject: "true"
    flexiq.dev/attach: "flexiq-eu.platform.svc:7777"
    flexiq.dev/command: "flexiq executor --app myapp:queue"
    flexiq.dev/token-secret: "flexiq-eu-attach"
    flexiq.dev/token-key: "token"
```

The injected container reuses **the pod's own image reference**, so the image is
already on the node and nothing new is pulled — the same trick OpenTelemetry
auto-instrumentation uses, and the reason this works for any language.

| Annotation | Required | Meaning |
|---|---|---|
| `flexiq.dev/inject` | yes | `true` opts the pod in |
| `flexiq.dev/attach` | yes | Scheduler address: `host:port`, `:port`, or `unix:/path` |
| `flexiq.dev/command` | yes | Argv that runs an executor. A JSON array when an argument contains spaces, otherwise whitespace-split |
| `flexiq.dev/slots` | no | Concurrent jobs (default `1`) |
| `flexiq.dev/container` | no | Container to copy the image and environment from (default: the first) |
| `flexiq.dev/token-secret` | no | Secret holding the attach token |
| `flexiq.dev/token-key` | no | Key within it (default `token`) |
| `flexiq.dev/socket-volume` | no | Volume carrying the socket; required for a `unix:` address |
| `flexiq.dev/inherit-env` | no | `false` to skip copying the source container's `env`/`envFrom` |

Notes worth knowing:

- The sidecar **inherits the app container's environment** by default, so a
  handler reading the same config as the app keeps working. `FLEXIQ_ATTACH`,
  `FLEXIQ_SLOTS` and `FLEXIQ_ATTACH_TOKEN` are always the injector's, never an
  inherited value.
- Injection is idempotent. A pod that already has a `flexiq-executor` container
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

Kubernetes takes one probe of each type, so a release without a dashboard falls
back through the roles it does run: the webhook's own `/health` over HTTPS,
then — for a gRPC-only release — a TCP connect for liveness and `grpc.health.v1`
for readiness, which this server answers out of storage exactly as `/readiness`
does. Last of all, a bare TCP connect to the attach port, which speaks no
protocol a kubelet knows.

The `grpc` probe field is only understood from Kubernetes 1.24, and an older API
server rejects it rather than ignoring it. Set `grpc.healthProbe=false` there and
readiness falls back to a `tcpSocket` on the same port — which only says the
process is listening, so a replica that cannot reach storage stays in rotation.

## Multiple replicas

Replicas coordinate through storage: a job is claimed by exactly one, retention
sweeps under a lease only one holds, and a dead worker's in-flight jobs are
rescued by exactly one survivor. Set `maintenance: false` on extra replicas if
you would rather one release own the sweeps outright.
