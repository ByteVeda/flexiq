# flexiq-server Helm chart

This directory contains the FlexiQ server chart. OCI publishing begins with the
first server release after the publishing workflow change. Until that version
is publicly visible in GHCR, install the chart from a matching repository tag.
After publication, the OCI command is:

```bash
helm install flexiq oci://ghcr.io/byteveda/charts/flexiq-server \
  --version <released-version> \
  --set storage.dsn='postgres://flexiq:secret@postgres:5432/myapp' \
  --set attach.token="$(openssl rand -base64 32)"
```

The chart version and its default server image tag move together. Pin
`--version` so upgrades happen deliberately.

The canonical operator guide covers installation, listener roles, maintenance
ownership across replicas, sidecar injection, probes, secret rotation, and the
KEDA manifests:

**[Deploy FlexiQ on Kubernetes](https://docs.byteveda.org/flexiq/python/operate/kubernetes)**

With `grpc.enabled=true` the server also serves a native KEDA external scaler
on the chart's `<fullname>-grpc` Service (port `grpc.port`), authenticated with an `inspect` token. The manifests are
[`deploy/keda/scaled-object-external.yaml`](../../keda/scaled-object-external.yaml)
and its `-stream` variant, and
**[Scaling](https://docs.byteveda.org/flexiq/server/operate/scaling#external-scaler)**
covers the token and trigger metadata.

Chart defaults and source-level value comments live in [`values.yaml`](values.yaml).
