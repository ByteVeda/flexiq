# flexiq-server Helm chart

This directory contains the FlexiQ server chart. Each server release publishes
it as a versioned OCI artifact alongside the matching server image:

```bash
helm install flexiq oci://ghcr.io/byteveda/charts/flexiq-server \
  --version 2.0.0 \
  --set storage.dsn='postgres://flexiq:secret@postgres:5432/myapp' \
  --set attach.token="$(openssl rand -base64 32)"
```

The chart version and its default server image tag move together. Pin
`--version` so upgrades happen deliberately.

The canonical operator guide covers installation, listener roles, maintenance
ownership across replicas, sidecar injection, probes, secret rotation, and the
KEDA manifests:

**[Deploy FlexiQ on Kubernetes](https://docs.byteveda.org/flexiq/python/guides/operations/kubernetes)**

Chart defaults and source-level value comments live in [`values.yaml`](values.yaml).
