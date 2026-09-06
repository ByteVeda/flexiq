# flexiq-server Helm chart

This directory contains the FlexiQ server chart. The chart is consumed directly
from a checked-out FlexiQ release; it is not currently published in a Helm
repository or as an OCI artifact.

The canonical operator guide covers installation, listener roles, maintenance
ownership across replicas, sidecar injection, probes, secret rotation, and the
KEDA manifests:

**[Deploy FlexiQ on Kubernetes](https://docs.byteveda.org/flexiq/python/operate/kubernetes)**

Chart defaults and source-level value comments live in [`values.yaml`](values.yaml).
