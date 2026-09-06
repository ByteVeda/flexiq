# flexiq-server container image

FlexiQ publishes the scheduler image at
`ghcr.io/byteveda/flexiq-server`. The image contains the standalone
`flexiq-server` binary and can serve executors from any supported SDK.

```bash
docker pull ghcr.io/byteveda/flexiq-server:0.21.0
docker run --rm \
  -e FLEXIQ_DSN=postgres://user:pass@host/db \
  -e FLEXIQ_ATTACH_TOKEN="$ATTACH_TOKEN" \
  ghcr.io/byteveda/flexiq-server:0.21.0
```

See the [server configuration](../crates/flexiq-server/README.md) for the
runtime environment variables and the
[deployment guide](https://docs.byteveda.org/flexiq/python/guides/operations/deployment)
for complete Docker, Compose, and Kubernetes examples.

## Published tags and platforms

Each server release publishes two supported pull references:

| Reference | Meaning |
| --- | --- |
| `:<version>` (for example, `:0.21.0`) | A version-specific release reference. Use this for controlled deployments. |
| `:latest` | The newest server release. This tag moves whenever a release is published, so avoid it where reproducible deployment or rollback matters. |

Both references are manifest lists for `linux/amd64` and `linux/arm64`; Docker
selects the matching image automatically. FlexiQ does not currently publish
moving major tags such as `:0`, moving minor tags such as `:0.21`, or commit
tags such as `:sha-...`.

For the strongest reproducibility, resolve the versioned manifest to its digest
and deploy the digest reference:

```bash
docker buildx imagetools inspect ghcr.io/byteveda/flexiq-server:0.21.0
docker pull ghcr.io/byteveda/flexiq-server@sha256:<manifest-digest>
```

## Distroless runtime

The runtime image is based on `gcr.io/distroless/static-debian12:nonroot`. It
runs as a non-root user and contains the static server binary, but no shell,
package manager, or language runtime. Commands such as
`docker exec <container> sh` therefore do not work. Use the server's health and
diagnostic endpoints, container logs, or a separate ephemeral debug container
instead of installing tools in the running image.

The image entrypoint is `/usr/local/bin/flexiq-server`. It exposes the attach
listener on port 7777, the dashboard on port 8080, and gRPC on port 50051;
exposing a port in the image does not publish it on the host.

## Integrity and provenance

The release workflow smoke-tests the server version, publishes OCI image
labels, and verifies that the multi-architecture manifest contains both
supported platforms. It currently disables BuildKit provenance and SBOM
attestations, and the repository does not sign the published image. As a
result, consumers can pin and compare registry digests, but cannot yet verify a
FlexiQ-produced signature or build attestation.
