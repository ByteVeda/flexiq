# GHCR package page shows "No description provided"

## Problem

`ghcr.io/byteveda/flexiq-server:latest` renders with no description, although
`docker/scheduler.Dockerfile` already sets
`LABEL org.opencontainers.image.description`. GHCR reads a *multiarch* package's
description from the **index annotations**, not from the child images' config
labels — the label only feeds a single-architecture tag.

## Why adding `--annotation` alone is not enough

`publish-server.yml` builds each architecture with `load: true` and then
`docker push`es it out of the daemon, which rewrites the manifest to Docker
media types. Reproduced locally against a throwaway registry:

| children | `imagetools create --annotation index:...` result |
| --- | --- |
| `application/vnd.docker.distribution.manifest.v2+json` | `...manifest.list.v2+json`, `annotations: null` — **silently dropped** |
| `application/vnd.oci.image.manifest.v1+json` | `...image.index.v1+json`, annotations present |

So the per-architecture tags have to reach the registry as OCI media types.

## Changes

- [x] `publish-server.yml` build job: push the per-architecture tag with a buildx
      registry export (`type=image,push=true,oci-mediatypes=true`,
      `provenance/sbom: false`) re-run from the build cache, instead of
      `docker push`. The smoke test still gates it, so nothing untested is
      published.
- [x] `publish-server.yml` manifest job: read the child image's OCI labels back
      off the registry and mirror every one onto the index as an
      `index:<key>=<value>` annotation — the Dockerfile stays the only place the
      strings live.
- [x] `publish-server.yml` verify step: fail the release when the index carries
      no `org.opencontainers.image.description` annotation (also catches a
      regression back to Docker media types, which have no annotations field).
- [x] `ci-server-image.yml`: assert the built image carries the five OCI labels,
      so a dropped LABEL reds a PR instead of surfacing as a blank package page
      one release later.
- [x] `docker/scheduler.Dockerfile`: note that the labels are mirrored onto the
      index at publish time.

## Review

Verified with docker buildx 0.36.1 against a local `registry:2`: an OCI
single-platform push, the `{{ json .Image.Config.Labels }}` read-back, and the
annotated `imagetools create` round-trip (`org.opencontainers.image.description`
present on the pushed index). The Docker-media-type case was reproduced to
confirm the silent drop.

Not fixable from here: the already-published `1.0.0`/`latest` index keeps its
blank description until the next release re-publishes it, because its children
are Docker media types. The package is also still **Private** — GHCR visibility
has no REST endpoint, it is a package-settings toggle.
