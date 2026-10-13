#!/bin/sh
# Print an image's OCI labels as buildx annotations, one `<level>:<key>=<value>`
# per line.
#
# GHCR renders a version page from the manifest's annotations and ignores the
# config labels of an OCI image, so every published manifest — each
# per-architecture tag and the index over them — must carry the labels again as
# annotations. The Dockerfile stays the one place the strings live.
#
# Usage: oci-label-annotations.sh <manifest|index> <labels-json>
set -eu

if [ "$#" -ne 2 ]; then
    echo "usage: $0 <manifest|index> <labels-json>" >&2
    exit 2
fi
level=$1
labels=$2

case "$level" in
    manifest | index) ;;
    *)
        echo "unknown annotation level '$level'" >&2
        exit 2
        ;;
esac

# A missing description publishes a blank package page without any error.
if ! printf '%s' "$labels" |
    jq -e '(.["org.opencontainers.image.description"] // "") != ""' >/dev/null; then
    echo "image labels carry no org.opencontainers.image.description" >&2
    exit 1
fi

printf '%s' "$labels" |
    jq -r --arg level "$level" 'to_entries[] | "\($level):\(.key)=\(.value)"'
