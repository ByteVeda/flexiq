#!/usr/bin/env bash
# Format, lint and descriptor-drift gate for the FlexiQ wire contract.
#
# CI runs this file and so does a developer, so there is one definition of
# "the protos are in order" rather than two that drift.
#
# Usage:
#   scripts/proto-check.sh          # verify; touches nothing
#   scripts/proto-check.sh --fix    # rewrite formatting and the descriptor
#
# The breaking check is deliberately not here: it needs a baseline from
# `master`, which only CI has. See .github/workflows/ci-proto.yml.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
module="$repo_root/contracts/proto"
descriptor="$repo_root/contracts/descriptor.binpb"
pin="$(tr -d '[:space:]' <"$repo_root/contracts/BUF_VERSION")"

fix=false
case "${1-}" in
  --fix) fix=true ;;
  "") ;;
  *)
    echo "usage: $0 [--fix]" >&2
    exit 2
    ;;
esac

if ! command -v buf >/dev/null 2>&1; then
  echo "error: buf is not on PATH. Install the pinned version:" >&2
  echo "  curl -sSL https://github.com/bufbuild/buf/releases/download/v$pin/buf-\$(uname -s)-\$(uname -m).tar.gz | tar -xz -C ~/.local --strip-components=1" >&2
  exit 1
fi

# Not fatal: a dev with a newer buf should still be able to lint. But the
# descriptor is only byte-stable for a fixed version, so a mismatch is the
# first thing to suspect when the drift check fails on an untouched tree.
local_version="$(buf --version)"
if [ "$local_version" != "$pin" ]; then
  echo "warning: buf $local_version is not the pinned $pin (contracts/BUF_VERSION)." >&2
  echo "warning: the descriptor check below may disagree with CI." >&2
fi

cd "$module"

if [ "$fix" = true ]; then
  buf format --write
  buf lint
  buf build --as-file-descriptor-set -o "$descriptor"
  echo "proto-check: formatted, linted, descriptor regenerated."
  exit 0
fi

buf format --diff --exit-code
buf lint

# Build to a scratch path and compare, rather than regenerating in place and
# asking git. A `git diff` check reports nothing when the descriptor is
# untracked, and it cannot run at all on a tree with unrelated staged changes.
regenerated="$(mktemp)"
trap 'rm -f "$regenerated"' EXIT
buf build --as-file-descriptor-set -o "$regenerated"

if [ ! -f "$descriptor" ]; then
  echo "error: contracts/descriptor.binpb is missing. Run: $0 --fix" >&2
  exit 1
fi

if ! cmp -s "$regenerated" "$descriptor"; then
  echo "error: contracts/descriptor.binpb is stale — it does not match the protos." >&2
  echo "error: regenerate it with: $0 --fix" >&2
  exit 1
fi

echo "proto-check: format, lint and descriptor all clean."
