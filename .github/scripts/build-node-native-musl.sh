#!/bin/sh
# Build the Node native addon inside the napi-rs Alpine cross image.
#
# Kept out of the workflow so it can be shellchecked and run locally against the
# same image, which is the only way this path gets exercised — no PR job builds
# musl.
set -eu

REPO_ROOT=$(CDPATH='' cd -- "$(dirname -- "$0")/../.." && pwd)
ALPINE_MIRROR=https://dl-cdn.alpinelinux.org/alpine

# The image bundles Node 18, but @napi-rs/cli v3 needs 20+. Its apk points at
# edge, whose Node will not relocate against the older base, so repin apk to the
# image's own Alpine release and put that Node ahead of the bundled one. Derived
# rather than hardcoded so a rebased image stays ABI-consistent.
alpine_branch="v$(cut -d. -f1,2 /etc/alpine-release)"
printf '%s/%s/main\n%s/%s/community\n' \
    "$ALPINE_MIRROR" "$alpine_branch" "$ALPINE_MIRROR" "$alpine_branch" \
    >/etc/apk/repositories

# perl and build-base: vendored openssl-sys and pq-src build libpq from source.
apk add --no-cache perl build-base nodejs npm
PATH=/usr/bin:$PATH
export PATH

# The image ships Rust 1.82; some dependencies need edition 2024.
rustup update stable
rustup default stable

cd "$REPO_ROOT/sdks/node"

# Taken from `packageManager` so the pin cannot drift from the repo's own.
# --force overwrites the pnpm the image preinstalls, which would otherwise EEXIST.
pnpm_version=$(node -p "require('./package.json').packageManager.split('@')[1]")
npm install -g "pnpm@${pnpm_version}" --force

pnpm install --frozen-lockfile
pnpm run build:native
