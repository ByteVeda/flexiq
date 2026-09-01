# syntax=docker/dockerfile:1

# flexiq-server with no language runtime: one static musl binary on distroless,
# so the same image schedules for apps written in any SDK. glibc/musl variants
# would be indistinguishable here — nothing in the image links libc.
#
#   docker build -f docker/scheduler.Dockerfile -t flexiq-server .
#
# Releases additionally pass `--build-arg VERSION=$(node scripts/version.mjs
# --current)` for the OCI label, and build one architecture per native runner
# before merging the two into a manifest list — see publish-server.yml. The
# binary's own `--version` always comes from the workspace Cargo.toml.

# --- dashboard ---------------------------------------------------------------
# The SPA is embedded into the binary at compile time
# (crates/flexiq-server/build.rs), so it has to exist before cargo runs.
FROM node:22-alpine AS dashboard
ENV COREPACK_ENABLE_DOWNLOAD_PROMPT=0
WORKDIR /src/dashboard
# Manifest first: dependency installs then survive every source-only edit.
COPY dashboard/package.json dashboard/pnpm-lock.yaml ./
RUN corepack enable && pnpm install --frozen-lockfile
COPY dashboard/ ./
RUN pnpm exec tsr generate && pnpm exec vite build --outDir dist --emptyOutDir

# --- binary ------------------------------------------------------------------
# Alpine builds musl natively on both architectures, so the static binary needs
# no cross toolchain — each publish runner compiles for its own platform.
FROM rust:1-alpine AS builder
# build-base: the C toolchain the bundled SQLite, libpq and OpenSSL sources
# need. perl + linux-headers: OpenSSL's configure. git: the dagron-core git
# dependency of flexiq-workflows.
RUN apk add --no-cache build-base perl linux-headers pkgconfig git
# Cargo's downloader hits "HTTP2 framing layer" errors often enough to fail a
# release build; the CI workflows pin the same two settings.
ENV CARGO_HTTP_MULTIPLEXING=false \
    CARGO_NET_RETRY=10
WORKDIR /src
COPY . .
COPY --from=dashboard /src/dashboard/dist ./dashboard/dist
# Postgres and Redis are compiled in so one image covers every backend; the DSN
# picks at runtime. `grpc` too, so FLEXIQ_GRPC_LISTEN turns the role on rather
# than being refused by a binary that has no gRPC server to start.
#
# The binary is copied out of the cache mount because cache mounts are not part
# of the resulting layer, and the PT_INTERP check fails the build rather than
# the container: distroless/static ships no dynamic loader.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    FLEXIQ_DASHBOARD_ASSETS_DIR=/src/dashboard/dist \
    cargo build --release --locked -p flexiq-server --features postgres,redis,grpc \
    && cp target/release/flexiq-server /flexiq-server \
    && if readelf -l /flexiq-server | grep -q INTERP; then \
         echo "flexiq-server is dynamically linked — distroless/static cannot run it" >&2; \
         exit 1; \
       fi

# --- image -------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS runtime
# Overridden per release; a literal here would outrank scripts/version.mjs,
# which is why `--check` guards against one.
ARG VERSION=dev
# publish-server.yml copies these onto the multiarch index as OCI annotations:
# GHCR reads a manifest list's title and description from the index and never
# from the child images' config labels, so a label alone leaves the package page
# blank. ci-server-image.yml asserts they are all still set.
LABEL org.opencontainers.image.title="flexiq-server" \
      org.opencontainers.image.description="FlexiQ scheduler, executor attach listener, and dashboard" \
      org.opencontainers.image.source="https://github.com/ByteVeda/flexiq" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"
COPY --from=builder /flexiq-server /usr/local/bin/flexiq-server
# Attach listener, dashboard and gRPC. All stay off until FLEXIQ_LISTEN /
# FLEXIQ_DASHBOARD / FLEXIQ_GRPC_LISTEN are set, so this documents the ports
# rather than opening them.
EXPOSE 7777 8080 50051
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/flexiq-server"]
