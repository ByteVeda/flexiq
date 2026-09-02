<div align="center">

<p><img src="docs/public/logo.png" alt="" width="150"></p>

<p>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/public/wordmark-dark.svg">
    <img src="docs/public/wordmark-light.svg" alt="FlexiQ" width="190">
  </picture>
</p>

A Rust-powered task queue with native SDKs. One engine — no broker required, just SQLite, Postgres, or Redis.

[![PyPI version](https://img.shields.io/pypi/v/flexiq.svg)](https://pypi.org/project/flexiq/)
[![npm version](https://img.shields.io/npm/v/@byteveda/flexiq.svg)](https://www.npmjs.com/package/@byteveda/flexiq)
[![Maven Central](https://img.shields.io/maven-central/v/org.byteveda/flexiq.svg)](https://central.sonatype.com/artifact/org.byteveda/flexiq) <br>
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/ByteVeda/flexiq/blob/master/LICENSE)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/ByteVeda/flexiq)

</div>

Most task queues need a separate broker (Redis, RabbitMQ) even for single-machine workloads.
flexiq embeds storage, scheduling, and worker management into one install with no external
services. The engine is a single Rust core — a Tokio async scheduler, an OS-thread worker pool,
and Diesel over SQLite in WAL mode — exposed to each language through a thin native SDK.

> **Formerly Taskito.** The project was renamed to FlexiQ in 1.0.0. Packages ship as `flexiq`
> (PyPI), `@byteveda/flexiq` (npm), `org.byteveda:flexiq` (Maven) and `flexiq`/`flexiq-core`
> (crates.io); the old `taskito` packages receive no further releases. See
> [Migrating to FlexiQ](https://docs.byteveda.org/flexiq/resources/migrating-to-flexiq).

## SDKs

| Language | Install | Package | Docs |
|----------|---------|---------|------|
| **Python** | `pip install flexiq` | [PyPI](https://pypi.org/project/flexiq/) · [`sdks/python`](sdks/python) | [Python docs](https://docs.byteveda.org/flexiq) |
| **Node.js** | `npm install @byteveda/flexiq` | [npm](https://www.npmjs.com/package/@byteveda/flexiq) · [`sdks/node`](sdks/node) | [Node docs](https://docs.byteveda.org/flexiq/node/getting-started/installation) |
| **Java** | `org.byteveda:flexiq` | [Maven Central](https://central.sonatype.com/artifact/org.byteveda/flexiq) · [`sdks/java`](sdks/java) | [Java docs](https://docs.byteveda.org/flexiq/java/getting-started/installation) |

Each SDK is self-contained — see its README for install, quickstart, and the full API.

## Architecture

One Rust core (`crates/`), one thin SDK shell per language (`sdks/`). The DB is the source of
truth; the GIL/event loop is held only during task execution. `WorkerDispatcher` in
`flexiq-core` is binding-free, so new language shells implement one trait against
[`BINDING_CONTRACT.md`](crates/flexiq-core/BINDING_CONTRACT.md).

## Features

- **Reliability** — retries with backoff, per-exception rules, soft timeouts, dead-letter queue with replay, circuit breakers, idempotent enqueue.
- **Workflows** — chain, fan-out (`group`), fan-in (`chord`), dependency graphs with cascade cancel, approval gates, saga compensation.
- **Concurrency** — thread pool for I/O, prefork pool for true CPU parallelism with no GIL contention.
- **Scheduling** — priorities, rate limiting, periodic (cron) tasks, delayed execution, job expiration.
- **Observability** — built-in web dashboard, events, HMAC-signed webhooks, Prometheus + OpenTelemetry exporters, worker heartbeats.
- **Backends** — SQLite (default), Postgres or Redis for multi-machine workers; same API.

## Comparison

| Feature | flexiq | Celery | RQ | Dramatiq | Huey |
|---|---|---|---|---|---|
| Broker required | **No** | Yes | Yes | Yes | Yes |
| Core language | **Rust** | Python | Python | Python | Python |
| Language SDKs | **Python, Node, Java** | Python | Python | Python | Python |
| Priority queues | **Yes** | Yes | No | No | Yes |
| Rate limiting | **Yes** | Yes | No | Yes | No |
| Dead letter queue | **Yes** | No | Yes | No | No |
| Task dependencies | **Yes** | No | No | No | No |
| Workflows (chain/group/chord) | **Yes** | Yes | No | Yes | No |
| Built-in dashboard | **Yes** | No | No | No | No |
| Cancel running tasks | **Yes** | Yes | No | No | No |
| CPU parallelism (prefork pool) | **Yes** | Yes | Yes | Yes | Yes |
| Postgres backend | **Yes** | Yes | No | No | No |
| Setup | **one install** | Broker + backend | Redis | Broker | Redis |

## Documentation

**[Read the docs →](https://docs.byteveda.org/flexiq)** — guides, API reference, and architecture.
Coming from Celery? See the **[Migration Guide](https://docs.byteveda.org/flexiq/python/operate/migration)**.

## Contributing

The repo is a Cargo workspace (`crates/`) plus per-language SDK packages (`sdks/`). Build and
test commands live in each SDK's README. All PRs target `master`.

## License

MIT
