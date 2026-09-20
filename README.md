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

### Remote clients

| Language | Install | Package | Opens |
|----------|---------|---------|-------|
| **Go** | `go get github.com/ByteVeda/flexiq/sdks/go/v2` | [`sdks/go`](sdks/go) | The producer door of a running `flexiq-server` |

A remote client is not an SDK: it holds no database credential, links no native binding, and
**cannot execute tasks** — it submits work that somebody else's workers drain. For a language with
no client at all, [`REMOTE_SDK_CONTRACT.md`](contracts/REMOTE_SDK_CONTRACT.md) is what you
implement against.

## Architecture

One Rust core (`crates/`), one thin SDK shell per language (`sdks/`). The DB is the source of
truth; the GIL/event loop is held only during task execution. `WorkerDispatcher` in
`flexiq-core` is binding-free, so new language shells implement one trait against
[`BINDING_CONTRACT.md`](crates/flexiq-core/BINDING_CONTRACT.md). A client that talks to
`flexiq-server` over the network instead implements
[`REMOTE_SDK_CONTRACT.md`](contracts/REMOTE_SDK_CONTRACT.md), which needs no native binding.

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

## Benchmarks

One scenario, run against FlexiQ, Celery, Dramatiq, RQ and BullMQ, from one
command, with pinned dependencies and the machine recorded in the output. The
harness is [`bench/`](bench/) and the numbers below are its committed output —
nothing here is typed by hand.

<!-- bench:start -->

<!-- Generated by scripts/sync-bench.mjs from bench/results/latest.json. Rerun `python bench/run.py --publish` to change it. -->

20,000 jobs, a 256-byte payload and concurrency 4, every system on its own defaults, measured on 4-core shared cloud VM (Intel Xeon @ 2.10GHz, 15.7 GB). Throughput is from a burst; the percentiles are from a separate phase paced at 50/s, so they are what one caller waits rather than the backlog in front of it:

| System | Enqueue/s | Completion/s | p50 ms | p99 ms | Idle MB |
|---|---:|---:|---:|---:|---:|
| FlexiQ · SQLite (WAL) | 5,660 | 143.9 | 29.2 | 54.3 | 51.1 |
| FlexiQ · Redis | 4,978 | 127.0 | 29.5 | 123.5 | 46.5 |
| Celery · Redis | 1,946 | 1,029 | 2.8 | 3.5 | 209.3 |
| Dramatiq · Redis | 6,110 | 1,046 | 7.7 | 20.8 | 129.6 |
| RQ · Redis | 1,873 | 155.7 | 16.6 | 20.6 | 172.5 |
| BullMQ · Redis | 5,584 | 5,584 | 1.6 | 2.4 | 73.9 |

FlexiQ does not win this on every axis. The rows where it loses are in the table above, the harness that produced them is in [`bench/`](bench/), and the raw numbers — including what was measured on what kernel from which commit — are in [`bench/results/`](bench/results/). Measured inside a shared cloud container: a virtualised host with neighbours this run cannot see or control. Treat the ranking as indicative and the tails as noisy; rerun on dedicated hardware before quoting a number in a decision.

<!-- bench:end -->

Measure your own: `python bench/run.py`. The numbers above describe one shared
cloud VM and a 256-byte no-op task; your handler's own work will dominate all of
it.

## Documentation

**[Read the docs →](https://docs.byteveda.org/flexiq)** — guides, API reference, and architecture.
Coming from Celery? See the **[Migration Guide](https://docs.byteveda.org/flexiq/python/operate/migration)**.

## Contributing

The repo is a Cargo workspace (`crates/`) plus per-language SDK packages (`sdks/`). Build and
test commands live in each SDK's README. All PRs target `master`.

## License

MIT
