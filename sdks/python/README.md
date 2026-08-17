<div align="center">

# flexiq (Python)

A Rust-powered task queue for Python. No broker required — just SQLite, Postgres, or Redis.

[![PyPI version](https://img.shields.io/pypi/v/flexiq.svg)](https://pypi.org/project/flexiq/)
[![Python versions](https://img.shields.io/pypi/pyversions/flexiq.svg)](https://pypi.org/project/flexiq/)
[![License](https://img.shields.io/pypi/l/flexiq.svg)](https://github.com/ByteVeda/flexiq/blob/master/LICENSE)

</div>

```bash
pip install flexiq                # SQLite (default)
pip install flexiq[postgres]      # with Postgres backend
```

The engine runs in Rust — a Tokio async scheduler, an OS-thread worker pool, and Diesel over
SQLite in WAL mode. The GIL is held only during task execution; run `--pool prefork` for true
parallelism on CPU-bound work. Part of the [flexiq](https://github.com/ByteVeda/flexiq)
project (Rust core + native SDKs for Python and Node).

## Quickstart

**1. Define tasks** in `tasks.py`:

```python
from flexiq import Queue

queue = Queue(db_path="tasks.db")

@queue.task()
def add(a: int, b: int) -> int:
    return a + b
```

**2. Start a worker:**

```bash
flexiq worker --app tasks:queue
```

**3. Enqueue jobs:**

```python
from tasks import add

job = add.delay(2, 3)
print(job.result(timeout=10))  # 5
```

## Features

Each section links to its deep-dive guide. New here? Start with
**[Capabilities at a glance](https://docs.byteveda.org/flexiq/python/getting-started/capabilities)**.

- **Reliability** — retries with backoff, per-exception retry rules, soft timeouts, a dead-letter queue with replay, circuit breakers, idempotent enqueue. [→ guide](https://docs.byteveda.org/flexiq/python/guides/reliability)
- **Workflows** — compose with `chain`, fan out with `group`, fan in with `chord`, plus dependency graphs with cascade cancel. [→ guide](https://docs.byteveda.org/flexiq/python/guides/workflows/canvas)
- **Concurrency** — thread pool by default (I/O-bound); switch to `--pool prefork` for true CPU parallelism with no GIL contention. [→ guide](https://docs.byteveda.org/flexiq/python/guides/advanced-execution/prefork)
- **Scheduling** — priorities, rate limiting, periodic (cron) tasks, delayed execution, job expiration. [→ guide](https://docs.byteveda.org/flexiq/python/guides/core/scheduling)
- **Observability** — built-in web dashboard, events, HMAC-signed webhooks, Prometheus + OpenTelemetry exporters, worker heartbeats. [→ guide](https://docs.byteveda.org/flexiq/python/guides/dashboard)
- **Extensibility** — pluggable serializers, per-task middleware, a fully async API, Postgres/Redis backends. [→ guide](https://docs.byteveda.org/flexiq/python/guides/extensibility)

```python
from flexiq import chain, group, chord

# Sequential pipeline — each step receives the previous result
chain(fetch.s(url), parse.s(), store.s()).apply()

# Parallel fan-out, then a callback once all complete
chord([download.s(u) for u in urls], merge.s()).apply()
```

## Integrations

| Extra | Install | What you get |
|-------|---------|--------------|
| **Flask** | `pip install flexiq[flask]` | `FlexiQ(app)` extension, `flask flexiq worker` CLI |
| **FastAPI** | `pip install flexiq[fastapi]` | `FlexiQRouter` for instant REST API over the queue |
| **Django** | `pip install flexiq[django]` | Admin integration, management commands |
| **OpenTelemetry** | `pip install flexiq[otel]` | Distributed tracing with span-per-task |
| **Prometheus** | `pip install flexiq[prometheus]` | `PrometheusMiddleware`, queue depth gauges, `/metrics` server |
| **Sentry** | `pip install flexiq[sentry]` | `SentryMiddleware` with auto error capture and task tags |
| **Postgres** | `pip install flexiq[postgres]` | Multi-machine workers via PostgreSQL backend |
| **Redis** | `pip install flexiq[redis]` | Redis storage backend |

## Testing

Built-in test mode — no worker needed:

```python
def test_add():
    with queue.test_mode() as results:
        add.delay(2, 3)
        assert results[0].return_value == 5
```

## Documentation

**[Read the docs →](https://docs.byteveda.org/flexiq)** — guides, API reference, and architecture.
Coming from Celery? See the **[Migration Guide](https://docs.byteveda.org/flexiq/python/guides/operations/migration)**.
For a project overview and the other SDKs, see the [main repository](https://github.com/ByteVeda/flexiq).

## License

MIT
