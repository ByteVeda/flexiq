<div align="center">

# taskito (Python)

A Rust-powered task queue for Python. No broker required — just SQLite, Postgres, or Redis.

[![PyPI version](https://img.shields.io/pypi/v/taskito.svg)](https://pypi.org/project/taskito/)
[![Python versions](https://img.shields.io/pypi/pyversions/taskito.svg)](https://pypi.org/project/taskito/)
[![License](https://img.shields.io/pypi/l/taskito.svg)](https://github.com/ByteVeda/taskito/blob/master/LICENSE)

</div>

```bash
pip install taskito                # SQLite (default)
pip install taskito[postgres]      # with Postgres backend
```

The engine runs in Rust — a Tokio async scheduler, an OS-thread worker pool, and Diesel over
SQLite in WAL mode. The GIL is held only during task execution; run `--pool prefork` for true
parallelism on CPU-bound work. Part of the [taskito](https://github.com/ByteVeda/taskito)
project (Rust core + native SDKs for Python and Node).

## Quickstart

**1. Define tasks** in `tasks.py`:

```python
from taskito import Queue

queue = Queue(db_path="tasks.db")

@queue.task()
def add(a: int, b: int) -> int:
    return a + b
```

**2. Start a worker:**

```bash
taskito worker --app tasks:queue
```

**3. Enqueue jobs:**

```python
from tasks import add

job = add.delay(2, 3)
print(job.result(timeout=10))  # 5
```

## Features

Each section links to its deep-dive guide. New here? Start with
**[Capabilities at a glance](https://docs.byteveda.org/taskito/python/getting-started/capabilities)**.

- **Reliability** — retries with backoff, per-exception retry rules, soft timeouts, a dead-letter queue with replay, circuit breakers, idempotent enqueue. [→ guide](https://docs.byteveda.org/taskito/python/guides/reliability)
- **Workflows** — compose with `chain`, fan out with `group`, fan in with `chord`, plus dependency graphs with cascade cancel. [→ guide](https://docs.byteveda.org/taskito/python/guides/workflows/canvas)
- **Concurrency** — thread pool by default (I/O-bound); switch to `--pool prefork` for true CPU parallelism with no GIL contention. [→ guide](https://docs.byteveda.org/taskito/python/guides/advanced-execution/prefork)
- **Scheduling** — priorities, rate limiting, periodic (cron) tasks, delayed execution, job expiration. [→ guide](https://docs.byteveda.org/taskito/python/guides/core/scheduling)
- **Observability** — built-in web dashboard, events, HMAC-signed webhooks, Prometheus + OpenTelemetry exporters, worker heartbeats. [→ guide](https://docs.byteveda.org/taskito/python/guides/dashboard)
- **Extensibility** — pluggable serializers, per-task middleware, a fully async API, Postgres/Redis backends. [→ guide](https://docs.byteveda.org/taskito/python/guides/extensibility)

```python
from taskito import chain, group, chord

# Sequential pipeline — each step receives the previous result
chain(fetch.s(url), parse.s(), store.s()).apply()

# Parallel fan-out, then a callback once all complete
chord([download.s(u) for u in urls], merge.s()).apply()
```

## Integrations

| Extra | Install | What you get |
|-------|---------|--------------|
| **Flask** | `pip install taskito[flask]` | `Taskito(app)` extension, `flask taskito worker` CLI |
| **FastAPI** | `pip install taskito[fastapi]` | `TaskitoRouter` for instant REST API over the queue |
| **Django** | `pip install taskito[django]` | Admin integration, management commands |
| **OpenTelemetry** | `pip install taskito[otel]` | Distributed tracing with span-per-task |
| **Prometheus** | `pip install taskito[prometheus]` | `PrometheusMiddleware`, queue depth gauges, `/metrics` server |
| **Sentry** | `pip install taskito[sentry]` | `SentryMiddleware` with auto error capture and task tags |
| **Postgres** | `pip install taskito[postgres]` | Multi-machine workers via PostgreSQL backend |
| **Redis** | `pip install taskito[redis]` | Redis storage backend |

## Testing

Built-in test mode — no worker needed:

```python
def test_add():
    with queue.test_mode() as results:
        add.delay(2, 3)
        assert results[0].return_value == 5
```

## Documentation

**[Read the docs →](https://docs.byteveda.org/taskito)** — guides, API reference, and architecture.
Coming from Celery? See the **[Migration Guide](https://docs.byteveda.org/taskito/python/guides/operations/migration)**.
For a project overview and the other SDKs, see the [main repository](https://github.com/ByteVeda/taskito).

## License

MIT
