"""Byte-for-byte parity between this dashboard and the standalone server.

One SPA build is served by every implementation, so the JSON they return for
the same database has to agree. Reading both implementations side by side is
how drift creeps in; this compares them.

Skipped unless the ``flexiq-server`` binary is available — set
``FLEXIQ_SERVER_BIN`` or build it first::

    cargo build -p flexiq-server
    uv run python -m pytest tests/dashboard/test_rust_server_parity.py -v
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Generator
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest

from conftest import join_worker
from flexiq import Queue
from flexiq.dashboard import _make_handler

# ── Route classification ──────────────────────────────────────────────
#
# Every route the SPA calls falls into exactly one of these three groups.

#: Must match exactly. A difference here is a broken dashboard page.
IDENTICAL_ROUTES: tuple[str, ...] = (
    "/api/stats",
    "/api/stats/queues",
    "/api/jobs",
    "/api/jobs?status=pending",
    "/api/jobs?limit=2&offset=1",
    "/api/dead-letters",
    "/api/workers",
    "/api/circuit-breakers",
    "/api/queues/paused",
    "/api/settings",
    "/api/logs?since=3600&limit=100",
    "/api/metrics?since=3600",
    "/api/topics",
    "/api/webhooks",
    "/api/event-types",
    "/api/retention",
    "/api/workflows/runs",
    "/api/auth/status",
    "/health",
)

#: Match after dropping fields that are computed from "now" on each side, or
#: that report a capacity the two processes measure differently.
NORMALIZED_ROUTES: dict[str, tuple[str, ...]] = {
    # Buckets are anchored to each server's own clock.
    "/api/metrics/timeseries?since=3600&bucket=3600": ("timestamp",),
    # The snapshot instant differs by however long the two calls are apart.
    "/api/retention/dry-run": ("reference_time",),
    # `totalCapacity` is the in-process pool here and advertised executor slots
    # there; `workerUtilization` derives from it.
    "/api/scaler": ("totalCapacity", "workerUtilization"),
}

#: Deliberately different, and documented as such: these read a language
#: runtime that the standalone server does not have. Only the shape is
#: compared, so a route disappearing still fails.
DIVERGENT_ROUTES: tuple[str, ...] = (
    "/api/tasks",
    "/api/queues",
    "/api/middleware",
    "/api/resources",
    "/api/proxy-stats",
    "/api/interception-stats",
)


def _server_binary() -> Path | None:
    """Locate the standalone server, or ``None`` when it is not built."""
    explicit = os.environ.get("FLEXIQ_SERVER_BIN")
    if explicit:
        candidate = Path(explicit)
        return candidate if candidate.is_file() else None
    root = Path(__file__).resolve().parents[4]
    for profile in ("debug", "release"):
        candidate = root / "target" / profile / "flexiq-server"
        if candidate.is_file():
            return candidate
    return None


def _free_port() -> int:
    """Reserve a port by binding and releasing it."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _get(base: str, path: str, timeout: float = 10.0) -> tuple[int, Any]:
    """GET ``path``, returning ``(status, decoded body)``."""
    request = urllib.request.Request(base + path, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"null")


def _strip(value: Any, keys: tuple[str, ...]) -> Any:
    """Recursively drop ``keys`` wherever they appear."""
    if isinstance(value, dict):
        return {k: _strip(v, keys) for k, v in value.items() if k not in keys}
    if isinstance(value, list):
        return [_strip(item, keys) for item in value]
    return value


@pytest.fixture
def seeded_queue(tmp_path: Path) -> tuple[Queue, str]:
    """A database carrying one row of every shape the dashboard renders."""
    db_path = str(tmp_path / "parity.db")
    queue = Queue(db_path=db_path, workers=2)

    @queue.task(name="parity.alpha", queue="default")
    def alpha(x: int) -> int:
        return x * 2

    @queue.task(name="parity.beta", queue="email", max_retries=5, timeout=42)
    def beta(x: int) -> int:
        return x + 1

    @queue.task(name="parity.boom", queue="default", max_retries=0)
    def boom() -> None:
        raise ValueError("seeded failure")

    jobs = [alpha.delay(i) for i in range(3)]
    jobs.append(beta.delay(9))
    # One cancelled job, so a terminal status is represented too.
    queue.cancel_job(jobs[0].id)

    # A real failure, run by a real worker: it is the only way to seed a
    # structured error, its per-attempt rows, and a metric — which is exactly
    # the summarisation logic most at risk of drifting between the two ports.
    failing = boom.delay()
    worker = threading.Thread(target=queue.run_worker, daemon=True)
    worker.start()
    try:
        deadline = time.time() + 30
        while time.time() < deadline:
            current = queue.get_job(failing.id)
            if current is not None and current.to_dict()["status"] in {"failed", "dead"}:
                break
            time.sleep(0.1)
        else:
            pytest.fail("the seeded failure never reached a terminal state")
    finally:
        queue.shutdown()
        join_worker(worker)

    queue._inner.write_task_log(jobs[1].id, "parity.alpha", "info", "seeded log", None)
    queue.set_setting("dashboard:branding", json.dumps({"title": "Parity"}))
    queue.add_webhook(url="https://example.com/hook", description="parity")
    queue.declare_topic("parity.topic")

    return queue, db_path


@pytest.fixture
def parity_servers(seeded_queue: tuple[Queue, str]) -> Generator[tuple[str, str]]:
    """Serve the same database from both implementations.

    Yields ``(python base url, rust base url)``.
    """
    binary = _server_binary()
    if binary is None:
        pytest.skip("flexiq-server is not built; run `cargo build -p flexiq-server`")

    queue, db_path = seeded_queue

    # This dashboard, in the same open mode the standalone server defaults to.
    handler = _make_handler(queue)
    python_server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    python_url = f"http://127.0.0.1:{python_server.server_address[1]}"
    threading.Thread(target=python_server.serve_forever, daemon=True).start()

    # The standalone server, with no attach listener: the dashboard is what is
    # under test, and a listener would need a port for nothing.
    rust_port = _free_port()
    rust_url = f"http://127.0.0.1:{rust_port}"
    process = subprocess.Popen(
        [str(binary)],
        env={
            **os.environ,
            "FLEXIQ_DSN": db_path,
            "FLEXIQ_BACKEND": "sqlite",
            "FLEXIQ_DASHBOARD": f"127.0.0.1:{rust_port}",
            "RUST_LOG": "warn",
        },
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    deadline = time.time() + 30
    while time.time() < deadline:
        if process.poll() is not None:
            stdout = process.stdout.read() if process.stdout is not None else b""
            pytest.fail(
                "flexiq-server exited during startup:\n" + (stdout or b"").decode(errors="replace")
            )
        try:
            if _get(rust_url, "/health", timeout=1.0)[0] == 200:
                break
        except OSError:
            pass
        # Sleep on every miss, not only on a connection error: a server that
        # answers non-200 while starting would otherwise spin this loop.
        time.sleep(0.2)
    else:
        process.kill()
        process.wait(timeout=5)
        pytest.fail("flexiq-server did not become ready within 30s")

    try:
        yield python_url, rust_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
        python_server.shutdown()
        python_server.server_close()


@pytest.mark.parametrize("route", IDENTICAL_ROUTES)
def test_route_matches_the_standalone_server(parity_servers: tuple[str, str], route: str) -> None:
    python_url, rust_url = parity_servers
    python_status, python_body = _get(python_url, route)
    rust_status, rust_body = _get(rust_url, route)

    assert python_status == rust_status == 200, route
    assert python_body == rust_body, (
        f"{route} differs between the dashboards\n"
        f"  this dashboard: {json.dumps(python_body, sort_keys=True)[:800]}\n"
        f"  flexiq-server: {json.dumps(rust_body, sort_keys=True)[:800]}"
    )


@pytest.mark.parametrize("route", list(NORMALIZED_ROUTES))
def test_route_matches_apart_from_clock_derived_fields(
    parity_servers: tuple[str, str], route: str
) -> None:
    python_url, rust_url = parity_servers
    volatile = NORMALIZED_ROUTES[route]

    python_status, python_body = _get(python_url, route)
    rust_status, rust_body = _get(rust_url, route)

    assert python_status == rust_status == 200, route
    assert _strip(python_body, volatile) == _strip(rust_body, volatile), (
        f"{route} differs beyond {volatile}\n"
        f"  this dashboard: {json.dumps(python_body, sort_keys=True)[:800]}\n"
        f"  flexiq-server: {json.dumps(rust_body, sort_keys=True)[:800]}"
    )


@pytest.mark.parametrize("route", DIVERGENT_ROUTES)
def test_divergent_route_still_answers_with_the_same_shape(
    parity_servers: tuple[str, str], route: str
) -> None:
    """These read an in-process runtime the standalone server does not have.

    The contents legitimately differ; the route existing and returning the same
    JSON type does not, or the SPA breaks on one of them.
    """
    python_url, rust_url = parity_servers
    python_status, python_body = _get(python_url, route)
    rust_status, rust_body = _get(rust_url, route)

    assert python_status == rust_status == 200, route
    assert type(python_body) is type(rust_body), (
        f"{route} returns {type(python_body).__name__} here and "
        f"{type(rust_body).__name__} from flexiq-server"
    )


def test_a_job_detail_matches(parity_servers: tuple[str, str]) -> None:
    """The per-job routes, driven off every job the seed created.

    The failed one matters most: its `error` is a summarised structured
    `TaskError`, and that summariser is hand-ported on the Rust side.
    """
    python_url, rust_url = parity_servers
    _, listed = _get(python_url, "/api/jobs?limit=50")
    ids = [job["id"] for job in listed]
    assert ids, "the seed must have produced jobs"
    summarised = [job for job in listed if job["error"]]
    assert summarised, "the seed must have produced a failed job with an error"

    for job_id in ids:
        for route in (
            f"/api/jobs/{job_id}",
            f"/api/jobs/{job_id}/errors",
            f"/api/jobs/{job_id}/logs",
            f"/api/jobs/{job_id}/replay-history",
            f"/api/jobs/{job_id}/dag",
        ):
            python_status, python_body = _get(python_url, route)
            rust_status, rust_body = _get(rust_url, route)
            assert python_status == rust_status == 200, route
            assert python_body == rust_body, (
                f"{route} differs\n"
                f"  this dashboard: {json.dumps(python_body, sort_keys=True)[:800]}\n"
                f"  flexiq-server: {json.dumps(rust_body, sort_keys=True)[:800]}"
            )


def test_a_missing_job_is_the_same_error(parity_servers: tuple[str, str]) -> None:
    """Error bodies are part of the contract too — the SPA renders them."""
    python_url, rust_url = parity_servers
    python_status, python_body = _get(python_url, "/api/jobs/does-not-exist")
    rust_status, rust_body = _get(rust_url, "/api/jobs/does-not-exist")

    assert python_status == rust_status == 404
    assert python_body == rust_body
