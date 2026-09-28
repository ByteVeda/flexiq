"""``flexiq executor`` dialling a ``tls://`` scheduler.

The fake scheduler terminates TLS with Python's own ``ssl`` module, so what
passes here is the executor's TLS against an independent implementation, not
against itself. The certificates are the repository's shared test fixtures.
"""

from __future__ import annotations

import ssl
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

# tests/worker is not a package, so pytest's rootdir insertion is what makes
# this a plain module import rather than a relative one.
from test_executor_attach import (  # noqa: F401 — `_app_importable` is an autouse fixture
    ECHO,
    FakeScheduler,
    _app_importable,
    payload_for,
    read_stderr,
    spawn_executor,
    terminate,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="the executor runs tasks on prefork children, which Windows does not support",
)

REPO = Path(__file__).resolve().parents[4]
FIXTURES = REPO / "crates" / "flexiq-core" / "tests" / "fixtures" / "tls"


def server_context(require_client_cert: bool) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(FIXTURES / "server.pem", FIXTURES / "server-key.pem")
    if require_client_cert:
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(FIXTURES / "ca.pem")
    return context


@pytest.fixture
def tls_scheduler() -> Iterator[FakeScheduler]:
    fake = FakeScheduler(tls=server_context(require_client_cert=False))
    try:
        yield fake
    finally:
        fake.close()


@pytest.fixture
def mtls_scheduler() -> Iterator[FakeScheduler]:
    fake = FakeScheduler(tls=server_context(require_client_cert=True))
    try:
        yield fake
    finally:
        fake.close()


def run_one_job(scheduler: FakeScheduler) -> None:
    scheduler.accept()
    scheduler.send_job("job-1", ECHO, payload_for(ECHO, "hello"))
    header, _ = scheduler.next_result()
    assert header["type"] == "success", header


def test_a_job_runs_over_tls(tls_scheduler: FakeScheduler, tmp_path: Path) -> None:
    process = spawn_executor(
        tls_scheduler.port,
        tmp_path / "t.db",
        # `localhost`, so the name the executor verifies is the certificate's.
        address=f"tls://localhost:{tls_scheduler.port}",
        extra_args=["--tls-ca", str(FIXTURES / "ca.pem")],
    )
    try:
        run_one_job(tls_scheduler)
    finally:
        terminate(process)


def test_mtls_material_can_come_from_the_environment(
    mtls_scheduler: FakeScheduler, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLEXIQ_ATTACH_TLS_CA", str(FIXTURES / "ca.pem"))
    monkeypatch.setenv("FLEXIQ_ATTACH_TLS_CERT", str(FIXTURES / "client.pem"))
    monkeypatch.setenv("FLEXIQ_ATTACH_TLS_KEY", str(FIXTURES / "client-key.pem"))
    process = spawn_executor(
        mtls_scheduler.port,
        tmp_path / "t.db",
        address=f"tls://localhost:{mtls_scheduler.port}",
    )
    try:
        run_one_job(mtls_scheduler)
    finally:
        terminate(process)


def test_mtls_refuses_an_executor_without_a_certificate(
    mtls_scheduler: FakeScheduler, tmp_path: Path
) -> None:
    process = spawn_executor(
        mtls_scheduler.port,
        tmp_path / "t.db",
        address=f"tls://localhost:{mtls_scheduler.port}",
        extra_args=["--tls-ca", str(FIXTURES / "ca.pem")],
    )
    try:
        with pytest.raises((ssl.SSLError, OSError)):
            mtls_scheduler.accept()
        assert process.wait(timeout=60) != 0
    finally:
        terminate(process)


def test_tls_options_beside_a_plaintext_address_are_refused(tmp_path: Path) -> None:
    """Configured for TLS but pointed at ``host:port``: refuse, never dial in the clear."""
    process = spawn_executor(
        1,
        tmp_path / "t.db",
        extra_args=["--tls-ca", str(FIXTURES / "ca.pem")],
    )
    try:
        assert process.wait(timeout=60) != 0
        assert "tls://" in read_stderr(process)
    finally:
        terminate(process)


def test_blank_tls_variables_are_unset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Compose sets variables to "" freely; that must not read as TLS material."""
    for name in ("FLEXIQ_ATTACH_TLS_CA", "FLEXIQ_ATTACH_TLS_CERT", "FLEXIQ_ATTACH_TLS_KEY"):
        monkeypatch.setenv(name, "")
    fake = FakeScheduler()
    process = spawn_executor(fake.port, tmp_path / "t.db")
    try:
        run_one_job(fake)
    finally:
        terminate(process)
        fake.close()
