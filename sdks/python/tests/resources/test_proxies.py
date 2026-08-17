"""Tests for the proxy system (Layer 3 — transparent reconstruction)."""

from __future__ import annotations

import logging
import time
from typing import Any

import pytest

from flexiq import ProxyReconstructionError, Queue
from flexiq.proxies import BuiltInProxy, ProxyRegistry, cleanup_proxies, reconstruct_proxies
from flexiq.proxies.built_in import register_builtin_handlers
from flexiq.proxies.handlers.file import FileHandler
from flexiq.proxies.handlers.logger import LoggerHandler
from flexiq.proxies.reconstruct import _ReconstructionTimeout, _run_with_timeout

# ---------------------------------------------------------------------------
# FileHandler
# ---------------------------------------------------------------------------


class TestFileHandler:
    def test_detect_open_file(self, tmp_path: Any) -> None:
        f = open(tmp_path / "test.txt", "w")  # noqa: SIM115
        try:
            handler = FileHandler()
            assert handler.detect(f) is True
        finally:
            f.close()

    def test_detect_closed_file(self, tmp_path: Any) -> None:
        f = open(tmp_path / "test.txt", "w")  # noqa: SIM115
        f.close()
        handler = FileHandler()
        assert handler.detect(f) is False

    def test_detect_stdin(self) -> None:
        import sys

        handler = FileHandler()
        assert handler.detect(sys.stdin) is False

    def test_deconstruct_text_file(self, tmp_path: Any) -> None:
        path = tmp_path / "data.txt"
        path.write_text("hello world")
        with open(path) as f:
            handler = FileHandler()
            recipe = handler.deconstruct(f)
            assert recipe["path"] == str(path)
            assert recipe["mode"] == "r"
            assert recipe["encoding"] is not None
            assert recipe["position"] == 0

    def test_deconstruct_binary_file(self, tmp_path: Any) -> None:
        path = tmp_path / "data.bin"
        path.write_bytes(b"\x00\x01\x02")
        with open(path, "rb") as f:
            handler = FileHandler()
            recipe = handler.deconstruct(f)
            assert recipe["mode"] == "rb"
            assert recipe["encoding"] is None

    def test_reconstruct_text_file(self, tmp_path: Any) -> None:
        path = tmp_path / "data.txt"
        path.write_text("hello world")
        handler = FileHandler()
        recipe = {"path": str(path), "mode": "r", "encoding": "utf-8", "position": 0}
        f = handler.reconstruct(recipe, version=1)
        try:
            assert f.read() == "hello world"
        finally:
            f.close()

    def test_reconstruct_at_position(self, tmp_path: Any) -> None:
        path = tmp_path / "data.txt"
        path.write_text("hello world")
        handler = FileHandler()
        recipe = {"path": str(path), "mode": "r", "encoding": "utf-8", "position": 6}
        f = handler.reconstruct(recipe, version=1)
        try:
            assert f.read() == "world"
        finally:
            f.close()

    def test_cleanup_closes_file(self, tmp_path: Any) -> None:
        path = tmp_path / "data.txt"
        path.write_text("test")
        f = open(path)  # noqa: SIM115
        handler = FileHandler()
        handler.cleanup(f)
        assert f.closed

    def test_cleanup_already_closed(self, tmp_path: Any) -> None:
        path = tmp_path / "data.txt"
        path.write_text("test")
        f = open(path)  # noqa: SIM115
        f.close()
        handler = FileHandler()
        handler.cleanup(f)  # no error


# ---------------------------------------------------------------------------
# LoggerHandler
# ---------------------------------------------------------------------------


class TestLoggerHandler:
    def test_detect_logger(self) -> None:
        handler = LoggerHandler()
        lgr = logging.getLogger("test.proxies.detect")
        assert handler.detect(lgr) is True

    def test_detect_non_logger(self) -> None:
        handler = LoggerHandler()
        assert handler.detect("not a logger") is False

    def test_round_trip(self) -> None:
        handler = LoggerHandler()
        lgr = logging.getLogger("test.proxies.roundtrip")
        lgr.setLevel(logging.WARNING)
        recipe = handler.deconstruct(lgr)
        reconstructed = handler.reconstruct(recipe, version=1)
        assert reconstructed.name == "test.proxies.roundtrip"
        assert reconstructed.level == logging.WARNING

    def test_cleanup_noop(self) -> None:
        handler = LoggerHandler()
        lgr = logging.getLogger("test.proxies.cleanup")
        handler.cleanup(lgr)  # no error


# ---------------------------------------------------------------------------
# ProxyRegistry
# ---------------------------------------------------------------------------


class TestProxyRegistry:
    def test_register_and_get(self) -> None:
        reg = ProxyRegistry()
        handler = FileHandler()
        reg.register(handler)
        assert reg.get("file") is handler

    def test_get_missing(self) -> None:
        reg = ProxyRegistry()
        assert reg.get("nonexistent") is None

    def test_find_handler(self, tmp_path: Any) -> None:
        reg = ProxyRegistry()
        register_builtin_handlers(reg)
        f = open(tmp_path / "test.txt", "w")  # noqa: SIM115
        try:
            found = reg.find_handler(f)
            assert found is not None
            assert found.name == "file"
        finally:
            f.close()

    def test_find_handler_no_match(self) -> None:
        reg = ProxyRegistry()
        register_builtin_handlers(reg)
        assert reg.find_handler(42) is None


# ---------------------------------------------------------------------------
# Reconstruction
# ---------------------------------------------------------------------------


class TestReconstruct:
    def test_proxy_marker_reconstructed(self, tmp_path: Any) -> None:
        path = tmp_path / "data.txt"
        path.write_text("content")
        reg = ProxyRegistry()
        reg.register(FileHandler())

        marker = {
            "__flexiq_proxy__": True,
            "handler": "file",
            "version": 1,
            "identity": "id-1",
            "recipe": {
                "path": str(path),
                "mode": "r",
                "encoding": "utf-8",
                "position": 0,
            },
        }
        args, _kwargs, cleanup_list = reconstruct_proxies((marker,), {}, reg)
        try:
            assert hasattr(args[0], "read")
            assert args[0].read() == "content"
            assert len(cleanup_list) == 1
        finally:
            cleanup_proxies(cleanup_list)

    def test_cleanup_list_populated(self, tmp_path: Any) -> None:
        path = tmp_path / "data.txt"
        path.write_text("test")
        reg = ProxyRegistry()
        reg.register(FileHandler())

        marker = {
            "__flexiq_proxy__": True,
            "handler": "file",
            "version": 1,
            "identity": "id-2",
            "recipe": {
                "path": str(path),
                "mode": "r",
                "encoding": "utf-8",
                "position": 0,
            },
        }
        _, _, cleanup_list = reconstruct_proxies((marker,), {}, reg)
        assert len(cleanup_list) == 1
        handler, obj = cleanup_list[0]
        assert handler.name == "file"
        assert not obj.closed
        cleanup_proxies(cleanup_list)
        assert obj.closed

    def test_cleanup_runs_lifo(self, tmp_path: Any) -> None:
        """Cleanup runs in reverse reconstruction order."""
        p1 = tmp_path / "a.txt"
        p1.write_text("a")
        p2 = tmp_path / "b.txt"
        p2.write_text("b")
        reg = ProxyRegistry()
        reg.register(FileHandler())

        markers = [
            {
                "__flexiq_proxy__": True,
                "handler": "file",
                "version": 1,
                "identity": f"id-{i}",
                "recipe": {
                    "path": str(p),
                    "mode": "r",
                    "encoding": "utf-8",
                    "position": 0,
                },
            }
            for i, p in enumerate([p1, p2])
        ]
        args, _, cleanup_list = reconstruct_proxies(tuple(markers), {}, reg)
        assert not args[0].closed
        assert not args[1].closed
        cleanup_proxies(cleanup_list)
        assert args[0].closed
        assert args[1].closed

    def test_cleanup_catches_errors(self) -> None:
        """Cleanup errors are logged, not raised."""

        class BadHandler:
            name = "bad"
            version = 1
            handled_types: tuple[type, ...] = ()

            def detect(self, obj: Any) -> bool:
                return False

            def deconstruct(self, obj: Any) -> dict[str, Any]:
                return {}

            def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
                return "obj"

            def cleanup(self, obj: Any) -> None:
                raise RuntimeError("cleanup boom")

        cleanup_list: list[tuple[Any, Any]] = [(BadHandler(), "obj")]
        cleanup_proxies(cleanup_list)  # should not raise

    def test_missing_handler_raises(self) -> None:
        reg = ProxyRegistry()
        marker = {
            "__flexiq_proxy__": True,
            "handler": "nonexistent",
            "version": 1,
            "recipe": {},
        }
        with pytest.raises(ProxyReconstructionError, match="No proxy handler"):
            reconstruct_proxies((marker,), {}, reg)

    def test_no_markers_passthrough(self) -> None:
        """Args without markers pass through unchanged."""
        reg = ProxyRegistry()
        register_builtin_handlers(reg)
        args, kwargs, cleanup = reconstruct_proxies((1, "hello", [3, 4]), {"key": "val"}, reg)
        assert args == (1, "hello", [3, 4])
        assert kwargs == {"key": "val"}
        assert cleanup == []


# ---------------------------------------------------------------------------
# Identity tracking
# ---------------------------------------------------------------------------


class TestIdentityTracking:
    def test_same_object_deduped(self, tmp_path: Any) -> None:
        """Same file handle passed twice produces one marker and one ref."""
        path = tmp_path / "data.txt"
        path.write_text("test")

        queue = Queue(db_path=str(tmp_path / "q.db"), interception="strict")
        f = open(path)  # noqa: SIM115
        try:
            assert queue._interceptor is not None
            walker = queue._interceptor._walker
            args, _kw, _res = walker.walk((f, f), {})

            # First should be a full proxy marker
            assert args[0].get("__flexiq_proxy__") is True
            # Second should be a reference to the first
            assert "__flexiq_ref__" in args[1]
            assert args[1]["__flexiq_ref__"] == args[0]["identity"]
        finally:
            f.close()

    def test_identity_reconstructed_once(self, tmp_path: Any) -> None:
        """Reconstruction creates one object; both positions share it."""
        path = tmp_path / "data.txt"
        path.write_text("shared")
        reg = ProxyRegistry()
        reg.register(FileHandler())

        identity = "shared-id"
        marker = {
            "__flexiq_proxy__": True,
            "handler": "file",
            "version": 1,
            "identity": identity,
            "recipe": {
                "path": str(path),
                "mode": "r",
                "encoding": "utf-8",
                "position": 0,
            },
        }
        ref = {"__flexiq_ref__": identity}

        args, _, cleanup = reconstruct_proxies((marker, ref), {}, reg)
        try:
            assert args[0] is args[1]  # same object
            assert len(cleanup) == 1  # only one cleanup entry
        finally:
            cleanup_proxies(cleanup)

    def test_different_objects_separate(self, tmp_path: Any) -> None:
        """Two different file handles get separate recipes."""
        p1 = tmp_path / "a.txt"
        p1.write_text("a")
        p2 = tmp_path / "b.txt"
        p2.write_text("b")

        queue = Queue(db_path=str(tmp_path / "q.db"), interception="strict")
        f1 = open(p1)  # noqa: SIM115
        f2 = open(p2)  # noqa: SIM115
        try:
            assert queue._interceptor is not None
            walker = queue._interceptor._walker
            args, _, _ = walker.walk((f1, f2), {})
            assert args[0].get("__flexiq_proxy__") is True
            assert args[1].get("__flexiq_proxy__") is True
            assert args[0]["identity"] != args[1]["identity"]
        finally:
            f1.close()
            f2.close()


# ---------------------------------------------------------------------------
# Proxy in nested structures
# ---------------------------------------------------------------------------


def test_proxy_in_nested_dict(tmp_path: Any) -> None:
    """File inside a dict is proxied."""
    path = tmp_path / "nested.txt"
    path.write_text("nested content")

    queue = Queue(db_path=str(tmp_path / "q.db"), interception="strict")
    f = open(path)  # noqa: SIM115
    try:
        assert queue._interceptor is not None
        walker = queue._interceptor._walker
        _, kwargs, _ = walker.walk((), {"config": {"file": f}})
        inner = kwargs["config"]["file"]
        assert inner.get("__flexiq_proxy__") is True
        assert inner["handler"] == "file"
    finally:
        f.close()


# ---------------------------------------------------------------------------
# End-to-end with Queue test mode
# ---------------------------------------------------------------------------


def test_proxy_roundtrip_in_test_mode(queue: Queue) -> None:
    """In test mode, original objects pass through (no serialization)."""
    captured: list[Any] = []

    @queue.task()
    def use_logger(lgr: Any) -> None:
        captured.append(lgr)

    lgr = logging.getLogger("test.proxy.e2e")
    with queue.test_mode() as results:
        use_logger.delay(lgr)

    assert len(results) == 1
    assert results[0].succeeded
    assert captured[0] is lgr


def test_logger_proxy_marker_production(tmp_path: Any) -> None:
    """Logger produces a proxy marker when interception is on."""
    queue = Queue(db_path=str(tmp_path / "q.db"), interception="strict")
    lgr = logging.getLogger("test.proxy.marker")

    assert queue._interceptor is not None
    walker = queue._interceptor._walker
    args, _, _ = walker.walk((lgr,), {})
    assert args[0].get("__flexiq_proxy__") is True
    assert args[0]["handler"] == "logger"
    assert args[0]["recipe"]["name"] == "test.proxy.marker"


class TestReconstructionTimeout:
    """The reconstruction watchdog bounds caller wait time without blocking."""

    def test_run_with_timeout_returns_and_propagates(self) -> None:
        assert _run_with_timeout(lambda: 42, 1.0) == 42

        def boom() -> None:
            raise ValueError("handler failed")

        with pytest.raises(ValueError, match="handler failed"):
            _run_with_timeout(boom, 1.0)

    def test_run_with_timeout_unblocks_caller_on_hang(self) -> None:
        """A hung callable raises promptly — the caller never waits it out."""
        started = time.monotonic()
        with pytest.raises(_ReconstructionTimeout):
            _run_with_timeout(lambda: time.sleep(5), 0.05)
        # Returned on the ~0.05s budget, not after the 5s sleep.
        assert time.monotonic() - started < 1.0

    def test_slow_reconstruct_times_out_as_proxy_error(self) -> None:
        class SlowHandler:
            name = "slow"
            version = 1
            handled_types: tuple[type, ...] = ()

            def detect(self, obj: Any) -> bool:
                return False

            def deconstruct(self, obj: Any) -> dict[str, Any]:
                return {}

            def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
                time.sleep(5)
                return "never"

            def cleanup(self, obj: Any) -> None:
                pass

        reg = ProxyRegistry()
        reg.register(SlowHandler())
        marker = {
            "__flexiq_proxy__": True,
            "handler": "slow",
            "version": 1,
            "recipe": {},
        }

        started = time.monotonic()
        with pytest.raises(ProxyReconstructionError, match="timed out"):
            reconstruct_proxies((marker,), {}, reg, max_timeout=1)
        # Surfaced on the 1s budget rather than blocking for the 5s sleep.
        assert time.monotonic() - started < 3.0


def test_file_handler_rejects_sibling_prefix_bypass(tmp_path: Any) -> None:
    """A string-prefix check would let '<allow>_exfil' through; component
    containment must not (H4)."""
    allowed = tmp_path / "data"
    allowed.mkdir()
    sibling = tmp_path / "data_exfil"
    sibling.mkdir()
    secret = sibling / "secret.txt"
    secret.write_text("top-secret")

    handler = FileHandler(path_allowlist=[str(allowed)])
    recipe = {"path": str(secret), "mode": "r"}
    with pytest.raises(ProxyReconstructionError, match="not in the allowed"):
        handler.reconstruct(recipe, 1)


class TestDisabledProxies:
    """``disabled_proxies`` ids are a closed set — a typo must not pass silently."""

    def test_enum_member_disables_handler(self) -> None:
        registry = ProxyRegistry()
        register_builtin_handlers(registry, disabled_proxies=[BuiltInProxy.LOGGER])
        assert registry.get("logger") is None
        assert registry.get("file") is not None

    def test_wire_string_still_disables_handler(self) -> None:
        """Existing string callers keep working."""
        registry = ProxyRegistry()
        register_builtin_handlers(registry, disabled_proxies=["logger"])
        assert registry.get("logger") is None

    def test_unknown_id_raises_instead_of_silently_registering(self) -> None:
        """The bug this replaces: 'requests' left requests_session registered."""
        with pytest.raises(ValueError, match="requests_session"):
            register_builtin_handlers(ProxyRegistry(), disabled_proxies=["requests"])
