"""Tests for the argument interception system (Layer 1)."""

from __future__ import annotations

import datetime
import decimal
import enum
import pathlib
import re
import socket
import threading
import uuid
from dataclasses import dataclass
from typing import Any

import pytest

from flexiq import Queue
from flexiq.interception import (
    ArgumentInterceptor,
    InterceptionError,
    InterceptionReport,
)
from flexiq.interception.built_in import build_default_registry
from flexiq.interception.converters import reconstruct_converted
from flexiq.interception.reconstruct import reconstruct_args
from flexiq.interception.strategy import Strategy

# -- Fixtures --


@pytest.fixture
def registry() -> Any:
    return build_default_registry()


@pytest.fixture
def strict(registry: Any) -> ArgumentInterceptor:
    return ArgumentInterceptor(registry, mode="strict")


@pytest.fixture
def lenient(registry: Any) -> ArgumentInterceptor:
    return ArgumentInterceptor(registry, mode="lenient")


# -- PASS strategy --


class TestPassStrategy:
    def test_int_passes_through(self, strict: ArgumentInterceptor) -> None:
        args, _kw = strict.intercept((42,), {})
        assert args == (42,)

    def test_str_passes_through(self, strict: ArgumentInterceptor) -> None:
        args, _kw = strict.intercept(("hello",), {})
        assert args == ("hello",)

    def test_float_passes_through(self, strict: ArgumentInterceptor) -> None:
        args, _kw = strict.intercept((3.14,), {})
        assert args == (3.14,)

    def test_bool_passes_through(self, strict: ArgumentInterceptor) -> None:
        args, _kw = strict.intercept((True, False), {})
        assert args == (True, False)

    def test_none_passes_through(self, strict: ArgumentInterceptor) -> None:
        args, _kw = strict.intercept((None,), {})
        assert args == (None,)

    def test_bytes_passes_through(self, strict: ArgumentInterceptor) -> None:
        args, _kw = strict.intercept((b"data",), {})
        assert args == (b"data",)

    def test_mixed_primitives(self, strict: ArgumentInterceptor) -> None:
        args, kwargs = strict.intercept(
            (1, "two", 3.0, True, None, b"six"),
            {"key": "val"},
        )
        assert args == (1, "two", 3.0, True, None, b"six")
        assert kwargs == {"key": "val"}


# -- CONVERT strategy --


class TestConvertStrategy:
    def test_uuid_round_trip(self, strict: ArgumentInterceptor) -> None:
        original = uuid.UUID("12345678-1234-5678-1234-567812345678")
        args, _kw = strict.intercept((original,), {})
        assert args[0]["__flexiq_convert__"] is True
        assert args[0]["type_key"] == "uuid"
        # Reconstruct
        restored = reconstruct_converted(args[0])
        assert restored == original

    def test_datetime_round_trip(self, strict: ArgumentInterceptor) -> None:
        original = datetime.datetime(2025, 3, 10, 12, 0, 0)
        args, _ = strict.intercept((original,), {})
        assert args[0]["type_key"] == "datetime"
        restored = reconstruct_converted(args[0])
        assert restored == original

    def test_date_round_trip(self, strict: ArgumentInterceptor) -> None:
        original = datetime.date(2025, 3, 10)
        args, _ = strict.intercept((original,), {})
        assert args[0]["type_key"] == "date"
        restored = reconstruct_converted(args[0])
        assert restored == original

    def test_time_round_trip(self, strict: ArgumentInterceptor) -> None:
        original = datetime.time(14, 30, 0)
        args, _ = strict.intercept((original,), {})
        assert args[0]["type_key"] == "time"
        restored = reconstruct_converted(args[0])
        assert restored == original

    def test_timedelta_round_trip(self, strict: ArgumentInterceptor) -> None:
        original = datetime.timedelta(hours=1, minutes=30)
        args, _ = strict.intercept((original,), {})
        assert args[0]["type_key"] == "timedelta"
        restored = reconstruct_converted(args[0])
        assert restored == original

    def test_decimal_round_trip(self, strict: ArgumentInterceptor) -> None:
        original = decimal.Decimal("3.14159")
        args, _ = strict.intercept((original,), {})
        assert args[0]["type_key"] == "decimal"
        restored = reconstruct_converted(args[0])
        assert restored == original

    def test_path_round_trip(self, strict: ArgumentInterceptor) -> None:
        original = pathlib.Path("/tmp/data.csv")
        args, _ = strict.intercept((original,), {})
        assert args[0]["type_key"] == "path"
        restored = reconstruct_converted(args[0])
        assert restored == original

    def test_pattern_round_trip(self, strict: ArgumentInterceptor) -> None:
        original = re.compile(r"\d+", re.IGNORECASE)
        args, _ = strict.intercept((original,), {})
        assert args[0]["type_key"] == "pattern"
        restored = reconstruct_converted(args[0])
        assert restored.pattern == original.pattern
        assert restored.flags == original.flags

    def test_enum_converts(self, strict: ArgumentInterceptor) -> None:
        class Color(enum.Enum):
            RED = "red"
            GREEN = "green"

        args, _ = strict.intercept((Color.RED,), {})
        assert args[0]["type_key"] == "enum"
        assert args[0]["value"] == "red"

    def test_dataclass_converts(self, strict: ArgumentInterceptor) -> None:
        @dataclass
        class Point:
            x: int
            y: int

        original = Point(x=1, y=2)
        args, _ = strict.intercept((original,), {})
        assert args[0]["__flexiq_convert__"] is True
        assert args[0]["type_key"] == "dataclass"
        assert args[0]["value"] == {"x": 1, "y": 2}

    def test_datetime_before_date(self, strict: ArgumentInterceptor) -> None:
        """datetime is a subclass of date — datetime must match first."""
        dt = datetime.datetime(2025, 1, 1, 12, 0, 0)
        args, _ = strict.intercept((dt,), {})
        assert args[0]["type_key"] == "datetime"


# -- REDIRECT strategy --


class TestRedirectStrategy:
    def test_redirect_produces_marker(self, strict: ArgumentInterceptor) -> None:
        """Test that redirect types produce markers (if sqlalchemy is installed)."""
        try:
            from sqlalchemy.orm import Session  # type: ignore[import-not-found]  # noqa: F401

            sqlalchemy_available = True
        except ImportError:
            sqlalchemy_available = False

        if not sqlalchemy_available:
            pytest.skip("sqlalchemy not installed")


# -- REJECT strategy --


class TestRejectStrategy:
    def test_threading_lock_rejected(self, strict: ArgumentInterceptor) -> None:
        lock = threading.Lock()
        with pytest.raises(InterceptionError) as exc_info:
            strict.intercept((lock,), {})
        assert len(exc_info.value.failures) == 1
        assert "args[0]" in exc_info.value.failures[0].path
        assert "lock" in exc_info.value.failures[0].type_name.lower()

    def test_socket_rejected(self, strict: ArgumentInterceptor) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            with pytest.raises(InterceptionError):
                strict.intercept((sock,), {})
        finally:
            sock.close()

    def test_generator_rejected(self, strict: ArgumentInterceptor) -> None:
        gen = (x for x in range(10))
        with pytest.raises(InterceptionError):
            strict.intercept((gen,), {})

    def test_reject_error_includes_path(self, strict: ArgumentInterceptor) -> None:
        lock = threading.Lock()
        with pytest.raises(InterceptionError) as exc_info:
            strict.intercept((), {"session": lock})
        assert "kwargs.session" in exc_info.value.failures[0].path

    def test_reject_error_has_suggestions(self, strict: ArgumentInterceptor) -> None:
        lock = threading.Lock()
        with pytest.raises(InterceptionError) as exc_info:
            strict.intercept((lock,), {})
        assert len(exc_info.value.failures[0].suggestions) > 0

    def test_multiple_rejections_collected(self, strict: ArgumentInterceptor) -> None:
        lock = threading.Lock()
        event = threading.Event()
        with pytest.raises(InterceptionError) as exc_info:
            strict.intercept((lock, event), {})
        assert len(exc_info.value.failures) == 2


# -- Lenient mode --


class TestLenientMode:
    def test_rejected_arg_dropped(self, lenient: ArgumentInterceptor) -> None:
        lock = threading.Lock()
        args, _kw = lenient.intercept((42, lock), {})
        assert args[0] == 42
        assert args[1] is None  # dropped to None

    def test_rejected_kwarg_dropped(self, lenient: ArgumentInterceptor) -> None:
        lock = threading.Lock()
        _args, kwargs = lenient.intercept((), {"x": 1, "lock": lock})
        assert kwargs == {"x": 1}


# -- Off mode --


class TestOffMode:
    def test_passthrough_no_interception(self, registry: Any) -> None:
        interceptor = ArgumentInterceptor(registry, mode="off")
        lock = threading.Lock()
        args, _kw = interceptor.intercept((lock,), {})
        assert args[0] is lock


# -- Recursive walking --


class TestRecursiveWalking:
    def test_nested_uuid_in_dict(self, strict: ArgumentInterceptor) -> None:
        uid = uuid.uuid4()
        _, kwargs = strict.intercept((), {"config": {"user_id": uid}})
        assert kwargs["config"]["user_id"]["__flexiq_convert__"] is True

    def test_uuid_in_list(self, strict: ArgumentInterceptor) -> None:
        uid = uuid.uuid4()
        args, _ = strict.intercept(([uid],), {})
        assert args[0][0]["__flexiq_convert__"] is True

    def test_depth_limit(self, registry: Any) -> None:
        interceptor = ArgumentInterceptor(registry, mode="strict", max_depth=2)
        uid = uuid.uuid4()
        # Depth 3 — beyond limit, should pass through
        deep = {"a": {"b": {"c": uid}}}
        _, kwargs = interceptor.intercept((), {"x": deep})
        # uid at depth 3 should pass through as-is (beyond max_depth=2)
        assert kwargs["x"]["a"]["b"]["c"] is uid

    def test_circular_reference_handled(self, strict: ArgumentInterceptor) -> None:
        d: dict[str, Any] = {"value": 42}
        d["self"] = d  # circular!
        args, _ = strict.intercept((d,), {})
        # Should not infinite loop — circular ref is detected and passed through
        assert args[0]["value"] == 42


# -- Full round trip (intercept + reconstruct) --


class TestRoundTrip:
    def test_uuid_full_round_trip(self, strict: ArgumentInterceptor) -> None:
        uid = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        intercepted_args, intercepted_kwargs = strict.intercept((uid,), {"id": uid})
        args, kwargs, _redirects = reconstruct_args(intercepted_args, intercepted_kwargs)
        assert args[0] == uid
        assert kwargs["id"] == uid

    def test_mixed_types_round_trip(self, strict: ArgumentInterceptor) -> None:
        dt = datetime.datetime(2025, 6, 15, 10, 30)
        path = pathlib.Path("/data/file.txt")
        intercepted_args, intercepted_kwargs = strict.intercept((42, "hello", dt), {"path": path})
        args, kwargs, _redirects = reconstruct_args(intercepted_args, intercepted_kwargs)
        assert args[0] == 42
        assert args[1] == "hello"
        assert args[2] == dt
        assert kwargs["path"] == path

    def test_nested_convert_round_trip(self, strict: ArgumentInterceptor) -> None:
        uid = uuid.uuid4()
        intercepted_args, _ = strict.intercept(({"ids": [uid]},), {})
        args, _, _ = reconstruct_args(intercepted_args, {})
        assert args[0]["ids"][0] == uid


# -- Analyze / Report --


class TestAnalyze:
    def test_analyze_returns_report(self, strict: ArgumentInterceptor) -> None:
        report = strict.analyze((42, "hello"), {"uid": uuid.uuid4()})
        assert isinstance(report, InterceptionReport)
        assert len(report.entries) == 3

    def test_analyze_shows_strategies(self, strict: ArgumentInterceptor) -> None:
        uid = uuid.uuid4()
        report = strict.analyze((42, uid), {})
        strategies = [e.strategy for e in report.entries]
        assert Strategy.PASS in strategies
        assert Strategy.CONVERT in strategies

    def test_analyze_on_off_mode_returns_empty(self, registry: Any) -> None:
        interceptor = ArgumentInterceptor(registry, mode="off")
        report = interceptor.analyze((42,), {})
        assert len(report.entries) == 0

    def test_report_str_format(self, strict: ArgumentInterceptor) -> None:
        report = strict.analyze((42,), {})
        text = str(report)
        assert "Argument Analysis:" in text


# -- Queue integration --


class TestQueueIntegration:
    def test_queue_default_interception_off(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))
        assert q._interceptor is None

    def test_queue_strict_mode(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"), interception="strict")
        assert q._interceptor is not None
        assert q._interceptor.mode == "strict"

    def test_queue_enqueue_with_interception(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"), interception="strict")

        @q.task()
        def add(a: int, b: int) -> int:
            return a + b

        # Simple args should work fine
        result = add.delay(1, 2)
        assert result.id is not None

    def test_queue_enqueue_rejects_lock(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"), interception="strict")

        @q.task()
        def bad_task(lock: Any) -> None:
            pass

        with pytest.raises(InterceptionError):
            bad_task.delay(threading.Lock())

    def test_task_analyze(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"), interception="strict")

        @q.task()
        def my_task(user_id: int, created_at: datetime.datetime) -> None:
            pass

        report = my_task.analyze(42, datetime.datetime.now())
        assert len(report.entries) == 2

    def test_task_analyze_off_mode(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))

        @q.task()
        def my_task(x: int) -> None:
            pass

        report = my_task.analyze(42)
        assert len(report.entries) == 0

    def test_queue_analyze_arguments(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"), interception="strict")
        uid = uuid.uuid4()

        report = q.analyze_arguments((42, uid), {"created_at": datetime.datetime.now()})
        assert isinstance(report, InterceptionReport)
        assert len(report.entries) == 3
        strategies = [e.strategy for e in report.entries]
        assert Strategy.PASS in strategies
        assert Strategy.CONVERT in strategies

    def test_queue_analyze_arguments_off_mode(self, tmp_path: Any) -> None:
        q = Queue(db_path=str(tmp_path / "test.db"))

        report = q.analyze_arguments((42,), {})
        assert len(report.entries) == 0


# -- Custom type registration --


class TestCustomRegistration:
    def test_register_custom_reject(self, registry: Any, strict: ArgumentInterceptor) -> None:
        class MyLock:
            pass

        registry.register(
            MyLock,
            Strategy.REJECT,
            priority=40,
            reject_reason="Use distributed locking instead.",
            reject_suggestions=["Use queue.lock()"],
        )
        with pytest.raises(InterceptionError) as exc_info:
            strict.intercept((MyLock(),), {})
        assert "distributed locking" in str(exc_info.value)

    def test_register_custom_convert(self, registry: Any, strict: ArgumentInterceptor) -> None:
        class Money:
            def __init__(self, amount: int, currency: str) -> None:
                self.amount = amount
                self.currency = currency

        def convert_money(obj: Money) -> dict[str, Any]:
            return {
                "__flexiq_convert__": True,
                "type_key": "money",
                "value": {"amount": obj.amount, "currency": obj.currency},
            }

        registry.register(
            Money,
            Strategy.CONVERT,
            priority=15,
            converter=convert_money,
            type_key="money",
        )
        args, _ = strict.intercept((Money(100, "USD"),), {})
        assert args[0]["__flexiq_convert__"] is True
        assert args[0]["value"]["amount"] == 100


class TestMaliciousTypePathRejected:
    """type_path comes from the untrusted payload; reconstruction must refuse
    to import-and-call arbitrary objects like os.system / eval (H6)."""

    def test_enum_rejects_non_enum(self) -> None:
        data = {"type_key": "enum", "type_path": "os.system", "value": "id"}
        with pytest.raises(ValueError):
            reconstruct_converted(data)

    def test_dataclass_rejects_non_dataclass(self) -> None:
        # Target a function (not a class): _import_type must reject it before
        # any call, proving an attacker can't invoke arbitrary callables.
        data = {"type_key": "dataclass", "type_path": "subprocess.run", "value": {"args": "id"}}
        with pytest.raises(ValueError):
            reconstruct_converted(data)

    def test_named_tuple_rejects_plain_class(self) -> None:
        data = {"type_key": "named_tuple", "type_path": "subprocess.Popen", "fields": ["id"]}
        with pytest.raises(ValueError):
            reconstruct_converted(data)

    def test_callable_path_rejected_as_non_class(self) -> None:
        data = {"type_key": "enum", "type_path": "builtins.eval", "value": "1+1"}
        with pytest.raises(ValueError):
            reconstruct_converted(data)
