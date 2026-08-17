"""Predicate-driven middleware filtering tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from flexiq.app import Queue
from flexiq.context import JobContext
from flexiq.middleware import TaskMiddleware, legacy_task_filter_to_predicate
from flexiq.predicates import Predicate, PredicateContext


@dataclass(frozen=True)
class _Const(Predicate):
    value: Any

    def evaluate(self, ctx: PredicateContext) -> Any:
        return self.value


class _RecordingMiddleware(TaskMiddleware):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.before_calls: list[str] = []
        self.after_calls: list[str] = []
        self.enqueue_calls: list[str] = []

    def before(self, ctx: JobContext) -> None:
        self.before_calls.append(ctx.task_name)

    def after(self, ctx: JobContext, result: Any, error: Exception | None) -> None:
        self.after_calls.append(ctx.task_name)

    def on_enqueue(self, task_name: str, args: tuple, kwargs: dict, options: dict) -> None:
        self.enqueue_calls.append(task_name)


def test_no_predicate_means_always_apply(tmp_path: Any) -> None:
    mw = _RecordingMiddleware()
    queue = Queue(db_path=str(tmp_path / "t.db"), workers=1, middleware=[mw])

    @queue.task()
    def t() -> str:
        return "ok"

    t.delay()
    assert mw.enqueue_calls == [t.name]


def test_legacy_task_filter_callable_still_works(tmp_path: Any) -> None:
    mw = _RecordingMiddleware(predicate=None)  # set up base first
    # Then re-init via the legacy helper
    mw2 = _RecordingMiddleware(
        predicate=legacy_task_filter_to_predicate(lambda name: name.endswith(".allowed"), None)
    )
    queue = Queue(db_path=str(tmp_path / "t.db"), workers=1, middleware=[mw, mw2])

    @queue.task(name="x.allowed")
    def allowed() -> int:
        return 1

    @queue.task(name="x.blocked")
    def blocked() -> int:
        return 1

    allowed.delay()
    blocked.delay()

    assert mw2.enqueue_calls == ["x.allowed"]
    assert mw.enqueue_calls == ["x.allowed", "x.blocked"]


def test_predicate_filters_enqueue_hook(tmp_path: Any) -> None:
    mw = _RecordingMiddleware(predicate=_Const(False))
    queue = Queue(db_path=str(tmp_path / "t.db"), workers=1, middleware=[mw])

    @queue.task()
    def t() -> str:
        return "ok"

    t.delay()
    assert mw.enqueue_calls == []


def test_predicate_with_callable(tmp_path: Any) -> None:
    mw = _RecordingMiddleware(predicate=lambda ctx: ctx.task_name.endswith(".ok"))
    queue = Queue(db_path=str(tmp_path / "t.db"), workers=1, middleware=[mw])

    @queue.task(name="x.ok")
    def ok() -> int:
        return 1

    @queue.task(name="x.skip")
    def skip() -> int:
        return 1

    ok.delay()
    skip.delay()
    assert mw.enqueue_calls == ["x.ok"]


def test_legacy_helper_predicate_wins_when_both_supplied() -> None:
    pred = _Const(True)
    legacy = lambda name: False  # noqa: E731
    assert legacy_task_filter_to_predicate(legacy, pred) is pred


def test_legacy_helper_returns_none_when_both_none() -> None:
    assert legacy_task_filter_to_predicate(None, None) is None
