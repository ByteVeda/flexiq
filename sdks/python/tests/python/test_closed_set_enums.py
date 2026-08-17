"""Closed-set options accept an enum or its wire string, and reject anything else."""

from __future__ import annotations

from pathlib import Path

import pytest

from flexiq import InterceptionMode, PredicateAction, Queue, StorageBackend
from flexiq.dashboard.delivery_store import DeliveryStatus, DeliveryStore
from flexiq.dashboard.errors import _BadRequest
from flexiq.dashboard.handlers.webhook_deliveries import handle_list_deliveries
from flexiq.dashboard.webhook_store import WebhookSubscriptionStore
from flexiq.interception.strategy import Strategy
from flexiq.mixins._log_consumer import ConsumerErrorAction
from flexiq.workflows import Workflow, WorkflowCondition
from flexiq.workflows.types import DiagramFormat, FanStrategy, GateAction


def test_wire_values_are_the_cross_sdk_contract() -> None:
    """The persisted/wire form of every closed set is its lowercase string."""
    assert InterceptionMode.STRICT.value == "strict"
    assert PredicateAction.CANCEL.value == "cancel"
    assert ConsumerErrorAction.SKIP.value == "skip"
    assert GateAction.APPROVE.value == "approve"
    assert FanStrategy.EACH.value == "each"
    assert DiagramFormat.DOT.value == "dot"
    assert WorkflowCondition.ON_SUCCESS.value == "on_success"
    assert StorageBackend.POSTGRES.value == "postgres"
    assert Strategy.PROXY.value == "proxy"
    assert DeliveryStatus.DEAD.value == "dead"
    # A (str, Enum) member carries its value as its string content, which is what
    # the native layer reads — str() would render "FanStrategy.EACH" instead.
    assert FanStrategy.EACH.encode() == b"each"


def test_interception_accepts_enum_and_string(tmp_path: Path) -> None:
    enum_queue = Queue(db_path=str(tmp_path / "a.db"), interception=InterceptionMode.LENIENT)
    string_queue = Queue(db_path=str(tmp_path / "b.db"), interception="lenient")
    try:
        assert enum_queue._interceptor is not None
        assert enum_queue._interceptor.mode is InterceptionMode.LENIENT
        assert string_queue._interceptor is not None
        assert string_queue._interceptor.mode is InterceptionMode.LENIENT
    finally:
        enum_queue.close()
        string_queue.close()


def test_interception_typo_raises_naming_the_valid_set(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="'strict', 'lenient', 'off'"):
        Queue(db_path=str(tmp_path / "c.db"), interception="strictt")


def test_on_false_accepts_enum_and_string(queue: Queue) -> None:
    @queue.task(predicate=lambda: False, on_false=PredicateAction.CANCEL)
    def cancels() -> None:
        """Registered only to record its on_false setting."""

    @queue.task(predicate=lambda: False, on_false="defer")
    def defers() -> None:
        """Registered only to record its on_false setting."""

    stored = queue._task_predicate_on_false
    assert stored[cancels._task_name] is PredicateAction.CANCEL
    assert stored[defers._task_name] is PredicateAction.DEFER


def test_on_false_typo_raises(queue: Queue) -> None:
    with pytest.raises(ValueError, match="'defer', 'cancel'"):

        @queue.task(predicate=lambda: False, on_false="canccel")
        def bad() -> None:
            """Never registered — the option is rejected first."""


def test_log_consumer_on_error_typo_raises(queue: Queue) -> None:
    with pytest.raises(ValueError, match="'retry', 'skip'"):

        @queue.log_consumer("topic", on_error="ignore")
        def bad(message: object) -> None:
            """Never registered — the option is rejected first."""


def test_unknown_visualize_format_raises_instead_of_defaulting(queue: Queue) -> None:
    """Previously anything that wasn't 'dot' silently rendered Mermaid."""

    @queue.task()
    def step() -> None:
        """A single node, enough to render."""

    workflow = Workflow("wf").step("a", step)
    assert workflow.visualize(DiagramFormat.DOT).startswith("digraph")
    assert "graph" in workflow.visualize("mermaid")
    with pytest.raises(ValueError, match="'mermaid', 'dot'"):
        workflow.visualize("graphviz")


def test_step_condition_accepts_enum_and_string(queue: Queue) -> None:
    @queue.task()
    def step() -> None:
        """A node to gate."""

    wf = Workflow("wf").step("a", step)
    wf.step("b", step, after="a", condition=WorkflowCondition.ON_FAILURE)
    wf.step("c", step, after="a", condition="always")
    assert wf._steps["b"].condition is WorkflowCondition.ON_FAILURE
    assert wf._steps["c"].condition is WorkflowCondition.ALWAYS


def test_step_condition_typo_raises(queue: Queue) -> None:
    @queue.task()
    def step() -> None:
        """A node that never gets a valid condition."""

    wf = Workflow("wf").step("a", step)
    with pytest.raises(ValueError, match="'on_success', 'on_failure', 'always'"):
        wf.step("b", step, after="a", condition="on_succes")


def test_gate_condition_typo_raises(queue: Queue) -> None:
    @queue.task()
    def step() -> None:
        """Predecessor for the gate."""

    wf = Workflow("wf").step("a", step)
    with pytest.raises(ValueError, match="'on_success', 'on_failure', 'always'"):
        wf.gate("g", after="a", condition="whenever")


def test_backend_accepts_enum_and_string(tmp_path: Path) -> None:
    enum_queue = Queue(db_path=str(tmp_path / "a.db"), backend=StorageBackend.SQLITE)
    string_queue = Queue(db_path=str(tmp_path / "b.db"), backend="sqlite")
    try:
        # Normalized to the wire string so the display / native layer see "sqlite".
        assert enum_queue._backend == "sqlite"
        assert string_queue._backend == "sqlite"
    finally:
        enum_queue.close()
        string_queue.close()


def test_register_type_accepts_enum_and_string(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "a.db"), interception="lenient")
    try:
        queue.register_type(complex, Strategy.PASS)
        queue.register_type(bytes, "pass")
    finally:
        queue.close()


def test_register_type_typo_raises(tmp_path: Path) -> None:
    queue = Queue(db_path=str(tmp_path / "a.db"), interception="lenient")
    try:
        with pytest.raises(ValueError, match="'pass', 'convert'"):
            queue.register_type(complex, "passs")
    finally:
        queue.close()


def test_delivery_status_filter_round_trips_and_rejects_typo(queue: Queue) -> None:
    sub = WebhookSubscriptionStore(queue).create(url="https://example.test/hook", events=["*"])
    store = DeliveryStore(queue)
    store.record_attempt(
        sub.id, event="job.completed", payload={}, status=DeliveryStatus.DELIVERED, attempts=1
    )
    store.record_attempt(
        sub.id, event="job.failed", payload={}, status=DeliveryStatus.DEAD, attempts=3
    )

    only_dead = store.list_for(sub.id, status=DeliveryStatus.DEAD)
    assert [r.status for r in only_dead] == [DeliveryStatus.DEAD]

    body = handle_list_deliveries(queue, {"status": ["delivered"]}, sub.id)
    assert [item["status"] for item in body["items"]] == ["delivered"]

    with pytest.raises(_BadRequest, match="delivered, failed, dead"):
        handle_list_deliveries(queue, {"status": ["gone"]}, sub.id)
