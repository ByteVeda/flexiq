"""Tests for the saga API plumbing — schema + decorator + step override.

PR 5 only wires the data plumbing; the compensation orchestrator
(``SagaOrchestrator``) and tracker integration land in PR 6.
"""

from __future__ import annotations

import pytest

from flexiq import Queue
from flexiq.workflows import Workflow


def test_task_decorator_records_compensator(queue: Queue) -> None:
    @queue.task()
    def refund_payment(args: tuple, kwargs: dict, result: object) -> None:  # pragma: no cover
        pass

    @queue.task(compensates=refund_payment)
    def charge_payment(amount: int) -> str:  # pragma: no cover
        return f"charge-{amount}"

    assert queue._task_compensates[charge_payment.name] == refund_payment.name


def test_task_decorator_accepts_compensator_name_string(queue: Queue) -> None:
    @queue.task()
    def refund_v2(args: tuple, kwargs: dict, result: object) -> None:  # pragma: no cover
        pass

    @queue.task(compensates=refund_v2.name)
    def charge_v2(amount: int) -> str:  # pragma: no cover
        return f"charge-{amount}"

    assert queue._task_compensates[charge_v2.name] == refund_v2.name


def test_task_decorator_rejects_invalid_compensator(queue: Queue) -> None:
    with pytest.raises(TypeError, match="compensates="):

        @queue.task(compensates=42)  # type: ignore[arg-type]
        def bad(x: int) -> None:  # pragma: no cover
            pass


def test_workflow_step_inherits_decorator_default(queue: Queue) -> None:
    @queue.task()
    def refund(args: tuple, kwargs: dict, result: object) -> None:  # pragma: no cover
        pass

    @queue.task(compensates=refund)
    def charge(amount: int) -> str:  # pragma: no cover
        return f"charge-{amount}"

    wf = Workflow(name="saga_inherit_default")
    wf.step("charge", charge, args=(100,))
    wf._compile(queue)

    assert wf._compiled_compensation_map == {"charge": refund.name}


def test_workflow_step_overrides_decorator_default(queue: Queue) -> None:
    @queue.task()
    def refund_default(args: tuple, kwargs: dict, result: object) -> None:  # pragma: no cover
        pass

    @queue.task()
    def refund_step_override(
        args: tuple, kwargs: dict, result: object
    ) -> None:  # pragma: no cover
        pass

    @queue.task(compensates=refund_default)
    def charge_override(amount: int) -> str:  # pragma: no cover
        return f"charge-{amount}"

    wf = Workflow(name="saga_step_override")
    wf.step("charge", charge_override, args=(100,), compensates=refund_step_override)
    wf._compile(queue)

    assert wf._compiled_compensation_map == {"charge": refund_step_override.name}


def test_workflow_step_disables_with_none(queue: Queue) -> None:
    @queue.task()
    def refund(args: tuple, kwargs: dict, result: object) -> None:  # pragma: no cover
        pass

    @queue.task(compensates=refund)
    def charge_disabled(amount: int) -> str:  # pragma: no cover
        return f"charge-{amount}"

    wf = Workflow(name="saga_step_disabled")
    wf.step("charge", charge_disabled, args=(100,), compensates=None)
    wf._compile(queue)

    assert "charge" not in wf._compiled_compensation_map


def test_workflow_step_without_compensator_is_omitted(queue: Queue) -> None:
    @queue.task()
    def plain_step(amount: int) -> int:  # pragma: no cover
        return amount * 2

    wf = Workflow(name="saga_plain")
    wf.step("plain", plain_step, args=(100,))
    wf._compile(queue)

    assert wf._compiled_compensation_map == {}
