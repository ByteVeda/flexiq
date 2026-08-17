"""Shared workflow tracker dataclasses."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass
class _RunConfig:
    """In-memory configuration for a tracker-managed workflow run."""

    step_metadata: dict[str, dict[str, Any]]
    successors: dict[str, list[str]]
    predecessors: dict[str, list[str]]
    deferred_nodes: set[str]
    deferred_payloads: dict[str, bytes]
    on_failure: str
    callable_conditions: dict[str, Callable[..., bool]]
    gate_configs: dict[str, Any]
    sub_workflow_refs: dict[str, Any]
    # Maps step_name -> compensation_task_name. Populated from the
    # workflow's compiled compensation map at submit time. Empty when the
    # workflow has no saga steps.
    compensation_map: dict[str, str] = None  # type: ignore[assignment]
    # When True and on_failure="continue", a partial-failure run still
    # triggers compensation for the completed nodes (after transitioning
    # through the CompletedWithFailures state). When False (default),
    # continue-mode partial failures never compensate. Ignored for
    # on_failure="fail_fast" (which always compensates on terminal failure).
    compensate_on_continue: bool = False
    # Set to True by the tracker the first time a node ends in `failed`
    # state during a continue-mode run. Drives the
    # Completed -> CompletedWithFailures override at finalization.
    partial_failure_occurred: bool = False

    def __post_init__(self) -> None:
        if self.compensation_map is None:
            self.compensation_map = {}
