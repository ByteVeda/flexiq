"""Prefork worker pool — multi-process execution for CPU-bound tasks."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PreforkConfig:
    """Configuration for the prefork worker pool.

    Attributes:
        app: Import path to the Queue instance (e.g. ``"myapp:queue"``).
        workers: Number of child worker processes.
        max_tasks_per_child: Recycle a child after this many tasks (0 = never).
    """

    app: str
    workers: int = 4
    max_tasks_per_child: int = 0
