"""Inject["resource_name"] annotation for resource injection."""

from __future__ import annotations

from typing import Any


class _InjectAlias:
    """Resolved marker from ``Inject["name"]``."""

    def __init__(self, resource_name: str) -> None:
        self.resource_name = resource_name

    def __repr__(self) -> str:
        return f'Inject["{self.resource_name}"]'

    def __eq__(self, other: object) -> bool:
        if isinstance(other, _InjectAlias):
            return self.resource_name == other.resource_name
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.resource_name)


class _InjectMeta(type):
    def __getitem__(cls, resource_name: str) -> Any:
        return _InjectAlias(resource_name)


class Inject(metaclass=_InjectMeta):
    """Marker for resource injection via type annotations.

    Usage::

        from flexiq import Inject

        @queue.task()
        def process(order_id: int, db: Inject["db"]):
            # db is injected from the worker's resource runtime
            ...
    """
