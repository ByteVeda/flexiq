"""Fan-out splitting and fan-in aggregation strategies."""

from __future__ import annotations

from typing import Any

from flexiq.enums import coerce_enum
from flexiq.serializers import Serializer
from flexiq.workflows.types import FanStrategy


def apply_fan_out(strategy: FanStrategy | str, result: Any) -> list[Any]:
    """Split a step's return value into individual fan-out items.

    Args:
        strategy: The fan-out strategy — :attr:`FanStrategy.EACH`. Read back
            from persisted step metadata, so a string is accepted.
        result: The predecessor step's return value.

    Returns:
        A list of items, one per fan-out child job.

    Raises:
        TypeError: If the result is not iterable for the given strategy.
        ValueError: If the strategy is unknown.
    """
    if coerce_enum(FanStrategy, strategy, param="fan_out strategy") is FanStrategy.EACH:
        try:
            return list(result)
        except TypeError as exc:
            raise TypeError(
                f"fan_out='each' requires an iterable result, got {type(result).__name__}"
            ) from exc
    raise ValueError(f"unknown fan_out strategy: {strategy!r}")


def build_child_payload(item: Any, serializer: Serializer) -> bytes:
    """Serialize a single fan-out item as ``((item,), {})``."""
    return serializer.dumps(((item,), {}))


def build_fan_in_payload(results: list[Any], serializer: Serializer) -> bytes:
    """Serialize collected results as ``((results,), {})``."""
    return serializer.dumps(((results,), {}))
