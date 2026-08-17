"""Frozen resource proxy — wraps an instance to prevent mutation."""

from __future__ import annotations

from typing import Any

from flexiq.exceptions import ResourceError


class FrozenResource:
    """Read-only proxy that raises on attribute mutation.

    Used for worker-scoped resources marked ``frozen=True``.
    """

    __slots__ = ("_frozen_instance", "_frozen_name")

    def __init__(self, instance: Any, resource_name: str) -> None:
        object.__setattr__(self, "_frozen_instance", instance)
        object.__setattr__(self, "_frozen_name", resource_name)

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_frozen_instance"), name)

    def __setattr__(self, name: str, value: Any) -> None:
        res_name = object.__getattribute__(self, "_frozen_name")
        raise ResourceError(
            f"Cannot modify frozen resource '{res_name}': attribute '{name}' is read-only"
        )

    def __delattr__(self, name: str) -> None:
        res_name = object.__getattribute__(self, "_frozen_name")
        raise ResourceError(
            f"Cannot modify frozen resource '{res_name}': attribute '{name}' is read-only"
        )

    def __repr__(self) -> str:
        instance = object.__getattribute__(self, "_frozen_instance")
        name = object.__getattribute__(self, "_frozen_name")
        return f"<FrozenResource({name!r}) wrapping {instance!r}>"
