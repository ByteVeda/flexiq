"""Pluggable provider Protocols used by recipe predicates."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from flexiq.predicates.context import PredicateContext


_TRUTHY = frozenset({"1", "true", "t", "yes", "y", "on"})


@runtime_checkable
class FeatureFlagProvider(Protocol):
    """Plug a feature-flag system (LaunchDarkly, Statsig, custom) into recipes.

    Implementations return ``True`` if the named flag is enabled for the
    given evaluation context. Errors should not propagate — the recipe is
    wrapped in fail-closed evaluation, but providers themselves should
    handle their own errors and return a sensible default rather than
    raising.
    """

    def is_enabled(self, name: str, ctx: PredicateContext, /) -> bool: ...


class _EnvFeatureFlagProvider:
    """Default provider: reads ``${prefix}{NAME}`` from process env.

    A value of any of ``1 / true / yes / on`` (case-insensitive) is
    considered enabled. Anything else, including a missing variable, is
    disabled.
    """

    __slots__ = ("_prefix",)

    def __init__(self, prefix: str = "FF_") -> None:
        self._prefix = prefix

    def is_enabled(self, name: str, ctx: PredicateContext, /) -> bool:
        key = f"{self._prefix}{name.upper()}"
        return os.environ.get(key, "").strip().lower() in _TRUTHY

    def __repr__(self) -> str:
        return f"EnvFeatureFlagProvider(prefix={self._prefix!r})"


def env_feature_flag_provider(prefix: str = "FF_") -> FeatureFlagProvider:
    """Build an env-var-backed :class:`FeatureFlagProvider`."""
    return _EnvFeatureFlagProvider(prefix=prefix)
