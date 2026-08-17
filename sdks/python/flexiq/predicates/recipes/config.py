"""Predicates that consult external configuration (env vars, feature flags)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from flexiq.predicates.core import Predicate
from flexiq.predicates.providers import (
    FeatureFlagProvider,
    env_feature_flag_provider,
)
from flexiq.predicates.registry import PredicateValidationError

if TYPE_CHECKING:
    from flexiq.predicates.context import PredicateContext


_TRUTHY = frozenset({"1", "true", "t", "yes", "y", "on"})


@dataclass(frozen=True)
class EnvVarTruthy(Predicate):
    """Allow when env var ``name`` is set to a truthy value."""

    OP: ClassVar[str | None] = "env_var_truthy"

    name: str = ""

    def __post_init__(self) -> None:
        if not self.name:
            raise PredicateValidationError("env_var_truthy: name must be non-empty")

    def evaluate(self, ctx: PredicateContext) -> bool:
        return os.environ.get(self.name, "").strip().lower() in _TRUTHY


def env_var_truthy(name: str) -> Predicate:
    """Allow when env var ``name`` is set to ``1``/``true``/``yes``/``on``."""
    return EnvVarTruthy(name=name)


# Per-process feature-flag providers, addressable by stable string id.
# Allows ``feature_flag("billing", provider="my-ld")`` to round-trip
# through JSON/string forms — the provider object itself isn't JSON,
# but its registered name is.
_FF_PROVIDERS: dict[str, FeatureFlagProvider] = {}


def register_feature_flag_provider(name: str, provider: FeatureFlagProvider) -> None:
    """Register a :class:`FeatureFlagProvider` under a stable string id.

    Once registered, ``feature_flag("flag", provider="<name>")``
    serializes and deserializes cleanly. The default ``"env"`` provider
    is always available.
    """
    if not name:
        raise PredicateValidationError("provider name must be non-empty")
    _FF_PROVIDERS[name] = provider


def _resolve_ff_provider(
    value: FeatureFlagProvider | str | None,
) -> tuple[FeatureFlagProvider, str]:
    """Return (provider, stable_name)."""
    if value is None:
        return env_feature_flag_provider(), "env"
    if isinstance(value, str):
        if value == "env":
            return env_feature_flag_provider(), "env"
        try:
            return _FF_PROVIDERS[value], value
        except KeyError:
            raise PredicateValidationError(
                f"unknown feature-flag provider {value!r}; register it via "
                "register_feature_flag_provider() before deserializing"
            ) from None
    # Instance — best-effort reverse lookup; falls back to a synthetic
    # marker that fails clean round-trip.
    for stored_name, stored in _FF_PROVIDERS.items():
        if stored is value:
            return value, stored_name
    return value, "<custom>"


@dataclass(frozen=True)
class FeatureFlag(Predicate):
    """Allow when feature flag ``flag`` is enabled via ``provider``."""

    OP: ClassVar[str | None] = "feature_flag"

    flag: str = ""
    provider: str = "env"
    _resolved: FeatureFlagProvider | None = field(
        default=None, init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not self.flag:
            raise PredicateValidationError("feature_flag: flag must be non-empty")
        resolved, _name = _resolve_ff_provider(self.provider)
        object.__setattr__(self, "_resolved", resolved)

    def evaluate(self, ctx: PredicateContext) -> bool:
        assert self._resolved is not None  # set in __post_init__
        return bool(self._resolved.is_enabled(self.flag, ctx))

    def to_dict(self) -> dict[str, Any]:
        return {"op": self.OP, "flag": self.flag, "provider": self.provider}


def feature_flag(
    name: str,
    *,
    provider: FeatureFlagProvider | str | None = None,
) -> Predicate:
    """Allow when feature flag ``name`` is enabled.

    ``provider`` may be:

    * ``None`` (default) — env-var-backed reading ``FF_<NAME>``.
    * A registered provider name (e.g. ``"launchdarkly"``) previously
      bound via :func:`register_feature_flag_provider`.
    * A :class:`FeatureFlagProvider` instance — used directly. If the
      instance was registered, serialization round-trips by name;
      otherwise serialization emits ``"<custom>"`` and re-deserializing
      requires re-binding.
    """
    _resolved, stable_name = _resolve_ff_provider(provider)
    return FeatureFlag(flag=name, provider=stable_name)
