"""Time-based predicates.

All recipes work with timezone-aware datetimes. ``tz`` arguments accept
an IANA timezone string (e.g. ``"US/Pacific"``); ``None`` means UTC.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from typing import TYPE_CHECKING, ClassVar

from flexiq.predicates.core import Predicate
from flexiq.predicates.outcomes import Defer
from flexiq.predicates.registry import PredicateValidationError

if TYPE_CHECKING:
    from flexiq.predicates.context import PredicateContext

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    _HAS_ZONEINFO = True
except ImportError:  # pragma: no cover - zoneinfo is stdlib on 3.9+
    _HAS_ZONEINFO = False

    class ZoneInfoNotFoundError(Exception):  # type: ignore[no-redef]
        pass


_ONE_DAY = timedelta(days=1)


def _resolve_tz(tz: str | None) -> tzinfo:
    if tz is None:
        return timezone.utc
    # Fast-path UTC so the predicate works on hosts that lack an IANA tz
    # database (notably Windows without the ``tzdata`` package).
    if tz.upper() == "UTC":
        return timezone.utc
    if not _HAS_ZONEINFO:
        raise PredicateValidationError(
            "zoneinfo is not available; pass tz=None or install Python 3.9+"
        )
    try:
        return ZoneInfo(tz)
    except ZoneInfoNotFoundError as exc:
        raise PredicateValidationError(
            f"unknown timezone: {tz!r} "
            "(on Windows, install the `tzdata` package for IANA timezone support)"
        ) from exc


def _parse_hhmm(value: str) -> tuple[int, int]:
    parts = value.split(":")
    if len(parts) != 2:
        raise PredicateValidationError(f"expected 'HH:MM', got {value!r}")
    try:
        h, m = int(parts[0]), int(parts[1])
    except ValueError as exc:
        raise PredicateValidationError(f"expected 'HH:MM', got {value!r}") from exc
    if not 0 <= h <= 23 or not 0 <= m <= 59:
        raise PredicateValidationError(f"hour/minute out of range: {value!r}")
    return h, m


def _seconds_until(now_local: datetime, target: datetime) -> float:
    return max(1.0, (target - now_local).total_seconds())


# -- is_business_hours ---------------------------------------------------


@dataclass(frozen=True)
class IsBusinessHours(Predicate):
    """Allow only Mon-Fri ``start_hour..end_hour`` in the configured tz.

    Outside the window the predicate returns :class:`Defer` aimed at the
    next opening, so jobs land neatly at the next business start.
    """

    OP: ClassVar[str | None] = "is_business_hours"

    start_hour: int = 9
    end_hour: int = 17
    tz: str | None = None
    weekdays_only: bool = True

    def __post_init__(self) -> None:
        if not (0 <= self.start_hour < 24 and 0 < self.end_hour <= 24):
            raise PredicateValidationError(
                f"is_business_hours: hours out of range ({self.start_hour}, {self.end_hour})"
            )
        if self.start_hour >= self.end_hour:
            raise PredicateValidationError(
                f"is_business_hours: start_hour ({self.start_hour}) must be < "
                f"end_hour ({self.end_hour})"
            )
        # Validate tz at construction.
        _resolve_tz(self.tz)

    def evaluate(self, ctx: PredicateContext) -> bool | Defer:
        local = ctx.now().astimezone(_resolve_tz(self.tz))
        if self.weekdays_only and local.weekday() >= 5:
            days_to_monday = 7 - local.weekday()
            target = (local + timedelta(days=days_to_monday)).replace(
                hour=self.start_hour, minute=0, second=0, microsecond=0
            )
            return Defer(seconds=_seconds_until(local, target))
        if local.hour < self.start_hour:
            target = local.replace(hour=self.start_hour, minute=0, second=0, microsecond=0)
            return Defer(seconds=_seconds_until(local, target))
        if local.hour >= self.end_hour:
            target = (local + _ONE_DAY).replace(
                hour=self.start_hour, minute=0, second=0, microsecond=0
            )
            return Defer(seconds=_seconds_until(local, target))
        return True


def is_business_hours(
    start_hour: int = 9,
    end_hour: int = 17,
    *,
    tz: str | None = None,
    weekdays_only: bool = True,
) -> Predicate:
    """Allow during business hours; defer to the next window otherwise."""
    return IsBusinessHours(
        start_hour=start_hour, end_hour=end_hour, tz=tz, weekdays_only=weekdays_only
    )


# -- is_weekend ----------------------------------------------------------


@dataclass(frozen=True)
class IsWeekend(Predicate):
    """True on Saturday or Sunday in the configured timezone."""

    OP: ClassVar[str | None] = "is_weekend"

    tz: str | None = None

    def __post_init__(self) -> None:
        _resolve_tz(self.tz)

    def evaluate(self, ctx: PredicateContext) -> bool:
        local = ctx.now().astimezone(_resolve_tz(self.tz))
        return local.weekday() >= 5


def is_weekend(*, tz: str | None = None) -> Predicate:
    """True when the current local day is Saturday or Sunday."""
    return IsWeekend(tz=tz)


# -- in_time_window ------------------------------------------------------


@dataclass(frozen=True)
class InTimeWindow(Predicate):
    """Allow during ``[start, end)``; defer otherwise.

    Stored as ``"HH:MM"`` strings so the AST serializes cleanly.
    """

    OP: ClassVar[str | None] = "in_time_window"

    start: str = "00:00"
    end: str = "23:59"
    tz: str | None = None

    def __post_init__(self) -> None:
        sh, sm = _parse_hhmm(self.start)
        eh, em = _parse_hhmm(self.end)
        if (sh, sm) >= (eh, em):
            raise PredicateValidationError(
                f"in_time_window: start ({self.start}) must be before end ({self.end})"
            )
        _resolve_tz(self.tz)

    def evaluate(self, ctx: PredicateContext) -> bool | Defer:
        sh, sm = _parse_hhmm(self.start)
        eh, em = _parse_hhmm(self.end)
        local = ctx.now().astimezone(_resolve_tz(self.tz))
        cur = local.hour * 60 + local.minute
        start_m = sh * 60 + sm
        end_m = eh * 60 + em
        if start_m <= cur < end_m:
            return True
        if cur < start_m:
            return Defer(seconds=(start_m - cur) * 60.0)
        target = (local + _ONE_DAY).replace(hour=sh, minute=sm, second=0, microsecond=0)
        return Defer(seconds=_seconds_until(local, target))


def in_time_window(start: str, end: str, *, tz: str | None = None) -> Predicate:
    """Allow during ``[start, end)`` in the configured timezone."""
    return InTimeWindow(start=start, end=end, tz=tz)


# -- after / before ------------------------------------------------------


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _parse_iso(value: str) -> datetime:
    try:
        dt = datetime.fromisoformat(value)
    except ValueError as exc:
        raise PredicateValidationError(f"invalid ISO datetime: {value!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass(frozen=True)
class After(Predicate):
    """Allow only when wall-clock time is at or past ``target``."""

    OP: ClassVar[str | None] = "after"

    target: str = ""

    def __post_init__(self) -> None:
        if not self.target:
            raise PredicateValidationError("after: target must be non-empty")
        _parse_iso(self.target)

    def evaluate(self, ctx: PredicateContext) -> bool | Defer:
        target = _parse_iso(self.target)
        now = ctx.now()
        if now >= target:
            return True
        return Defer(seconds=max(1.0, (target - now).total_seconds()))


def after(target: datetime | str) -> Predicate:
    """Allow only at or after ``target`` (datetime or ISO string)."""
    iso = _iso(target) if isinstance(target, datetime) else target
    return After(target=iso)


@dataclass(frozen=True)
class Before(Predicate):
    """Allow only when wall-clock time is strictly before ``target``."""

    OP: ClassVar[str | None] = "before"

    target: str = ""

    def __post_init__(self) -> None:
        if not self.target:
            raise PredicateValidationError("before: target must be non-empty")
        _parse_iso(self.target)

    def evaluate(self, ctx: PredicateContext) -> bool:
        return ctx.now() < _parse_iso(self.target)


def before(target: datetime | str) -> Predicate:
    """Allow only strictly before ``target`` (datetime or ISO string)."""
    iso = _iso(target) if isinstance(target, datetime) else target
    return Before(target=iso)


# -- in_timezone ---------------------------------------------------------


@dataclass(frozen=True)
class InTimezone(Predicate):
    """No-op predicate that validates ``tz`` resolves on this host."""

    OP: ClassVar[str | None] = "in_timezone"

    tz: str = "UTC"

    def __post_init__(self) -> None:
        _resolve_tz(self.tz)

    def evaluate(self, ctx: PredicateContext) -> bool:
        return True


def in_timezone(tz: str) -> Predicate:
    """Validate ``tz`` is installed; always allows at runtime."""
    return InTimezone(tz=tz)
