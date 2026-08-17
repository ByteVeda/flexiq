"""Cross-cutting closed-set enums plus the coercion helper for entry points."""

from __future__ import annotations

import enum
from typing import TypeVar

E = TypeVar("E", bound=enum.Enum)


class StorageBackend(str, enum.Enum):
    """Storage backend for a :class:`~taskito.app.Queue`."""

    SQLITE = "sqlite"
    POSTGRES = "postgres"
    REDIS = "redis"


class OnExcess(str, enum.Enum):
    """What a saturated rate limit does to a task's jobs.

    ``DEFER`` keeps the job and retries it on a later dispatch cycle.
    ``DROP`` sheds it to the dead-letter queue, for traffic whose value
    expires with the moment and is worth less than the backlog it would build.
    """

    DEFER = "defer"
    DROP = "drop"


def coerce_enum(enum_cls: type[E], value: E | str, *, param: str) -> E:
    """Accept an enum member or its wire string, else raise naming the valid set.

    Public entry points keep taking plain strings, so a typo would otherwise
    surface as a bare ``'x' is not a valid Y`` — or, where nothing validated,
    not at all.
    """
    try:
        return enum_cls(value)
    except ValueError:
        valid = ", ".join(repr(member.value) for member in enum_cls)
        raise ValueError(f"{param} must be one of {valid}, got {value!r}") from None
