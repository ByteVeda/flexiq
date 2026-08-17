"""Cursor-based pagination result."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class Page(Generic[T]):
    """One page of a keyset-paginated listing.

    Pass :attr:`next_cursor` back as ``after=`` to fetch the following page.
    ``next_cursor`` is ``None`` once there are no more rows. Treat the cursor
    string as opaque — do not construct or parse it yourself.
    """

    items: list[T]
    next_cursor: str | None

    def __iter__(self) -> Iterator[T]:
        """Iterate the page's items directly."""
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)
