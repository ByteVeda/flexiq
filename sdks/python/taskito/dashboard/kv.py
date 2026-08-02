"""Read-modify-write over the settings key/value store, without losing edits.

Every dashboard feature store keeps a whole JSON document under one settings
key. A plain read-then-write drops a concurrent edit wholesale — the later
writer wins with a document that never saw the earlier one — and more than one
dashboard replica against one backend is a supported deployment.

:func:`update` closes that: it writes conditionally on the value it read and
re-reads on a lost race. Writes here are admin-frequency, so contention is rare
and a retry is cheap.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, Protocol, TypeVar

__all__ = ["MAX_ATTEMPTS", "SettingConflictError", "SettingsKV", "encode", "update"]

#: How many times :func:`update` re-reads and retries before giving up.
#:
#: A losing writer only ever loses to a writer that won, so the bound has to
#: clear the number of dashboards that could be editing one document at once.
#: Losing this many in a row is a fault, not contention worth waiting out.
MAX_ATTEMPTS = 25

Document = TypeVar("Document")
Outcome = TypeVar("Outcome")


class SettingConflictError(RuntimeError):
    """Raised when :func:`update` lost ``MAX_ATTEMPTS`` races in a row."""

    def __init__(self, key: str) -> None:
        super().__init__(f"setting '{key}' kept changing under a conditional write")
        self.key = key


class SettingsKV(Protocol):
    """The slice of ``Queue`` the settings documents are stored through."""

    def get_setting(self, key: str) -> str | None: ...

    def set_setting_if(self, key: str, expected: str | None, value: str) -> bool: ...


def encode(document: Any) -> str:
    """Serialise a document compactly, matching what every SDK dashboard stores."""
    return json.dumps(document, separators=(",", ":"))


def update(
    kv: SettingsKV,
    key: str,
    load: Callable[[str | None], Document],
    mutate: Callable[[Document], Outcome],
) -> Outcome:
    """Load, mutate and store a document, retrying if someone else wrote first.

    ``load`` turns the raw stored value (``None`` when unset) into the document
    — the same decoding each store already does, so a malformed row keeps
    reading as empty. ``mutate`` must change the document **in place** and do
    nothing else: it runs once per attempt. Its return value comes back from the
    winning attempt.
    """
    for _ in range(MAX_ATTEMPTS):
        stored = kv.get_setting(key)
        document = load(stored)
        # Compared against the document as loaded, not against the raw stored
        # string: on a missing key the raw is None while the encoding is the
        # empty document, so comparing to the raw would read "changed nothing"
        # as a change and write a row for it.
        before = encode(document)
        outcome = mutate(document)
        after = encode(document)
        if after == before:
            return outcome
        if kv.set_setting_if(key, stored, after):
            return outcome
    raise SettingConflictError(key)
