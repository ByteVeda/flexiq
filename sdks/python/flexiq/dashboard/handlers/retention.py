"""Retention route handler.

Echoes the windows the elected cleaner published for this namespace, so the
dashboard can explain why rows disappear from its listings. Retention runs in
the worker process, so this is never computed from local config — an
unreported policy is reported as such rather than guessed at.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from flexiq.dashboard.errors import _BadRequest
from flexiq.retention import RETENTION_TABLES, EffectiveRetention

if TYPE_CHECKING:
    from flexiq.app import Queue


def _windows(snapshot: EffectiveRetention | None) -> dict[str, int | None]:
    """Every table's window in ms, ``None`` for keep-forever and unreported."""
    windows = snapshot.windows if snapshot else {}
    return {f"{table}_ttl_ms": windows.get(table) for table in RETENTION_TABLES}


def _handle_retention(queue: Queue, _qs: dict) -> dict[str, Any]:
    """Return the published retention policy for this queue's namespace."""
    snapshot = queue.effective_retention()
    code = snapshot.code_windows if snapshot else None
    return {
        # Distinct from `enabled`: no worker has swept yet, so nothing is known
        # about the policy — not the same as retention being switched off.
        "reported": snapshot is not None,
        "enabled": snapshot.enabled if snapshot else False,
        "defaulted": snapshot.defaulted if snapshot else False,
        "namespace": snapshot.namespace if snapshot else None,
        "reported_at": snapshot.reported_at if snapshot else None,
        "windows": _windows(snapshot),
        "source": snapshot.source if snapshot else "code",
        "code_windows": (
            {f"{table}_ttl_ms": code.get(table) for table in RETENTION_TABLES}
            if code is not None
            else None
        ),
    }


# The override document is the core's, forwarded as parsed JSON, so its shape
# cannot drift from the other dashboards'. Through the native handle rather
# than a ``Queue`` method: it is a dashboard concern, not SDK surface.


def _handle_get_retention_override(queue: Queue, _qs: dict) -> dict[str, Any]:
    """The stored override, or the ``code`` default when none was saved."""
    document: dict[str, Any] = json.loads(queue._inner.retention_override())
    return document


def _handle_put_retention_override(queue: Queue, body: dict) -> dict[str, Any]:
    """Store the dashboard's windows and which source applies."""
    try:
        stored = queue._inner.set_retention_override(json.dumps(body))
    except ValueError as e:
        raise _BadRequest(str(e)) from None
    document: dict[str, Any] = json.loads(stored)
    return document


def _handle_delete_retention_override(queue: Queue) -> dict[str, bool]:
    """Forget the override; the code config applies from the next sweep."""
    return {"deleted": queue._inner.clear_retention_override()}


def _handle_retention_dry_run(queue: Queue, _qs: dict) -> dict[str, Any]:
    """Preview what a purge would delete under this queue's windows, without
    deleting. Computed in-process against the live data, so it always returns a
    document (never the unreported state ``/api/retention`` can report)."""
    preview = queue.dry_run_retention()
    return {
        "enabled": preview.enabled,
        "defaulted": preview.defaulted,
        "namespace": preview.namespace,
        "reference_time": preview.reference_time,
        "windows": {f"{table}_ttl_ms": preview.windows.get(table) for table in RETENTION_TABLES},
        "counts": {table: preview.counts.get(table, 0) for table in RETENTION_TABLES},
        "total": preview.total,
    }
