"""Dashboard settings (key/value store) accessor methods for the Queue."""

from __future__ import annotations

from typing import Any


class QueueSettingsMixin:
    """Persistent key/value settings backing the dashboard configuration page.

    Values are opaque strings as far as storage is concerned — callers that
    need structured data (lists, dicts, booleans) should ``json.dumps`` /
    ``json.loads`` around these methods. Settings are deployment-wide;
    every worker and dashboard instance pointed at the same backend sees
    the same values.
    """

    _inner: Any

    def get_setting(self, key: str) -> str | None:
        """Return the value for ``key``, or ``None`` if not set."""
        return self._inner.get_setting(key)  # type: ignore[no-any-return]

    def set_setting(self, key: str, value: str) -> None:
        """Insert or update a setting."""
        self._inner.set_setting(key, value)

    def set_setting_if(self, key: str, expected: str | None, value: str) -> bool:
        """Write ``key`` only if it still holds ``expected``.

        ``expected`` of ``None`` means the key must be unset. Returns ``False``
        when another writer got there first, so a caller that read the value it
        is deriving ``value`` from can re-read and retry instead of overwriting
        an edit it never saw. See :mod:`taskito.dashboard.kv`.
        """
        return self._inner.set_setting_if(key, expected, value)  # type: ignore[no-any-return]

    def delete_setting(self, key: str) -> bool:
        """Delete a setting. Returns ``True`` if the key existed."""
        return self._inner.delete_setting(key)  # type: ignore[no-any-return]

    def list_settings(self) -> dict[str, str]:
        """Return all settings as a ``{key: value}`` dict."""
        return self._inner.list_settings()  # type: ignore[no-any-return]

    def min_contract(self) -> int:
        """Return the lowest contract level a process may speak to open this storage.

        The contract level is the revision of the shared storage and wire
        contract an SDK build implements; a build below the floor refuses to
        open rather than misreading rows its contract never described.
        """
        return self._inner.min_contract()  # type: ignore[no-any-return]

    def set_min_contract(self, level: int) -> None:
        """Raise or lower the contract floor.

        Raise it only once every process in the deployment has been upgraded —
        older ones stop opening immediately. A level this build does not itself
        speak is rejected, since writing it would lock the caller out too.
        """
        self._inner.set_min_contract(level)
