"""Middleware discovery and per-task disable management on :class:`Queue`."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flexiq.dashboard.middleware_store import MiddlewareDisableStore
from flexiq.middleware import middleware_class_path, middleware_key

if TYPE_CHECKING:
    from flexiq.middleware import TaskMiddleware


class QueueMiddlewareAdminMixin:
    """Discovery + per-task enable/disable for registered middlewares."""

    _global_middleware: list[TaskMiddleware]
    _task_middleware: dict[str, list[TaskMiddleware]]
    _mw_disable_version: int

    def _bump_mw_disable_version(self) -> None:
        """Invalidate the per-task middleware-chain cache for same-process
        readers after a disable-list change."""
        self._mw_disable_version += 1

    # ── Discovery ──────────────────────────────────────────────────

    def list_middleware(self) -> list[dict[str, Any]]:
        """Return every registered middleware (global + per-task) with its
        name, source ("global" or task name), and Python class path. The
        ``name`` is the value the disable list keys on."""
        seen: dict[str, dict[str, Any]] = {}
        for mw in self._global_middleware:
            name = middleware_key(mw)
            seen.setdefault(
                name,
                {
                    "name": name,
                    "class_path": middleware_class_path(mw),
                    "scopes": [],
                },
            )["scopes"].append({"kind": "global"})
        for task_name, mws in self._task_middleware.items():
            for mw in mws:
                name = middleware_key(mw)
                entry = seen.setdefault(
                    name,
                    {
                        "name": name,
                        "class_path": middleware_class_path(mw),
                        "scopes": [],
                    },
                )
                entry["scopes"].append({"kind": "task", "task": task_name})
        return sorted(seen.values(), key=lambda x: x["name"])

    # ── Disable management ─────────────────────────────────────────

    def list_middleware_disables(self) -> dict[str, list[str]]:
        """Return every task that has at least one disabled middleware."""
        return MiddlewareDisableStore(self).list_all()  # type: ignore[arg-type]

    def get_disabled_middleware_for(self, task_name: str) -> list[str]:
        return MiddlewareDisableStore(self).get_for(task_name)  # type: ignore[arg-type]

    def disable_middleware_for_task(self, task_name: str, mw_name: str) -> list[str]:
        result = MiddlewareDisableStore(self).set_disabled(  # type: ignore[arg-type]
            task_name, mw_name, disabled=True
        )
        self._bump_mw_disable_version()
        return result

    def enable_middleware_for_task(self, task_name: str, mw_name: str) -> list[str]:
        result = MiddlewareDisableStore(self).set_disabled(  # type: ignore[arg-type]
            task_name, mw_name, disabled=False
        )
        self._bump_mw_disable_version()
        return result

    def clear_middleware_disables(self, task_name: str) -> bool:
        result = MiddlewareDisableStore(self).clear_for(task_name)  # type: ignore[arg-type]
        self._bump_mw_disable_version()
        return result
