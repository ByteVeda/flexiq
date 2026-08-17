"""Middleware discovery + per-task enable/disable endpoints."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from flexiq.dashboard.errors import _BadRequest, _NotFound
from flexiq.middleware import middleware_class_path, middleware_key

if TYPE_CHECKING:
    from flexiq.app import Queue


def handle_list_middleware(queue: Queue, _qs: dict) -> list[dict[str, Any]]:
    """Return every registered middleware with its scopes."""
    return queue.list_middleware()


def handle_get_task_middleware(queue: Queue, _qs: dict, task_name: str) -> dict[str, Any]:
    """Return the middleware chain that fires for ``task_name`` with each
    entry's enabled/disabled state."""
    chain = queue._get_middleware_chain(task_name)
    disabled = set(queue.get_disabled_middleware_for(task_name))
    # Build the full would-fire chain INCLUDING disabled entries so the UI
    # can render every toggle.
    base_chain = queue._global_middleware + queue._task_middleware.get(task_name, [])
    entries: list[dict[str, Any]] = []
    chain_names = {middleware_key(mw) for mw in chain}
    for mw in base_chain:
        name = middleware_key(mw)
        entries.append(
            {
                "name": name,
                "class_path": middleware_class_path(mw),
                "disabled": name in disabled,
                "effective": name in chain_names,
            }
        )
    return {"task": task_name, "middleware": entries}


def handle_put_task_middleware(queue: Queue, body: dict, ids: tuple[str, str]) -> dict[str, Any]:
    task_name, mw_name = ids
    if not isinstance(body, dict) or "enabled" not in body:
        raise _BadRequest('body must include {"enabled": bool}')
    if not isinstance(body["enabled"], bool):
        raise _BadRequest("'enabled' must be a boolean")
    # Confirm the middleware exists in the relevant chain so a typo doesn't
    # silently write a no-op disable entry.
    base_chain = queue._global_middleware + queue._task_middleware.get(task_name, [])
    names = {getattr(mw, "name", "") for mw in base_chain}
    if mw_name not in names:
        raise _NotFound(f"middleware '{mw_name}' is not registered on task '{task_name}'")
    if body["enabled"]:
        new = queue.enable_middleware_for_task(task_name, mw_name)
    else:
        new = queue.disable_middleware_for_task(task_name, mw_name)
    return {"task": task_name, "disabled": new}


def handle_delete_task_middleware(queue: Queue, task_name: str) -> dict[str, bool]:
    """Clear ALL disables for a task — every middleware fires again."""
    return {"cleared": queue.clear_middleware_disables(task_name)}
