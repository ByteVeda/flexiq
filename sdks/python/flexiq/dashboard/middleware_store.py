"""Per-task middleware disable list.

Operators turn individual middlewares off for individual tasks from the
dashboard. The disable list is persisted under
``middleware:disabled:<task_name>`` as a JSON array of middleware names,
read by :meth:`~flexiq.mixins.decorators.QueueDecoratorMixin._get_middleware_chain`
at every task invocation so changes take effect immediately on the next
job without a worker restart.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from flexiq.dashboard.kv import update

if TYPE_CHECKING:
    from flexiq.app import Queue


DISABLE_PREFIX = "middleware:disabled:"

logger = logging.getLogger("flexiq.dashboard.middleware")


def _parse(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning("middleware disable list is not valid JSON; treating as empty")
        return []
    if not isinstance(data, list):
        return []
    return [str(x) for x in data if isinstance(x, str)]


class MiddlewareDisableStore:
    """List/set/clear per-task middleware disables."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    def _key(self, task_name: str) -> str:
        return DISABLE_PREFIX + task_name

    def list_all(self) -> dict[str, list[str]]:
        """Return ``{task_name: [disabled_mw_name, ...]}`` for every task that
        has at least one disabled middleware."""
        out: dict[str, list[str]] = {}
        for key, raw in self._queue.list_settings().items():
            if not key.startswith(DISABLE_PREFIX):
                continue
            task_name = key[len(DISABLE_PREFIX) :]
            names = _parse(raw)
            if names:
                out[task_name] = names
        return out

    def get_for(self, task_name: str) -> list[str]:
        return _parse(self._queue.get_setting(self._key(task_name)))

    def is_disabled(self, task_name: str, mw_name: str) -> bool:
        return mw_name in self.get_for(task_name)

    def set_disabled(self, task_name: str, mw_name: str, disabled: bool) -> list[str]:
        """Flip a middleware on/off for a task and return the new disable list.

        An emptied list leaves a ``[]`` row rather than deleting it. Deleting sat
        outside the compare-and-set, so a concurrent writer's entry could be
        added between the swap and the delete and then removed by it — the very
        lost update the compare-and-set exists to prevent. Nothing reads the
        difference: :meth:`get_for` parses ``[]`` as "nothing disabled",
        :meth:`list_all` filters empty lists out, and the key sits under a
        reserved prefix, so the generic settings view does not show it either.
        """
        if not task_name:
            raise ValueError("task_name must not be empty")
        if not mw_name:
            raise ValueError("mw_name must not be empty")

        def toggle(names: list[str]) -> list[str]:
            if disabled:
                if mw_name not in names:
                    names.append(mw_name)
            else:
                names[:] = [name for name in names if name != mw_name]
            return list(names)

        return update(self._queue, self._key(task_name), _parse, toggle)

    def clear_for(self, task_name: str) -> bool:
        return self._queue.delete_setting(self._key(task_name))
