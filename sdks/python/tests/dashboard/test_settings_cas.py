"""Concurrent edits to the settings-backed feature stores must not be lost.

Every store keeps a whole JSON document under one settings key. A read-then-write
drops a concurrent edit wholesale, and more than one dashboard replica against
one backend is a supported deployment — so each store writes conditionally on
the value it read and retries when it loses the race.

The races here are deterministic: :class:`_RacingQueue` runs a supplied writer
immediately after a read, which is exactly the window a read-then-write loses.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any, cast

import pytest

from flexiq import Queue
from flexiq.dashboard.auth import AuthStore
from flexiq.dashboard.kv import MAX_ATTEMPTS, SettingConflictError, update
from flexiq.dashboard.middleware_store import MiddlewareDisableStore
from flexiq.dashboard.overrides_store import OverridesStore
from flexiq.dashboard.webhook_store import SUBSCRIPTIONS_KEY, WebhookSubscriptionStore


class _RacingQueue:
    """A queue that lets another writer in right after each settings read.

    Every read pops one callable off ``interlopers`` and runs it, simulating a
    second dashboard replica writing between this caller's read and its write.
    """

    def __init__(self, queue: Queue, interlopers: list[Callable[[], Any]]) -> None:
        self._queue = queue
        self._interlopers = interlopers
        self.reads = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._queue, name)

    def get_setting(self, key: str) -> str | None:
        value = self._queue.get_setting(key)
        self.reads += 1
        if self._interlopers:
            self._interlopers.pop(0)()
        return value


def _racing(queue: Queue, *interlopers: Callable[[], Any]) -> Queue:
    return cast(Queue, _RacingQueue(queue, list(interlopers)))


# ── The storage primitive ───────────────────────────────────────────────


def test_a_stale_expectation_loses(queue: Queue) -> None:
    queue.set_setting("k", "v1")

    assert not queue.set_setting_if("k", "stale", "v2")
    assert queue.get_setting("k") == "v1"

    assert queue.set_setting_if("k", "v1", "v2")
    assert queue.get_setting("k") == "v2"


def test_expecting_unset_inserts_exactly_once(queue: Queue) -> None:
    assert queue.set_setting_if("k", None, "first")
    assert not queue.set_setting_if("k", None, "second")
    assert queue.get_setting("k") == "first"


def test_expecting_a_value_on_a_missing_key_does_not_insert(queue: Queue) -> None:
    assert not queue.set_setting_if("missing", "anything", "v")
    assert queue.get_setting("missing") is None


# ── The retry helper ────────────────────────────────────────────────────


def _load_list(raw: str | None) -> list[Any]:
    return list(json.loads(raw)) if raw else []


def test_a_no_op_mutation_on_a_missing_key_writes_nothing(queue: Queue) -> None:
    # The skip compares the new encoding against the *document as loaded*, not
    # the raw stored string: on a missing key the raw is None while the encoding
    # is `[]`, so comparing to the raw would write a row for "changed nothing".
    def drop_absent(names: list[Any]) -> bool:
        before = len(names)
        names[:] = [name for name in names if name != "absent"]
        return len(names) != before

    assert not update(queue, "missing", _load_list, drop_absent)
    assert queue.get_setting("missing") is None


def test_update_retries_until_it_wins(queue: Queue) -> None:
    racing = _RacingQueue(queue, [lambda: queue.set_setting("k", '["interloper"]')])

    update(racing, "k", _load_list, lambda names: names.append("mine"))

    assert racing.reads == 2, "the first attempt must lose and re-read"
    assert queue.get_setting("k") == '["interloper","mine"]'


def test_update_gives_up_after_max_attempts(queue: Queue) -> None:
    # A *different* value on every read, so no attempt can ever win.
    tick = iter(range(MAX_ATTEMPTS + 5))
    interlope: Callable[[], None] = lambda: queue.set_setting("k", f"[{next(tick)}]")  # noqa: E731
    racing = _RacingQueue(queue, [interlope] * (MAX_ATTEMPTS + 5))

    with pytest.raises(SettingConflictError) as raised:
        update(racing, "k", _load_list, lambda names: names.append("mine"))

    assert raised.value.key == "k"
    assert racing.reads == MAX_ATTEMPTS


# ── The stores ──────────────────────────────────────────────────────────


def test_concurrent_user_creation_keeps_both(queue: Queue) -> None:
    quiet = AuthStore(queue)
    racing = AuthStore(_racing(queue, lambda: quiet.create_user("first", "password123")))

    racing.create_user("second", "password123")

    assert {user.username for user in quiet.list_users()} == {"first", "second"}


def test_a_user_deleted_mid_authenticate_is_not_resurrected(queue: Queue) -> None:
    quiet = AuthStore(queue)
    quiet.create_user("alice", "password123")
    racing = AuthStore(_racing(queue, lambda: quiet.delete_user("alice")))

    # The read that fed the password check saw the row, so the login stands —
    # but stamping last_login_at must not write the whole document back and
    # bring the deleted account with it.
    assert racing.authenticate("alice", "password123") is not None
    assert quiet.get_user("alice") is None


def test_concurrent_webhook_creation_keeps_both(queue: Queue) -> None:
    quiet = WebhookSubscriptionStore(queue)
    racing = WebhookSubscriptionStore(
        _racing(queue, lambda: quiet.create(url="https://example.test/first"))
    )

    racing.create(url="https://example.test/second")

    assert {sub.url for sub in quiet.list_all()} == {
        "https://example.test/first",
        "https://example.test/second",
    }


def test_deleting_an_unknown_webhook_writes_nothing(queue: Queue) -> None:
    store = WebhookSubscriptionStore(queue)

    assert not store.delete("nope")
    assert queue.get_setting(SUBSCRIPTIONS_KEY) is None


def test_concurrent_override_edits_both_survive(queue: Queue) -> None:
    quiet = OverridesStore(queue)
    racing = OverridesStore(
        _racing(queue, lambda: quiet.set_task("send_email", {"max_retries": 7}))
    )

    racing.set_task("send_email", {"timeout": 30})

    merged = quiet.get_task("send_email")
    assert merged is not None
    assert (merged.max_retries, merged.timeout) == (7, 30)


def test_emptying_a_disable_list_leaves_a_row_not_a_delete(queue: Queue) -> None:
    # Deleting sat outside the compare-and-set, so a concurrent writer's entry
    # could be added between the swap and the delete and then removed by it.
    store = MiddlewareDisableStore(queue)
    store.set_disabled("send_email", "RetryLogger", True)

    assert store.set_disabled("send_email", "RetryLogger", False) == []
    assert queue.get_setting("middleware:disabled:send_email") == "[]"
    assert store.get_for("send_email") == []
    assert store.list_all() == {}


def test_concurrent_middleware_toggles_both_survive(queue: Queue) -> None:
    quiet = MiddlewareDisableStore(queue)
    racing = MiddlewareDisableStore(
        _racing(queue, lambda: quiet.set_disabled("send_email", "Tracing", True))
    )

    assert sorted(racing.set_disabled("send_email", "Metrics", True)) == ["Metrics", "Tracing"]
