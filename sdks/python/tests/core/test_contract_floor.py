"""The storage carries the lowest contract level a process may speak.

A build below that floor must refuse to open rather than join a deployment and
misread rows its contract never described.
"""

from __future__ import annotations

from typing import Any

import pytest

from taskito import Queue
from taskito._taskito import reserved_setting_prefixes

CONTRACT_FLOOR_SETTING = "contract:min_sdk"


def test_an_unraised_floor_is_the_permissive_default(queue: Queue) -> None:
    # Opening never writes, so a deployment that leaves the dial alone carries
    # no row for it.
    assert queue.get_setting(CONTRACT_FLOOR_SETTING) is None
    assert queue.min_contract() >= 1


def test_a_floor_at_this_build_still_opens(tmp_path: Any) -> None:
    db_path = str(tmp_path / "q.db")
    queue = Queue(db_path=db_path)
    queue.set_min_contract(queue.min_contract())

    assert Queue(db_path=db_path).min_contract() == queue.min_contract()


def test_a_build_below_the_floor_refuses_to_open(tmp_path: Any) -> None:
    db_path = str(tmp_path / "q.db")
    queue = Queue(db_path=db_path)
    unreachable = queue.min_contract() + 1
    # Written through the raw setting: `set_min_contract` rejects a level this
    # build cannot speak, which is the guard the next assertion exercises.
    queue.set_setting(CONTRACT_FLOOR_SETTING, str(unreachable))

    with pytest.raises(RuntimeError) as excinfo:
        Queue(db_path=db_path)

    message = str(excinfo.value)
    assert str(unreachable) in message, message
    assert "contract" in message.lower(), message


def test_a_floor_this_build_cannot_speak_is_rejected(queue: Queue) -> None:
    before = queue.min_contract()

    with pytest.raises(ValueError, match="lock it out"):
        queue.set_min_contract(before + 1)

    assert queue.min_contract() == before


def test_the_floor_is_hidden_from_the_generic_settings_api() -> None:
    # Reserved prefixes keep the dial off the dashboard's key/value surface, so
    # nothing can spoof the level a process is checked against.
    assert any(CONTRACT_FLOOR_SETTING.startswith(prefix) for prefix in reserved_setting_prefixes())
