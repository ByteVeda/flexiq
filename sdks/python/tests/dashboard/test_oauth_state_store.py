"""Tests for the short-lived OAuth state store."""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from flexiq import Queue
from flexiq.dashboard.oauth.state_store import (
    DEFAULT_STATE_TTL_SECONDS,
    STATE_PREFIX,
    OAuthStateStore,
)


@pytest.fixture
def queue(tmp_path: Path) -> Queue:
    return Queue(db_path=str(tmp_path / "oauth_state.db"))


def test_create_persists_row_and_returns_state(queue: Queue) -> None:
    store = OAuthStateStore(queue)
    row = store.create(slot="google", next_url="/dashboard")

    assert row.slot == "google"
    assert row.next_url == "/dashboard"
    assert len(row.state) >= 32
    assert len(row.nonce) >= 16
    assert len(row.code_verifier) >= 32
    # state, nonce, and verifier must each be unique tokens.
    assert row.state != row.nonce != row.code_verifier
    # Row is in the settings store under the expected prefix.
    assert queue.get_setting(STATE_PREFIX + row.state) is not None


def test_consume_returns_row_then_invalidates_it(queue: Queue) -> None:
    store = OAuthStateStore(queue)
    row = store.create(slot="github", next_url="/")

    first = store.consume(row.state)
    assert first is not None
    assert first.slot == "github"
    assert first.code_verifier == row.code_verifier

    # Second consume is a replay attempt — must fail.
    assert store.consume(row.state) is None


def test_consume_rejects_empty_and_unknown_tokens(queue: Queue) -> None:
    store = OAuthStateStore(queue)
    assert store.consume("") is None
    assert store.consume("never-issued") is None


def test_consume_expired_row_returns_none(queue: Queue) -> None:
    store = OAuthStateStore(queue)
    row = store.create(slot="google", next_url="/", ttl_seconds=0)
    # Even at TTL=0 we deliberately treat the row as immediately expired
    # — but it's still single-use (deleted on consume).
    assert store.consume(row.state) is None
    # Underlying entry is gone after the consume.
    assert queue.get_setting(STATE_PREFIX + row.state) is None


def test_consume_strips_malformed_rows(queue: Queue) -> None:
    store = OAuthStateStore(queue)
    # Inject a garbage row directly into the settings store.
    queue.set_setting(STATE_PREFIX + "broken", "not-json-{}")
    assert store.consume("broken") is None
    assert queue.get_setting(STATE_PREFIX + "broken") is None


def test_prune_expired_removes_only_old_rows(queue: Queue) -> None:
    store = OAuthStateStore(queue)
    fresh = store.create(slot="google", next_url="/")
    stale = store.create(slot="github", next_url="/", ttl_seconds=0)

    # Simulate the prune sweep.
    removed = store.prune_expired()
    assert removed >= 1
    # Fresh row survives.
    assert queue.get_setting(STATE_PREFIX + fresh.state) is not None
    # Stale row is gone.
    assert queue.get_setting(STATE_PREFIX + stale.state) is None


def test_default_ttl_is_five_minutes() -> None:
    assert DEFAULT_STATE_TTL_SECONDS == 300


def test_create_sets_expected_expiry(queue: Queue) -> None:
    store = OAuthStateStore(queue)
    before = int(time.time())
    row = store.create(slot="google", next_url="/", ttl_seconds=120)
    after = int(time.time())
    assert before + 120 <= row.expires_at <= after + 120
