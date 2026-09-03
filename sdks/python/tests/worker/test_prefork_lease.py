"""A prefork child echoes the lease its dispatch arrived under.

The parent uses it to tell this child's answer from a sibling's after a job has
been re-dispatched — an operator requeuing a job a child is wedged on is exactly
the case that produces two live copies of one id. The stamping is one function
applied to every outgoing frame, so that is what these test.
"""

from __future__ import annotations

import pytest

from flexiq._flexiq import CAP_LEASE
from flexiq.prefork import child as child_mod


@pytest.fixture(autouse=True)
def _clean_leases() -> None:
    """The lease map is module state; no test may inherit another's."""
    child_mod._leases.clear()


def test_a_frame_naming_a_job_carries_its_lease() -> None:
    child_mod._remember_lease("job-1", "opaque-lease")

    stamped = child_mod._stamp_lease({"type": "progress", "job_id": "job-1", "progress": 42})

    assert stamped["lease"] == "opaque-lease"


def test_a_frame_naming_no_job_is_left_alone() -> None:
    child_mod._remember_lease("job-1", "opaque-lease")

    # `hello` is the connection's own frame, not any job's.
    assert "lease" not in child_mod._stamp_lease({"type": "hello", "sdk": "python"})


def test_a_dispatch_that_carried_no_lease_is_answered_without_one() -> None:
    # The give-up a parent without a lease book accepts: nothing is invented
    # here, so the frame goes back exactly as it always did.
    child_mod._remember_lease("job-1", None)

    assert "lease" not in child_mod._stamp_lease(
        {"type": "success", "job_id": "job-1", "result_len": None}
    )


@pytest.mark.parametrize("terminal", ["success", "failure", "cancelled", "slept"])
def test_a_terminal_frame_releases_the_lease(terminal: str) -> None:
    # Released rather than left behind: the map would otherwise grow for the
    # life of the child, one entry per job it ever ran.
    child_mod._remember_lease("job-1", "opaque-lease")

    assert child_mod._stamp_lease({"type": terminal, "job_id": "job-1"})["lease"] == (
        "opaque-lease"
    )
    assert child_mod._leases == {}


def test_a_non_terminal_frame_keeps_the_lease_for_the_next_one() -> None:
    # A task reports progress many times over one attempt, and every one of
    # those frames has to carry the lease.
    child_mod._remember_lease("job-1", "opaque-lease")

    for _ in range(3):
        frame = child_mod._stamp_lease({"type": "progress", "job_id": "job-1", "progress": 1})
        assert frame["lease"] == "opaque-lease"


def test_the_child_claims_the_lease_capability() -> None:
    # Announced unconditionally, because echoing a lease is entirely this
    # module's work — a shell opt-in would only be a way to forget.
    assert CAP_LEASE == "lease"
    assert CAP_LEASE in child_mod._CHILD_CAPABILITIES
