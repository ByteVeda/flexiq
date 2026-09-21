"""Running against somebody else's Redis without leaving anything behind.

The harness does not assume a disposable server. The instance this was first
published from is shared, already holds keys from the project's contract
suites, and exposes a single logical database — ``SELECT 3`` and ``SELECT 0``
are the same keyspace there, so there is no cheap partition to hide in and
``FLUSHDB`` is out of the question.

Two mechanisms, and the second is what makes the first safe:

* **Isolation by name.** Every run gets an id, and every queue and task name
  carries it. Workers poll only their own queue, so the pending rows a previous
  contract run left on ``default`` are invisible to the measurement.
* **Cleanup by diff.** The keyspace is snapshotted before an adapter runs and
  again after. Whatever is new, and recognisably a queue's, is unlinked. That
  is layout-independent: it stays correct when FlexiQ, BullMQ or RQ reshape
  their keys, which a hand-written list of key patterns would not.

Membership left behind in pre-existing container keys is the one thing a
name diff cannot see, so the job ids are removed from them explicitly, and
``DBSIZE`` returning to its baseline is asserted rather than assumed.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import cast

import redis

#: Key prefixes the harness is allowed to delete. A new key outside these is
#: somebody else's concurrent work: it is reported, never unlinked.
ENGINE_PREFIXES = ("flexiq:", "rq:", "bull:", "dramatiq:", "celery", "_kombu", "unacked")

#: Pre-existing FlexiQ containers that accumulate job ids regardless of queue.
#: A key-name diff cannot see a membership, so these are cleaned by value.
FLEXIQ_CONTAINERS = (
    "flexiq:jobs:all",
    "flexiq:jobs:cancel_requested",
    "flexiq:archived:all",
    "flexiq:archived:expiry",
    "flexiq:metrics:all",
    "flexiq:logs:all",
    "flexiq:exec_claims:by_time",
    *(f"flexiq:jobs:status:{n}" for n in range(8)),
    *(f"flexiq:archived:status:{n}" for n in range(8)),
)

_BATCH = 200


@dataclass(frozen=True)
class RunNames:
    """Queue and task names nobody else on this server is using."""

    run_id: str

    def queue(self, adapter: str) -> str:
        return f"bench_{adapter}_{self.run_id}"

    def task(self, adapter: str) -> str:
        return f"bench_task_{adapter}_{self.run_id}"


class RedisScope:
    """A keyspace snapshot around one adapter's run, and the cleanup that follows."""

    def __init__(self, url: str, run_id: str) -> None:
        self._client = redis.Redis.from_url(url, decode_responses=True)
        self.run_id = run_id

    def close(self) -> None:
        self._client.close()

    def dbsize(self) -> int:
        # redis-py types every command as sync-or-async; this client is sync.
        return int(cast(int, self._client.dbsize()))

    def snapshot(self) -> set[str]:
        """Every key name on the server. One sweep, batched so the RTT is paid once per 500."""
        return set(self._client.scan_iter(count=500))

    def _deletable(self, keys: Iterable[str]) -> tuple[list[str], list[str]]:
        """Split new keys into this run's and somebody else's.

        A key carrying the run id is ours by construction — Celery names its
        queue list after the queue and nothing else, so the engine prefixes
        alone would leave it behind. Anything matching neither rule appeared
        during the run without being ours, which is concurrent work: it is
        reported, never unlinked.
        """
        ours: list[str] = []
        theirs: list[str] = []
        for key in keys:
            mine = key.startswith(ENGINE_PREFIXES) or self.run_id in key
            (ours if mine else theirs).append(key)
        return ours, theirs

    def _drop_members(self, job_ids: Sequence[str]) -> None:
        """Remove our ids from containers that existed before the run.

        Type-dispatched rather than blind, because the same id can sit in a set
        on one release and a sorted set on the next, and a ``WRONGTYPE`` here
        would abort a cleanup half-done.
        """
        if not job_ids:
            return
        for key in FLEXIQ_CONTAINERS:
            kind = self._client.type(key)
            if kind == "none":
                continue
            pipe = self._client.pipeline(transaction=False)
            for start in range(0, len(job_ids), _BATCH):
                chunk = job_ids[start : start + _BATCH]
                if kind == "zset":
                    pipe.zrem(key, *chunk)
                elif kind == "set":
                    pipe.srem(key, *chunk)
                elif kind == "list":
                    for job_id in chunk:
                        pipe.lrem(key, 0, job_id)
                elif kind == "hash":
                    pipe.hdel(key, *chunk)
            pipe.execute()

    def cleanup(self, before: set[str], job_ids: Sequence[str]) -> dict[str, object]:
        """Unlink what this run added; report what it will not touch."""
        self._drop_members(job_ids)
        new_keys = self.snapshot() - before
        ours, theirs = self._deletable(new_keys)
        ours_list = sorted(ours)
        for start in range(0, len(ours_list), _BATCH):
            self._client.unlink(*ours_list[start : start + _BATCH])
        return {"deleted": len(ours_list), "left_alone": sorted(theirs)}

    def verify(self, baseline: int) -> dict[str, object]:
        """The postcondition: nothing of this run is left, and the server did not grow.

        Not ``==``: an engine's own maintenance can retire rows a previous run
        left behind, and removing the last member of a container removes the
        container with it. Under is fine and is reported; over means the
        harness left litter on somebody else's server, which is not.
        """
        leftovers = sorted(self._client.scan_iter(match=f"*{self.run_id}*", count=500))
        size = self.dbsize()
        return {
            "baseline_keys": baseline,
            "final_keys": size,
            "delta": size - baseline,
            "clean": size <= baseline and not leftovers,
            "leftovers": leftovers[:20],
        }
