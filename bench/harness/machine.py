"""What the numbers were produced on.

A throughput figure without the machine and the backend under it is a rumour.
Everything here ends up verbatim in the results artifact — except the Redis
credential, which never leaves the environment: the artifact records the shape
of the connection (remote or local, provider, region, version, measured
round-trip) and not the endpoint that would let a reader into it.
"""

from __future__ import annotations

import os
import platform
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlparse

import psutil
import redis

#: Hostname fragment → provider label. A private endpoint is never recorded;
#: the provider and the region are what a reader needs to judge the RTT.
_PROVIDERS = (
    ("cloud.redislabs.com", "Redis Cloud"),
    ("redns.redis-cloud.com", "Redis Cloud"),
    ("upstash.io", "Upstash"),
    ("cache.amazonaws.com", "AWS ElastiCache"),
)
_REGION = re.compile(r"\b([a-z]{2}-[a-z]+-\d)\b")
_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "", None})


def _cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as cpuinfo:
            for line in cpuinfo:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine()


def _node_version() -> str | None:
    try:
        done = subprocess.run(["node", "--version"], capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    return done.stdout.strip() or None


def describe_machine() -> dict[str, object]:
    """The host, and how loaded it was when the run started.

    The load average is here because a benchmark run on a developer's working
    machine is the normal case for this harness, and a reader deserves to see
    that rather than guess at it.
    """
    load1, load5, load15 = os.getloadavg()
    return {
        "os": f"{platform.system()} {platform.release()}",
        "arch": platform.machine(),
        "cpu": _cpu_model(),
        "cores": os.cpu_count(),
        "ram_gb": round(psutil.virtual_memory().total / 1024**3, 1),
        "python": platform.python_version(),
        "node": _node_version(),
        "load_avg_at_start": [round(load1, 2), round(load5, 2), round(load15, 2)],
    }


@dataclass(frozen=True)
class RedisTarget:
    """A reachable Redis, described without naming it."""

    url: str
    kind: str
    provider: str
    region: str | None
    version: str
    rtt_ms: dict[str, float]
    maxmemory_policy: str

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "provider": self.provider,
            "region": self.region,
            "version": self.version,
            "rtt_ms": self.rtt_ms,
            "maxmemory_policy": self.maxmemory_policy,
            "shared": self.kind == "remote",
        }


def probe_redis(url: str, samples: int = 30) -> RedisTarget:
    """Connect, identify and time the link before anything is measured over it.

    The round-trip is the floor under every Redis-backed runtime in the run —
    it belongs in the artifact next to the results, not in a footnote.
    """
    host = (urlparse(url).hostname or "").lower()
    kind = "local" if host in _LOCAL_HOSTS else "remote"
    provider = "self-hosted"
    for fragment, label in _PROVIDERS:
        if host.endswith(fragment):
            provider = label
            break
    region_match = _REGION.search(host)

    client = redis.Redis.from_url(url)
    # redis-py types every command as sync-or-async; this client is sync.
    server = cast(dict[str, Any], client.info("server"))
    version = str(server.get("redis_version", "unknown"))
    # BullMQ warns about this one loudly and it applies to every entrant: an
    # eviction policy that is not `noeviction` can drop a queued job under
    # memory pressure, which is a correctness property, not a speed one.
    memory = cast(dict[str, Any], client.info("memory"))
    policy = str(memory.get("maxmemory_policy", "unknown"))

    timings: list[float] = []
    for _ in range(samples):
        started = time.perf_counter()
        client.ping()
        timings.append((time.perf_counter() - started) * 1000)
    client.close()

    return RedisTarget(
        url=url,
        kind=kind,
        provider=provider,
        region=region_match.group(1) if region_match else None,
        version=version,
        rtt_ms={
            "min": round(min(timings), 2),
            "avg": round(sum(timings) / len(timings), 2),
            "max": round(max(timings), 2),
        },
        maxmemory_policy=policy,
    )


def redis_url_from_env() -> str | None:
    """``REDIS_URL`` or ``BENCH_REDIS_URL`` — never a literal in the repository."""
    return os.environ.get("BENCH_REDIS_URL") or os.environ.get("REDIS_URL")


def python_executable() -> str:
    """The interpreter the worker subprocesses must be spawned with."""
    return sys.executable
