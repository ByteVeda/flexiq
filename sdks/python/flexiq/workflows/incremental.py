"""Dirty-set computation for incremental workflow runs."""

from __future__ import annotations

import time


def compute_dirty_set(
    base_nodes: list[tuple[str, str, str | None]],
    new_node_names: list[str],
    successors: dict[str, list[str]],
    predecessors: dict[str, list[str]],
    cache_ttl: float | None = None,
    base_run_completed_at: int | None = None,
) -> tuple[set[str], dict[str, str]]:
    """Determine which nodes are dirty and which are cache hits.

    Args:
        base_nodes: ``[(node_name, status, result_hash)]`` from the base run.
        new_node_names: Names of nodes in the new run.
        successors: DAG successor map.
        predecessors: DAG predecessor map.
        cache_ttl: Optional TTL in seconds. If set and the base run is older
            than this, all nodes are considered dirty.
        base_run_completed_at: Timestamp (ms) when the base run completed.

    Returns:
        ``(dirty_nodes, cache_hit_nodes)`` where ``cache_hit_nodes`` maps
        node name to the result_hash to copy.
    """
    # Check TTL expiration.
    if cache_ttl is not None and base_run_completed_at is not None:
        age_seconds = (time.time() * 1000 - base_run_completed_at) / 1000
        if age_seconds > cache_ttl:
            return set(new_node_names), {}

    # Build lookup from base run.
    base_lookup: dict[str, tuple[str, str | None]] = {}
    for name, status, result_hash in base_nodes:
        base_lookup[name] = (status, result_hash)

    new_set = set(new_node_names)
    dirty: set[str] = set()
    cached: dict[str, str] = {}

    # First pass: mark nodes as dirty or cached based on base status.
    for name in new_node_names:
        base = base_lookup.get(name)
        if base is None:
            dirty.add(name)
            continue
        status, result_hash = base
        if status == "completed" and result_hash is not None:
            cached[name] = result_hash
        else:
            dirty.add(name)

    # Second pass: propagate dirty downstream.
    changed = True
    while changed:
        changed = False
        for name in list(cached.keys()):
            preds = predecessors.get(name, [])
            if any(p in dirty for p in preds if p in new_set):
                dirty.add(name)
                del cached[name]
                changed = True

    return dirty, cached
