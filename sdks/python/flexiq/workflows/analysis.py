"""Pre-execution graph analysis for workflows.

All functions operate on the builder's ``_steps`` dict (a mapping from
step name to :class:`~flexiq.workflows.builder._Step`). They perform
pure graph computations with no side effects.
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .builder import _Step


def _build_adjacency(
    steps: dict[str, _Step],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Build forward (successors) and backward (predecessors) adjacency maps."""
    successors: dict[str, list[str]] = {name: [] for name in steps}
    predecessors: dict[str, list[str]] = {name: list(s.after) for name, s in steps.items()}
    for name, step in steps.items():
        for pred in step.after:
            successors[pred].append(name)
    return successors, predecessors


def ancestors(steps: dict[str, _Step], node: str) -> list[str]:
    """Return all transitive predecessors of *node* (BFS, no particular order)."""
    if node not in steps:
        raise KeyError(f"node '{node}' not found")
    _, preds = _build_adjacency(steps)
    visited: set[str] = set()
    queue: deque[str] = deque(preds.get(node, []))
    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)
        queue.extend(preds.get(current, []))
    return sorted(visited)


def descendants(steps: dict[str, _Step], node: str) -> list[str]:
    """Return all transitive successors of *node* (BFS, no particular order)."""
    if node not in steps:
        raise KeyError(f"node '{node}' not found")
    succs, _ = _build_adjacency(steps)
    visited: set[str] = set()
    queue: deque[str] = deque(succs.get(node, []))
    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)
        queue.extend(succs.get(current, []))
    return sorted(visited)


def topological_levels(steps: dict[str, _Step]) -> list[list[str]]:
    """Group nodes by topological depth (Kahn's algorithm).

    Returns a list of lists where level 0 contains root nodes,
    level 1 contains nodes whose predecessors are all in level 0, etc.
    """
    _, preds = _build_adjacency(steps)
    succs, _ = _build_adjacency(steps)
    in_degree: dict[str, int] = {name: len(p) for name, p in preds.items()}

    levels: list[list[str]] = []
    current_level = sorted(n for n, d in in_degree.items() if d == 0)

    while current_level:
        levels.append(current_level)
        next_level_set: set[str] = set()
        for node in current_level:
            for succ in succs.get(node, []):
                in_degree[succ] -= 1
                if in_degree[succ] == 0:
                    next_level_set.add(succ)
        current_level = sorted(next_level_set)

    return levels


def stats(steps: dict[str, _Step]) -> dict[str, int | float]:
    """Compute basic DAG statistics.

    Returns:
        ``{nodes, edges, depth, width, density}``
    """
    n_nodes = len(steps)
    n_edges = sum(len(s.after) for s in steps.values())
    levels = topological_levels(steps)
    depth = len(levels)
    width = max((len(lv) for lv in levels), default=0)
    max_edges = n_nodes * (n_nodes - 1) / 2 if n_nodes > 1 else 1
    density = round(n_edges / max_edges, 4) if max_edges > 0 else 0.0
    return {
        "nodes": n_nodes,
        "edges": n_edges,
        "depth": depth,
        "width": width,
        "density": density,
    }


def critical_path(steps: dict[str, _Step], costs: dict[str, float]) -> tuple[list[str], float]:
    """Find the longest-weighted path through the DAG.

    Uses dynamic programming on topological order.

    Args:
        steps: Mapping of step name → ``_Step`` carrying its predecessors.
        costs: Mapping of step name → estimated duration.

    Returns:
        ``(path, total_cost)`` — the critical path nodes and sum of costs.
    """
    levels = topological_levels(steps)
    flat_order = [node for level in levels for node in level]

    dist: dict[str, float] = {}
    prev: dict[str, str | None] = {}
    for node in flat_order:
        dist[node] = costs.get(node, 0.0)
        prev[node] = None

    succs, _ = _build_adjacency(steps)
    for node in flat_order:
        for succ in succs.get(node, []):
            new_dist = dist[node] + costs.get(succ, 0.0)
            if new_dist > dist[succ]:
                dist[succ] = new_dist
                prev[succ] = node

    if not dist:
        return [], 0.0

    end_node = max(dist, key=lambda n: dist[n])
    path: list[str] = []
    current: str | None = end_node
    while current is not None:
        path.append(current)
        current = prev[current]
    path.reverse()
    return path, dist[end_node]


def execution_plan(steps: dict[str, _Step], max_workers: int = 1) -> list[list[str]]:
    """Generate a step-by-step execution plan respecting worker limits.

    Each stage contains up to *max_workers* nodes that can run concurrently.
    Nodes within the same topological level are batched together.
    """
    levels = topological_levels(steps)
    plan: list[list[str]] = []
    for level in levels:
        for i in range(0, len(level), max_workers):
            plan.append(level[i : i + max_workers])
    return plan


def bottleneck_analysis(steps: dict[str, _Step], costs: dict[str, float]) -> dict[str, Any]:
    """Identify the bottleneck node on the critical path.

    Returns:
        ``{node, cost, percentage, critical_path, total_cost, suggestion}``
    """
    path, total = critical_path(steps, costs)
    if not path or total == 0:
        return {"node": None, "cost": 0, "percentage": 0, "critical_path": [], "total_cost": 0}

    bottleneck_node = max(path, key=lambda n: costs.get(n, 0.0))
    bottleneck_cost = costs.get(bottleneck_node, 0.0)
    pct = round(bottleneck_cost / total * 100, 1) if total > 0 else 0

    return {
        "node": bottleneck_node,
        "cost": bottleneck_cost,
        "percentage": pct,
        "critical_path": path,
        "total_cost": total,
        "suggestion": (
            f"{bottleneck_node} is the bottleneck "
            f"({pct}% of total time). "
            f"Consider increasing max_concurrent or optimizing this step."
        ),
    }
