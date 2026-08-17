"""Dependency graph utilities — topological sort and cycle detection."""

from __future__ import annotations

from flexiq.exceptions import CircularDependencyError
from flexiq.resources.definition import ResourceDefinition


def topological_sort(defs: dict[str, ResourceDefinition]) -> list[str]:
    """Return resource names in dependency-first order (Kahn's algorithm).

    Raises:
        CircularDependencyError: If dependencies form a cycle.
    """
    # Build adjacency and in-degree maps
    in_degree: dict[str, int] = {name: 0 for name in defs}
    dependents: dict[str, list[str]] = {name: [] for name in defs}

    for name, defn in defs.items():
        for dep in defn.depends_on:
            if dep not in defs:
                raise KeyError(f"Resource '{name}' depends on '{dep}', which is not registered")
            dependents[dep].append(name)
            in_degree[name] += 1

    # Seed queue with zero-in-degree nodes (sorted for determinism)
    queue = sorted(name for name, deg in in_degree.items() if deg == 0)
    order: list[str] = []

    while queue:
        node = queue.pop(0)
        order.append(node)
        for dependent in sorted(dependents[node]):
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                queue.append(dependent)

    if len(order) != len(defs):
        cycle = detect_cycle(defs)
        raise CircularDependencyError(f"Circular dependency detected: {' -> '.join(cycle or [])}")

    return order


def detect_cycle(defs: dict[str, ResourceDefinition]) -> list[str] | None:
    """Return the cycle path if one exists, else None."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {name: WHITE for name in defs}
    path: list[str] = []

    def dfs(node: str) -> list[str] | None:
        color[node] = GRAY
        path.append(node)
        defn = defs.get(node)
        if defn is not None:
            for dep in defn.depends_on:
                if dep not in color:
                    continue
                if color[dep] == GRAY:
                    idx = path.index(dep)
                    return [*path[idx:], dep]
                if color[dep] == WHITE:
                    result = dfs(dep)
                    if result is not None:
                        return result
        path.pop()
        color[node] = BLACK
        return None

    for name in defs:
        if color[name] == WHITE:
            result = dfs(name)
            if result is not None:
                return result

    return None
