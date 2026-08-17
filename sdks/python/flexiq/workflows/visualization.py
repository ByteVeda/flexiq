"""Workflow DAG visualization in Mermaid and DOT formats."""

from __future__ import annotations

import json
from typing import Any

_STATUS_COLORS_MERMAID = {
    "completed": "#90EE90",
    "failed": "#FFB6C1",
    "running": "#87CEEB",
    "pending": "#D3D3D3",
    "skipped": "#F5F5F5",
    "waiting_approval": "#FFFACD",
    "ready": "#E0E0E0",
}

_STATUS_COLORS_DOT = {
    "completed": "lightgreen",
    "failed": "lightcoral",
    "running": "lightskyblue",
    "pending": "lightgray",
    "skipped": "whitesmoke",
    "waiting_approval": "lightyellow",
    "ready": "gainsboro",
}

_STATUS_SYMBOLS = {
    "completed": "\u2713",
    "failed": "\u2717",
    "running": "\u25b6",
    "pending": "\u25cb",
    "skipped": "\u2014",
    "waiting_approval": "\u23f8",
}


def render_mermaid(
    nodes: list[str],
    edges: list[tuple[str, str]],
    statuses: dict[str, str] | None = None,
) -> str:
    """Render a DAG as a Mermaid graph string.

    Args:
        nodes: List of node names.
        edges: List of ``(from, to)`` tuples.
        statuses: Optional mapping of node name to status string.

    Returns:
        A Mermaid ``graph LR`` diagram string.
    """
    lines = ["graph LR"]
    statuses = statuses or {}

    for name in nodes:
        status = statuses.get(name, "")
        symbol = _STATUS_SYMBOLS.get(status, "")
        label = f"{name} {symbol}".strip()
        lines.append(f"  {_safe_id(name)}[{label}]")

    for src, dst in edges:
        lines.append(f"  {_safe_id(src)} --> {_safe_id(dst)}")

    if statuses:
        for name in nodes:
            status = statuses.get(name, "")
            color = _STATUS_COLORS_MERMAID.get(status)
            if color:
                lines.append(f"  style {_safe_id(name)} fill:{color}")

    return "\n".join(lines)


def render_dot(
    nodes: list[str],
    edges: list[tuple[str, str]],
    statuses: dict[str, str] | None = None,
) -> str:
    """Render a DAG as a Graphviz DOT string.

    Args:
        nodes: List of node names.
        edges: List of ``(from, to)`` tuples.
        statuses: Optional mapping of node name to status string.

    Returns:
        A DOT ``digraph`` string.
    """
    lines = ["digraph workflow {", "  rankdir=LR;"]
    statuses = statuses or {}

    for name in nodes:
        status = statuses.get(name, "")
        symbol = _STATUS_SYMBOLS.get(status, "")
        label = f"{name} {symbol}".strip()
        color = _STATUS_COLORS_DOT.get(status, "white")
        lines.append(f'  {_safe_id(name)} [label="{label}" style=filled fillcolor={color}];')

    for src, dst in edges:
        lines.append(f"  {_safe_id(src)} -> {_safe_id(dst)};")

    lines.append("}")
    return "\n".join(lines)


def _safe_id(name: str) -> str:
    """Make a node name safe for use as a Mermaid/DOT identifier."""
    return name.replace("[", "_").replace("]", "_").replace(" ", "_")


def nodes_and_edges_from_steps(
    steps: dict[str, Any],
) -> tuple[list[str], list[tuple[str, str]]]:
    """Extract node list and edge list from builder ``_steps``."""
    nodes = list(steps.keys())
    edges: list[tuple[str, str]] = []
    for name, step in steps.items():
        for pred in step.after:
            edges.append((pred, name))
    return nodes, edges


def nodes_and_edges_from_dag_bytes(
    dag_bytes: bytes | list[int],
) -> tuple[list[str], list[tuple[str, str]]]:
    """Extract node list and edge list from serialized DAG JSON."""
    raw = bytes(dag_bytes) if isinstance(dag_bytes, list) else dag_bytes
    dag = json.loads(raw)
    nodes = [n["name"] for n in dag.get("nodes", [])]
    edges = [(e["from"], e["to"]) for e in dag.get("edges", [])]
    return nodes, edges
