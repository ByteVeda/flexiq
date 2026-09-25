"""Event sinks: job lifecycle events sent out as CloudEvents by the worker.

The configuration is one JSON document, the same across the cross-SDK
contract. It is parsed and validated by the native core; this module only
turns the forms ``Queue(event_sinks=...)`` accepts into that document.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

EventSinksConfig = dict[str, Any] | str | os.PathLike[str]
"""A configuration document: a dict, its JSON text, or a path to a JSON file."""


def event_sinks_document(config: EventSinksConfig | None) -> str | None:
    """Return ``config`` as JSON text, or ``None`` when no sinks are configured.

    A ``str`` is always the document itself, never a path: a path must be an
    :class:`os.PathLike`, so a mistyped document cannot be read as a file name.
    """
    if config is None:
        return None
    if isinstance(config, dict):
        return json.dumps(config)
    if isinstance(config, str):
        return config
    if isinstance(config, os.PathLike):
        return Path(config).read_text(encoding="utf-8")
    raise TypeError(
        f"event_sinks must be a dict, a JSON string or an os.PathLike, got {type(config).__name__}"
    )
