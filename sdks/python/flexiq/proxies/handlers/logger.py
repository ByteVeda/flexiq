"""LoggerHandler — proxy for logging.Logger instances."""

from __future__ import annotations

import logging
from typing import Any


class LoggerHandler:
    """Deconstructs/reconstructs Logger instances."""

    name = "logger"
    version = 1
    handled_types: tuple[type, ...] = (logging.Logger,)

    def detect(self, obj: Any) -> bool:
        return isinstance(obj, logging.Logger)

    def deconstruct(self, obj: Any) -> dict[str, Any]:
        return {
            "name": obj.name,
            "level": obj.level,
        }

    def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
        lgr = logging.getLogger(recipe["name"])
        if recipe.get("level"):
            lgr.setLevel(recipe["level"])
        return lgr

    def cleanup(self, obj: Any) -> None:
        pass  # loggers are singletons
