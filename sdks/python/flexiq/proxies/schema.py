"""Schema validation for proxy recipes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from flexiq.exceptions import ProxyReconstructionError


@dataclass
class FieldSpec:
    """Describes a single field in a proxy recipe schema."""

    type: type | tuple[type, ...]
    required: bool = True


def validate_recipe(
    handler_name: str,
    recipe: dict[str, Any],
    schema: dict[str, FieldSpec],
) -> None:
    """Validate a recipe against a schema.

    Raises ProxyReconstructionError on extra keys, missing required keys,
    or wrong types.
    """
    allowed_keys = set(schema.keys())
    actual_keys = set(recipe.keys())

    extra = actual_keys - allowed_keys
    if extra:
        raise ProxyReconstructionError(
            f"Recipe for '{handler_name}' has unexpected keys: {sorted(extra)}"
        )

    for key, spec in schema.items():
        if key not in recipe:
            if spec.required:
                raise ProxyReconstructionError(
                    f"Recipe for '{handler_name}' is missing required key: '{key}'"
                )
            continue
        value = recipe[key]
        if value is not None and not isinstance(value, spec.type):
            raise ProxyReconstructionError(
                f"Recipe for '{handler_name}': key '{key}' expected "
                f"{spec.type}, got {type(value).__name__}"
            )
