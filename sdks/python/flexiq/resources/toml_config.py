"""Load resource definitions from TOML configuration files."""

from __future__ import annotations

import importlib
from collections.abc import Callable
from typing import Any

from flexiq.resources.definition import ResourceDefinition, ResourceScope


def load_resources_from_toml(path: str) -> list[ResourceDefinition]:
    """Parse a TOML file and return ResourceDefinition instances.

    Expected TOML structure::

        [resources.db]
        factory = "myapp.resources:create_db_pool"
        teardown = "myapp.resources:close_db_pool"
        scope = "worker"
        depends_on = ["config"]

    Raises:
        ValueError: On malformed config.
        ImportError: If factory/teardown modules cannot be imported.
    """
    toml_data = _load_toml(path)
    resources_section = toml_data.get("resources", {})
    if not isinstance(resources_section, dict):
        raise ValueError(f"Expected [resources] table in {path}")

    definitions: list[ResourceDefinition] = []
    for name, config in resources_section.items():
        if not isinstance(config, dict):
            raise ValueError(f"Expected table for resource '{name}' in {path}")
        definitions.append(_parse_resource(name, config))
    return definitions


def _load_toml(path: str) -> dict[str, Any]:
    """Load a TOML file using tomllib (3.11+) or tomli fallback."""
    try:
        import tomllib  # type: ignore[import-not-found]
    except ImportError:
        try:
            import tomli as tomllib  # type: ignore[import-not-found]
        except ImportError:
            raise ImportError(
                "Install tomli for Python < 3.11 to use TOML resource config"
            ) from None

    with open(path, "rb") as f:
        result: dict[str, Any] = tomllib.load(f)
        return result


def _resolve_callable(dotted_path: str) -> Callable[..., Any]:
    """Import 'myapp.resources:create_db_pool' or 'myapp.resources.create_db_pool'."""
    if ":" in dotted_path:
        module_path, _, attr_name = dotted_path.partition(":")
    else:
        module_path, _, attr_name = dotted_path.rpartition(".")

    if not module_path or not attr_name:
        raise ValueError(f"Invalid callable path: {dotted_path!r}")

    mod = importlib.import_module(module_path)
    obj = getattr(mod, attr_name, None)
    if obj is None:
        raise ImportError(f"Cannot find '{attr_name}' in module '{module_path}'")
    if not callable(obj):
        raise ValueError(f"'{dotted_path}' is not callable")
    fn: Callable[..., Any] = obj
    return fn


def _parse_resource(name: str, config: dict[str, Any]) -> ResourceDefinition:
    """Parse a single resource config dict into a ResourceDefinition."""
    factory_path = config.get("factory")
    if not factory_path:
        raise ValueError(f"Resource '{name}' is missing required 'factory' key")

    factory = _resolve_callable(factory_path)

    teardown = None
    if "teardown" in config:
        teardown = _resolve_callable(config["teardown"])

    health_check = None
    if "health_check" in config:
        health_check = _resolve_callable(config["health_check"])

    scope_str = config.get("scope", "worker")
    try:
        scope = ResourceScope(scope_str)
    except ValueError:
        raise ValueError(
            f"Resource '{name}' has invalid scope: {scope_str!r}. "
            f"Valid: {[s.value for s in ResourceScope]}"
        ) from None

    return ResourceDefinition(
        name=name,
        factory=factory,
        teardown=teardown,
        health_check=health_check,
        health_check_interval=config.get("health_check_interval", 0.0),
        max_recreation_attempts=config.get("max_recreation_attempts", 3),
        scope=scope,
        depends_on=config.get("depends_on", []),
        pool_size=config.get("pool_size"),
        pool_min=config.get("pool_min", 0),
        acquire_timeout=config.get("acquire_timeout", 10.0),
        max_lifetime=config.get("max_lifetime", 3600.0),
        idle_timeout=config.get("idle_timeout", 300.0),
        reloadable=config.get("reloadable", False),
        frozen=config.get("frozen", False),
    )
