"""Boto3ClientHandler — proxy for AWS boto3 client objects."""

from __future__ import annotations

from typing import Any

try:
    import importlib.util

    _HAS_BOTO3 = importlib.util.find_spec("botocore") is not None
except (ImportError, ModuleNotFoundError):
    _HAS_BOTO3 = False


class Boto3ClientHandler:
    """Deconstructs/reconstructs boto3 service clients.

    Credentials are NOT included in the recipe — the worker uses its
    ambient credentials (IAM role, env vars, ``~/.aws/credentials``).
    """

    name = "boto3_client"
    version = 1
    handled_types: tuple[type, ...] = ()

    def detect(self, obj: Any) -> bool:
        if not _HAS_BOTO3:
            return False
        return (
            hasattr(obj, "_endpoint")
            and hasattr(obj, "meta")
            and hasattr(getattr(obj, "meta", None), "service_model")
        )

    def deconstruct(self, obj: Any) -> dict[str, Any]:
        endpoint_url = None
        if hasattr(obj._endpoint, "host"):
            endpoint_url = obj._endpoint.host
        return {
            "service_name": obj.meta.service_model.service_name,
            "region_name": obj.meta.region_name,
            "endpoint_url": endpoint_url,
        }

    def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
        import boto3  # type: ignore[import-not-found]

        kwargs: dict[str, Any] = {"region_name": recipe.get("region_name")}
        if recipe.get("endpoint_url"):
            kwargs["endpoint_url"] = recipe["endpoint_url"]
        return boto3.client(recipe["service_name"], **kwargs)

    def cleanup(self, obj: Any) -> None:
        pass  # boto3 clients manage their own lifecycle
