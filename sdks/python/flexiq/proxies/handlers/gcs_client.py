"""GCSClientHandler — proxy for Google Cloud Storage objects."""

from __future__ import annotations

from typing import Any

try:
    from google.cloud.storage import Blob as _GCSBlob  # type: ignore[import-not-found]
    from google.cloud.storage import Bucket as _GCSBucket
    from google.cloud.storage import Client as _GCSClient

    _HAS_GCS = True
except ImportError:
    _GCSClient = None
    _GCSBucket = None
    _GCSBlob = None
    _HAS_GCS = False


class GCSClientHandler:
    """Deconstructs/reconstructs Google Cloud Storage objects.

    Credentials are NOT included in the recipe — the worker uses its
    ambient credentials (Application Default Credentials).
    """

    name = "gcs_client"
    version = 1
    handled_types: tuple[type, ...] = ()

    def detect(self, obj: Any) -> bool:
        if not _HAS_GCS:
            return False
        return isinstance(obj, (_GCSClient, _GCSBucket, _GCSBlob))

    def deconstruct(self, obj: Any) -> dict[str, Any]:
        if _HAS_GCS and isinstance(obj, _GCSBlob):
            return {
                "type": "blob",
                "project": obj.client.project if obj.client else None,
                "bucket_name": obj.bucket.name,
                "blob_name": obj.name,
            }
        if _HAS_GCS and isinstance(obj, _GCSBucket):
            return {
                "type": "bucket",
                "project": obj.client.project if obj.client else None,
                "bucket_name": obj.name,
            }
        # Client
        return {
            "type": "client",
            "project": getattr(obj, "project", None),
        }

    def reconstruct(self, recipe: dict[str, Any], version: int) -> Any:
        from google.cloud import storage  # type: ignore[import-not-found]

        obj_type = recipe["type"]
        project = recipe.get("project")

        if obj_type == "client":
            return storage.Client(project=project)
        if obj_type == "bucket":
            client = storage.Client(project=project)
            return client.bucket(recipe["bucket_name"])
        if obj_type == "blob":
            client = storage.Client(project=project)
            bucket = client.bucket(recipe["bucket_name"])
            return bucket.blob(recipe["blob_name"])

        raise ValueError(f"Unknown GCS object type: {obj_type}")

    def cleanup(self, obj: Any) -> None:
        pass  # GCS objects manage their own lifecycle
