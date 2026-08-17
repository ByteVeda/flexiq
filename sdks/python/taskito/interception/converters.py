"""Built-in converters and reconstructors for CONVERT strategy types."""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import importlib
import pathlib
import re
import uuid
from typing import Any


def _import_type(type_path: str) -> type:
    """Import a type from its fully-qualified dotted path.

    The path comes from the (untrusted) payload, so the resolved object must
    be a class — never an arbitrary callable like ``os.system``. Per-kind
    checks in the reconstructors below further constrain *which* classes are
    acceptable.
    """
    module_path, _, class_name = type_path.rpartition(".")
    if not module_path:
        raise ValueError(f"Invalid type path: {type_path!r}")
    module = importlib.import_module(module_path)
    obj = getattr(module, class_name)
    if not isinstance(obj, type):
        raise ValueError(f"Type path {type_path!r} did not resolve to a class")
    return obj


def _type_path(cls: type) -> str:
    """Get the fully-qualified dotted path for a type."""
    return f"{cls.__module__}.{cls.__qualname__}"


# -- UUID --


def convert_uuid(obj: Any) -> dict[str, Any]:
    return {"__flexiq_convert__": True, "type_key": "uuid", "value": str(obj)}


def reconstruct_uuid(data: dict[str, Any]) -> Any:

    return uuid.UUID(data["value"])


# -- datetime --


def convert_datetime(obj: Any) -> dict[str, Any]:
    return {"__flexiq_convert__": True, "type_key": "datetime", "value": obj.isoformat()}


def reconstruct_datetime(data: dict[str, Any]) -> Any:

    return datetime.datetime.fromisoformat(data["value"])


# -- date --


def convert_date(obj: Any) -> dict[str, Any]:
    return {"__flexiq_convert__": True, "type_key": "date", "value": obj.isoformat()}


def reconstruct_date(data: dict[str, Any]) -> Any:

    return datetime.date.fromisoformat(data["value"])


# -- time --


def convert_time(obj: Any) -> dict[str, Any]:
    return {"__flexiq_convert__": True, "type_key": "time", "value": obj.isoformat()}


def reconstruct_time(data: dict[str, Any]) -> Any:

    return datetime.time.fromisoformat(data["value"])


# -- timedelta --


def convert_timedelta(obj: Any) -> dict[str, Any]:
    return {"__flexiq_convert__": True, "type_key": "timedelta", "value": obj.total_seconds()}


def reconstruct_timedelta(data: dict[str, Any]) -> Any:

    return datetime.timedelta(seconds=data["value"])


# -- Decimal --


def convert_decimal(obj: Any) -> dict[str, Any]:
    return {"__flexiq_convert__": True, "type_key": "decimal", "value": str(obj)}


def reconstruct_decimal(data: dict[str, Any]) -> Any:

    return decimal.Decimal(data["value"])


# -- Path --


def convert_path(obj: Any) -> dict[str, Any]:
    return {"__flexiq_convert__": True, "type_key": "path", "value": str(obj)}


def reconstruct_path(data: dict[str, Any]) -> Any:

    return pathlib.Path(data["value"])


# -- Enum --


def convert_enum(obj: Any) -> dict[str, Any]:
    return {
        "__flexiq_convert__": True,
        "type_key": "enum",
        "type_path": _type_path(type(obj)),
        "value": obj.value,
    }


def reconstruct_enum(data: dict[str, Any]) -> Any:
    cls = _import_type(data["type_path"])
    if not issubclass(cls, enum.Enum):
        raise ValueError(f"{data['type_path']!r} is not an Enum")
    return cls(data["value"])


# -- Pydantic BaseModel --


def convert_pydantic(obj: Any) -> dict[str, Any]:
    return {
        "__flexiq_convert__": True,
        "type_key": "pydantic",
        "type_path": _type_path(type(obj)),
        "value": obj.model_dump(mode="json"),
    }


def reconstruct_pydantic(data: dict[str, Any]) -> Any:
    cls = _import_type(data["type_path"])
    # Duck-typed so pydantic stays an optional dependency: a genuine model
    # class exposes ``model_validate`` / ``model_dump``.
    if not (hasattr(cls, "model_validate") and hasattr(cls, "model_fields")):
        raise ValueError(f"{data['type_path']!r} is not a pydantic model")
    return cls.model_validate(data["value"])


# -- dataclass --


def convert_dataclass(obj: Any) -> dict[str, Any]:
    return {
        "__flexiq_convert__": True,
        "type_key": "dataclass",
        "type_path": _type_path(type(obj)),
        "value": dataclasses.asdict(obj),
    }


def reconstruct_dataclass(data: dict[str, Any]) -> Any:
    cls = _import_type(data["type_path"])
    if not dataclasses.is_dataclass(cls):
        raise ValueError(f"{data['type_path']!r} is not a dataclass")
    return cls(**data["value"])


# -- re.Pattern --


def convert_pattern(obj: Any) -> dict[str, Any]:
    return {
        "__flexiq_convert__": True,
        "type_key": "pattern",
        "value": obj.pattern,
        "flags": obj.flags,
    }


def reconstruct_pattern(data: dict[str, Any]) -> Any:

    return re.compile(data["value"], data["flags"])


# -- NamedTuple --


def convert_named_tuple(obj: Any) -> dict[str, Any]:
    return {
        "__flexiq_convert__": True,
        "type_key": "named_tuple",
        "type_path": _type_path(type(obj)),
        "fields": list(obj._asdict().values()),
    }


def reconstruct_named_tuple(data: dict[str, Any]) -> Any:
    cls = _import_type(data["type_path"])
    if not (issubclass(cls, tuple) and hasattr(cls, "_fields")):
        raise ValueError(f"{data['type_path']!r} is not a NamedTuple")
    return cls(*data["fields"])


# -- OrderedDict --


def convert_ordered_dict(obj: Any) -> dict[str, Any]:
    return {
        "__flexiq_convert__": True,
        "type_key": "ordered_dict",
        "pairs": list(obj.items()),
    }


def reconstruct_ordered_dict(data: dict[str, Any]) -> Any:

    return collections.OrderedDict(data["pairs"])


# -- Reconstructor dispatch --

_RECONSTRUCTORS: dict[str, Any] = {
    "uuid": reconstruct_uuid,
    "datetime": reconstruct_datetime,
    "date": reconstruct_date,
    "time": reconstruct_time,
    "timedelta": reconstruct_timedelta,
    "decimal": reconstruct_decimal,
    "path": reconstruct_path,
    "enum": reconstruct_enum,
    "pydantic": reconstruct_pydantic,
    "dataclass": reconstruct_dataclass,
    "pattern": reconstruct_pattern,
    "named_tuple": reconstruct_named_tuple,
    "ordered_dict": reconstruct_ordered_dict,
}


def reconstruct_converted(data: dict[str, Any]) -> Any:
    """Reconstruct a CONVERT marker back to its original type."""
    type_key = data.get("type_key")
    fn = _RECONSTRUCTORS.get(type_key)  # type: ignore[arg-type]
    if fn is None:
        raise ValueError(f"Unknown convert type_key: {type_key!r}")
    return fn(data)
