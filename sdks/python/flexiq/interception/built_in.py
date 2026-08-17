"""Built-in type registry entries for all interception strategies."""

from __future__ import annotations

import collections
import datetime
import decimal
import enum
import importlib
import io
import logging as logging_mod
import pathlib
import re
import socket
import subprocess
import threading
import types as builtin_types
import uuid

from flexiq.interception import converters
from flexiq.interception.registry import TypeRegistry
from flexiq.interception.strategy import Strategy

# Sentinel for types that may not be installed
_OPTIONAL_TYPES: dict[str, tuple[str, ...]] = {
    # CONVERT types
    "pydantic.BaseModel": ("pydantic", "BaseModel"),
    # REDIRECT types
    "sqlalchemy.orm.Session": ("sqlalchemy.orm", "Session"),
    "sqlalchemy.ext.asyncio.AsyncSession": ("sqlalchemy.ext.asyncio", "AsyncSession"),
    "sqlalchemy.engine.Engine": ("sqlalchemy.engine", "Engine"),
    "sqlalchemy.ext.asyncio.AsyncEngine": ("sqlalchemy.ext.asyncio", "AsyncEngine"),
    "redis.Redis": ("redis", "Redis"),
    "redis.asyncio.Redis": ("redis.asyncio", "Redis"),
    "pymongo.MongoClient": ("pymongo", "MongoClient"),
    # REJECT types
    "asyncio.Task": ("asyncio", "Task"),
    "asyncio.Future": ("asyncio", "Future"),
}


def _try_import(module_path: str, class_name: str) -> type | None:
    """Try to import a type, returning None if not available or not a type."""
    try:
        mod = importlib.import_module(module_path)
        obj = getattr(mod, class_name, None)
        # Only return actual types — functions, modules, etc. will break isinstance()
        if obj is not None and isinstance(obj, type):
            result: type = obj
            return result
        return None
    except (ImportError, ModuleNotFoundError):
        return None


def build_default_registry() -> TypeRegistry:
    """Build the default type registry with all built-in entries."""
    reg = TypeRegistry()

    # -- PASS (priority 0, lowest — checked last) --
    # Primitive types are natively serializable. No transformation needed.
    # These are registered at priority 0 so more specific types always win.
    reg.register(
        (int, float, str, bool, bytes),
        Strategy.PASS,
        priority=0,
    )

    # -- CONVERT (priority 10) --
    # Types that need a small transformation but can be reconstructed exactly.

    reg.register(
        uuid.UUID,
        Strategy.CONVERT,
        priority=10,
        converter=converters.convert_uuid,
        type_key="uuid",
    )
    reg.register(
        datetime.datetime,
        Strategy.CONVERT,
        priority=11,  # datetime before date (datetime is a date subclass)
        converter=converters.convert_datetime,
        type_key="datetime",
    )
    reg.register(
        datetime.date,
        Strategy.CONVERT,
        priority=10,
        converter=converters.convert_date,
        type_key="date",
    )
    reg.register(
        datetime.time,
        Strategy.CONVERT,
        priority=10,
        converter=converters.convert_time,
        type_key="time",
    )
    reg.register(
        datetime.timedelta,
        Strategy.CONVERT,
        priority=10,
        converter=converters.convert_timedelta,
        type_key="timedelta",
    )
    reg.register(
        decimal.Decimal,
        Strategy.CONVERT,
        priority=10,
        converter=converters.convert_decimal,
        type_key="decimal",
    )
    reg.register(
        (pathlib.Path, pathlib.PurePath),
        Strategy.CONVERT,
        priority=10,
        converter=converters.convert_path,
        type_key="path",
    )
    reg.register(
        re.Pattern,
        Strategy.CONVERT,
        priority=10,
        converter=converters.convert_pattern,
        type_key="pattern",
    )
    reg.register(
        collections.OrderedDict,
        Strategy.CONVERT,
        priority=10,
        converter=converters.convert_ordered_dict,
        type_key="ordered_dict",
    )

    # Pydantic (optional dependency)
    pydantic_base = _try_import("pydantic", "BaseModel")
    if pydantic_base is not None:
        reg.register(
            pydantic_base,
            Strategy.CONVERT,
            priority=10,
            converter=converters.convert_pydantic,
            type_key="pydantic",
        )

    # Enum (must be lower priority than specific enum-like types)
    reg.register(
        enum.Enum,
        Strategy.CONVERT,
        priority=5,
        converter=converters.convert_enum,
        type_key="enum",
    )

    # -- REDIRECT (priority 20) --
    # These should come from worker resources, not serialized.

    _redirect_types: list[tuple[str, str, str]] = [
        ("sqlalchemy.orm", "Session", "db"),
        ("sqlalchemy.ext.asyncio", "AsyncSession", "db"),
        ("sqlalchemy.engine", "Engine", "db"),
        ("sqlalchemy.ext.asyncio", "AsyncEngine", "db"),
        ("redis", "Redis", "redis"),
        ("redis.asyncio", "Redis", "redis"),
        ("pymongo", "MongoClient", "mongo"),
        ("motor.motor_asyncio", "AsyncIOMotorClient", "mongo"),
        ("psycopg2.extensions", "connection", "db"),
        ("asyncpg.connection", "Connection", "db"),
        ("django.db.backends.base.base", "BaseDatabaseWrapper", "db"),
        ("aiohttp", "ClientSession", "aiohttp_session"),
    ]
    for mod_path, cls_name, resource in _redirect_types:
        typ = _try_import(mod_path, cls_name)
        if typ is not None:
            reg.register(
                typ,
                Strategy.REDIRECT,
                priority=20,
                redirect_resource=resource,
            )

    # -- PROXY (priority 20) --
    # Types that can be deconstructed into a recipe and reconstructed on the worker.

    reg.register(
        (io.TextIOWrapper, io.BufferedReader, io.BufferedWriter, io.FileIO),
        Strategy.PROXY,
        priority=20,
        proxy_handler="file",
    )
    reg.register(
        logging_mod.Logger,
        Strategy.PROXY,
        priority=20,
        proxy_handler="logger",
    )

    # Optional: requests.Session
    requests_session = _try_import("requests", "Session")
    if requests_session is not None:
        reg.register(
            requests_session,
            Strategy.PROXY,
            priority=20,
            proxy_handler="requests_session",
        )

    # Optional: httpx.Client / httpx.AsyncClient
    for httpx_cls in ("Client", "AsyncClient"):
        httpx_type = _try_import("httpx", httpx_cls)
        if httpx_type is not None:
            reg.register(
                httpx_type,
                Strategy.PROXY,
                priority=20,
                proxy_handler="httpx_client",
            )

    # Optional: boto3 clients
    boto3_base = _try_import("botocore.client", "BaseClient")
    if boto3_base is not None:
        reg.register(
            boto3_base,
            Strategy.PROXY,
            priority=20,
            proxy_handler="boto3_client",
        )

    # Optional: Google Cloud Storage
    for gcs_cls in ("Client", "Bucket", "Blob"):
        gcs_type = _try_import("google.cloud.storage", gcs_cls)
        if gcs_type is not None:
            reg.register(
                gcs_type,
                Strategy.PROXY,
                priority=20,
                proxy_handler="gcs_client",
            )

    # -- REJECT (priority 30, high — catch these before anything else) --

    # threading.Lock and threading.RLock are factory functions, not types.
    # Use the actual instance types for isinstance() checks.
    _lock_type = type(threading.Lock())
    _rlock_type = type(threading.RLock())

    _reject_entries: list[tuple[tuple[type, ...], str, list[str]]] = [
        (
            (_lock_type, _rlock_type, threading.Semaphore, threading.Event),
            "Thread synchronization primitives are process-local and cannot be sent to a worker.",
            [
                "Use flexiq's distributed lock: queue.lock('name')",
                "Remove the lock argument and use worker-scoped resources instead",
            ],
        ),
        (
            (socket.socket,),
            "Sockets are OS-level handles tied to the current process.",
            [
                "Pass connection parameters (host, port) instead and create it in the task",
                "Use a worker resource for persistent connections",
            ],
        ),
        (
            (builtin_types.GeneratorType,),
            "Generators are lazy iterators with internal state that cannot be serialized.",
            [
                "Materialize the generator to a list first: list(gen)",
                "Pass the generator's input parameters and recreate it in the task",
            ],
        ),
        (
            (builtin_types.CoroutineType,),
            "Coroutine objects cannot be serialized. They must be awaited in-process.",
            [
                "Await the coroutine before passing the result to delay()",
                "Pass the coroutine's arguments and call the async function inside the task",
            ],
        ),
        (
            (subprocess.Popen,),
            "Subprocess handles are OS-level and tied to the current process.",
            [
                "Pass the command and arguments instead and run the subprocess in the task",
            ],
        ),
    ]
    for types, reason, suggestions in _reject_entries:
        reg.register(
            types,
            Strategy.REJECT,
            priority=30,
            reject_reason=reason,
            reject_suggestions=suggestions,
        )

    # asyncio.Task and asyncio.Future
    for cls_name, reason in [
        ("Task", "Asyncio tasks are tied to a specific event loop and cannot be serialized."),
        ("Future", "Asyncio futures are tied to a specific event loop and cannot be serialized."),
    ]:
        typ = _try_import("asyncio", cls_name)
        if typ is not None:
            reg.register(
                typ,
                Strategy.REJECT,
                priority=30,
                reject_reason=reason,
                reject_suggestions=[
                    "Await the task/future and pass its result instead",
                ],
            )

    # contextvars.Context
    ctx_type = _try_import("contextvars", "Context")
    if ctx_type is not None:
        reg.register(
            ctx_type,
            Strategy.REJECT,
            priority=30,
            reject_reason=("Context variables are process-local and cannot be serialized."),
            reject_suggestions=[
                "Extract the values you need from the context and pass them directly",
            ],
        )

    # multiprocessing types
    _mp_reject: list[tuple[str, str, str]] = [
        (
            "multiprocessing.synchronize",
            "Lock",
            "Multiprocessing locks are tied to the current process.",
        ),
        (
            "multiprocessing.queues",
            "Queue",
            "Multiprocessing queues are tied to the current process.",
        ),
    ]
    for mod_path, cls_name, reason in _mp_reject:
        typ = _try_import(mod_path, cls_name)
        if typ is not None:
            reg.register(
                typ,
                Strategy.REJECT,
                priority=30,
                reject_reason=reason,
                reject_suggestions=[
                    "Use flexiq's task queue instead of multiprocessing primitives",
                ],
            )

    # Dataclass detection is handled specially in the walker since
    # dataclasses.is_dataclass() is needed (not isinstance). We register
    # a converter that the walker calls after checking is_dataclass().
    # NamedTuple, lambda, and tempfile detection are also in the walker.

    return reg
