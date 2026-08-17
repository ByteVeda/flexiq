"""Register built-in proxy handlers."""

from __future__ import annotations

import enum
from collections.abc import Sequence

from flexiq.enums import coerce_enum
from flexiq.proxies.handlers.file import FileHandler
from flexiq.proxies.handlers.logger import LoggerHandler
from flexiq.proxies.registry import ProxyRegistry


class BuiltInProxy(str, enum.Enum):
    """Ids of the proxy handlers registered by default, for ``disabled_proxies``."""

    FILE = "file"
    LOGGER = "logger"
    REQUESTS_SESSION = "requests_session"
    HTTPX_CLIENT = "httpx_client"
    BOTO3_CLIENT = "boto3_client"
    GCS_CLIENT = "gcs_client"


def register_builtin_handlers(
    registry: ProxyRegistry,
    *,
    disabled_proxies: Sequence[BuiltInProxy | str] | None = None,
    file_path_allowlist: list[str] | None = None,
) -> None:
    """Register all built-in proxy handlers into the given registry.

    Args:
        registry: The proxy registry to register handlers with.
        disabled_proxies: Handlers to skip registration for. An unknown id
            raises rather than silently leaving the handler registered.
        file_path_allowlist: Allowed paths for the file handler.
    """
    disabled = {
        coerce_enum(BuiltInProxy, proxy, param="disabled_proxies entry")
        for proxy in disabled_proxies or ()
    }

    if BuiltInProxy.FILE not in disabled:
        registry.register(FileHandler(path_allowlist=file_path_allowlist or []))
    if BuiltInProxy.LOGGER not in disabled:
        registry.register(LoggerHandler())

    # Optional dependencies
    if BuiltInProxy.REQUESTS_SESSION not in disabled:
        try:
            from flexiq.proxies.handlers.requests_session import (
                _HAS_REQUESTS,
                RequestsSessionHandler,
            )

            if _HAS_REQUESTS:
                registry.register(RequestsSessionHandler())
        except ImportError:
            pass

    if BuiltInProxy.HTTPX_CLIENT not in disabled:
        try:
            from flexiq.proxies.handlers.httpx_client import (
                _HAS_HTTPX,
                HttpxClientHandler,
            )

            if _HAS_HTTPX:
                registry.register(HttpxClientHandler())
        except ImportError:
            pass

    if BuiltInProxy.BOTO3_CLIENT not in disabled:
        try:
            from flexiq.proxies.handlers.boto3_client import (
                _HAS_BOTO3,
                Boto3ClientHandler,
            )

            if _HAS_BOTO3:
                registry.register(Boto3ClientHandler())
        except ImportError:
            pass

    if BuiltInProxy.GCS_CLIENT not in disabled:
        try:
            from flexiq.proxies.handlers.gcs_client import (
                _HAS_GCS,
                GCSClientHandler,
            )

            if _HAS_GCS:
                registry.register(GCSClientHandler())
        except ImportError:
            pass
