"""Intelligent argument interception for flexiq.

Three layers of argument handling before serialization:
- PASS: natively serializable, zero overhead
- CONVERT: auto-transform (UUID, datetime, Pydantic, dataclass, etc.)
- REDIRECT: substitute DI marker for worker-side resource injection
- PROXY: extract recipe for worker-side reconstruction (Phase 3)
- REJECT: raise with actionable error message
"""

from flexiq.interception.errors import ArgumentFailure, InterceptionError
from flexiq.interception.interceptor import ArgumentInterceptor, InterceptionReport
from flexiq.interception.mode import InterceptionMode
from flexiq.interception.reconstruct import reconstruct_args
from flexiq.interception.registry import TypeRegistry
from flexiq.interception.strategy import Strategy

__all__ = [
    "ArgumentFailure",
    "ArgumentInterceptor",
    "InterceptionError",
    "InterceptionMode",
    "InterceptionReport",
    "Strategy",
    "TypeRegistry",
    "reconstruct_args",
]
