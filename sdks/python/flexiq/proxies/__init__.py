"""Resource proxies — transparent deconstruction and reconstruction of objects."""

from flexiq.proxies.built_in import BuiltInProxy
from flexiq.proxies.handler import ProxyHandler
from flexiq.proxies.no_proxy import NoProxy
from flexiq.proxies.reconstruct import cleanup_proxies, reconstruct_proxies
from flexiq.proxies.registry import ProxyRegistry

__all__ = [
    "BuiltInProxy",
    "NoProxy",
    "ProxyHandler",
    "ProxyRegistry",
    "cleanup_proxies",
    "reconstruct_proxies",
]
