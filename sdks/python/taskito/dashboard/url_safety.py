"""Outbound URL safety checks for dashboard-configured webhooks.

We refuse to deliver to loopback, link-local, and RFC1918 addresses by
default — an operator who can write to ``dashboard_settings`` could
otherwise turn the worker into an SSRF proxy. The ``FLEXIQ_WEBHOOKS_ALLOW_PRIVATE``
environment variable disables the guard for local development.
"""

from __future__ import annotations

import ipaddress
import os
import socket
import urllib.parse

# Hostnames that always resolve to loopback / never-leave-this-host regardless
# of DNS, but might be missed by a strict ``ipaddress.is_private`` check.
_BLOCKED_HOSTNAME_SUFFIXES = (
    ".localhost",
    ".local",
    ".internal",
    ".intranet",
    ".lan",
    ".private",
)
_BLOCKED_HOSTNAMES = frozenset(
    {"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"}
)

_ALLOW_ENV_VAR = "FLEXIQ_WEBHOOKS_ALLOW_PRIVATE"


class UnsafeWebhookUrl(ValueError):
    """Raised when a webhook URL targets an address we won't deliver to."""


def _is_private_ip(ip: str) -> bool:
    try:
        address = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return (
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


def _hostname_is_blocked(hostname: str) -> bool:
    lowered = hostname.lower()
    if lowered in _BLOCKED_HOSTNAMES:
        return True
    return any(lowered.endswith(suffix) for suffix in _BLOCKED_HOSTNAME_SUFFIXES)


def validate_webhook_url(url: str) -> None:
    """Reject ``url`` if it targets a private/loopback/link-local destination.

    Set ``FLEXIQ_WEBHOOKS_ALLOW_PRIVATE=1`` in the environment to disable
    the guard (intended for local development against ``http://localhost``).

    Raises:
        UnsafeWebhookUrl: on scheme other than http/https, missing host, or
            a host that resolves to a private/loopback IP.
    """
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise UnsafeWebhookUrl(f"URL scheme must be http or https, got {parsed.scheme!r}")
    if not parsed.hostname:
        raise UnsafeWebhookUrl("URL must include a hostname")

    if os.environ.get(_ALLOW_ENV_VAR):
        return

    hostname = parsed.hostname
    if _hostname_is_blocked(hostname):
        raise UnsafeWebhookUrl(f"URL host {hostname!r} resolves to a private network")

    # Literal IPs are checked directly; named hosts are resolved.
    try:
        ipaddress.ip_address(hostname)
        addresses: list[str] = [hostname]
    except ValueError:
        try:
            addresses = [
                str(info[4][0])
                for info in socket.getaddrinfo(hostname, None, type=socket.SOCK_STREAM)
            ]
        except OSError as e:
            raise UnsafeWebhookUrl(f"could not resolve {hostname!r}: {e}") from None

    for ip in addresses:
        if _is_private_ip(ip):
            raise UnsafeWebhookUrl(f"URL host {hostname!r} resolves to private address {ip}")


def is_safe_redirect(path: str | None) -> bool:
    """Whether ``path`` is safe to use as a post-login same-origin redirect target.

    Accepts only relative paths rooted at ``/``. Rejects absolute URLs
    (``http://evil.com/x``), protocol-relative URLs (``//evil.com/x``),
    and anything without a leading slash. Empty / ``None`` is rejected so
    the caller can fall back to a default explicitly.
    """
    if not path:
        return False
    if not path.startswith("/"):
        return False
    if path.startswith("//") or path.startswith("/\\"):
        return False
    parsed = urllib.parse.urlparse(path)
    return not (parsed.scheme or parsed.netloc)
