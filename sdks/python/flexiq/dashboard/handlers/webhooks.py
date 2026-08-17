"""Webhook subscription CRUD endpoints."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from flexiq.dashboard.errors import _BadRequest, _NotFound
from flexiq.dashboard.url_safety import UnsafeWebhookUrl, validate_webhook_url
from flexiq.dashboard.webhook_store import (
    WebhookSubscription,
    WebhookSubscriptionStore,
    generate_secret,
)
from flexiq.events import EventType

if TYPE_CHECKING:
    from flexiq.app import Queue


_VALID_EVENT_VALUES = frozenset(e.value for e in EventType)


# ── Serialization ─────────────────────────────────────────────────────


def _serialize(
    subscription: WebhookSubscription, *, reveal_secret: bool = False
) -> dict[str, Any]:
    """Convert to a JSON-safe dict. The raw secret is redacted unless the
    caller is ``reveal_secret``-ing (used by the create and rotate endpoints,
    which need to surface the value to the user exactly once)."""
    row = asdict(subscription)
    secret = row.pop("secret", None)
    row["has_secret"] = bool(secret)
    if reveal_secret and secret:
        row["secret"] = secret
    return row


# ── Validation helpers ────────────────────────────────────────────────


def _require_str(body: dict, key: str) -> str:
    value = body.get(key)
    if not isinstance(value, str) or not value:
        raise _BadRequest(f"missing or empty field '{key}'")
    return value


def _coerce_event_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise _BadRequest("events must be a list of event type strings")
    events: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise _BadRequest("events must contain only strings")
        if item not in _VALID_EVENT_VALUES:
            raise _BadRequest(f"unknown event type {item!r}")
        events.append(item)
    return events


def _coerce_task_filter(value: Any) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise _BadRequest("task_filter must be a list of task names or null")
    out: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item:
            raise _BadRequest("task_filter entries must be non-empty strings")
        out.append(item)
    return out


def _coerce_headers(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise _BadRequest("headers must be an object of string→string")
    out: dict[str, str] = {}
    for k, v in value.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise _BadRequest("headers must map strings to strings")
        out[k] = v
    return out


def _coerce_positive_int(value: Any, name: str, default: int) -> int:
    if value is None:
        return default
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise _BadRequest(f"{name} must be a non-negative integer")
    return value


def _coerce_positive_float(value: Any, name: str, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise _BadRequest(f"{name} must be a positive number")
    return float(value)


# ── Handlers ──────────────────────────────────────────────────────────


def handle_list_webhooks(queue: Queue, _qs: dict) -> list[dict[str, Any]]:
    return [_serialize(s) for s in WebhookSubscriptionStore(queue).list_all()]


def handle_get_webhook(queue: Queue, _qs: dict, subscription_id: str) -> dict[str, Any]:
    sub = WebhookSubscriptionStore(queue).get(subscription_id)
    if sub is None:
        raise _NotFound(f"webhook '{subscription_id}' not found")
    return _serialize(sub)


def handle_create_webhook(queue: Queue, body: dict) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _BadRequest("body must be a JSON object")
    url = _require_str(body, "url")
    try:
        validate_webhook_url(url)
    except UnsafeWebhookUrl as e:
        raise _BadRequest(str(e)) from None

    events = _coerce_event_list(body.get("events"))
    task_filter = _coerce_task_filter(body.get("task_filter"))
    headers = _coerce_headers(body.get("headers"))
    max_retries = _coerce_positive_int(body.get("max_retries"), "max_retries", 3)
    timeout_seconds = _coerce_positive_float(body.get("timeout_seconds"), "timeout_seconds", 10.0)
    retry_backoff = _coerce_positive_float(body.get("retry_backoff"), "retry_backoff", 2.0)

    secret = body.get("secret")
    if secret is not None and not isinstance(secret, str):
        raise _BadRequest("secret must be a string or null")
    if body.get("generate_secret"):
        secret = generate_secret()

    description = body.get("description")
    if description is not None and not isinstance(description, str):
        raise _BadRequest("description must be a string or null")

    sub = queue.add_webhook(
        url=url,
        events=[EventType(v) for v in events] if events else None,
        headers=headers,
        secret=secret,
        max_retries=max_retries,
        timeout=timeout_seconds,
        retry_backoff=retry_backoff,
        task_filter=task_filter,
        description=description,
    )
    return _serialize(sub, reveal_secret=True)


def handle_update_webhook(queue: Queue, body: dict, subscription_id: str) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise _BadRequest("body must be a JSON object")
    sub = WebhookSubscriptionStore(queue).get(subscription_id)
    if sub is None:
        raise _NotFound(f"webhook '{subscription_id}' not found")

    changes: dict[str, Any] = {}
    if "url" in body:
        url = _require_str(body, "url")
        try:
            validate_webhook_url(url)
        except UnsafeWebhookUrl as e:
            raise _BadRequest(str(e)) from None
        changes["url"] = url
    if "events" in body:
        changes["events"] = _coerce_event_list(body["events"])
    if "task_filter" in body:
        changes["task_filter"] = _coerce_task_filter(body["task_filter"])
    if "headers" in body:
        changes["headers"] = _coerce_headers(body["headers"])
    if "max_retries" in body:
        changes["max_retries"] = _coerce_positive_int(body["max_retries"], "max_retries", 3)
    if "timeout_seconds" in body:
        changes["timeout_seconds"] = _coerce_positive_float(
            body["timeout_seconds"], "timeout_seconds", 10.0
        )
    if "retry_backoff" in body:
        changes["retry_backoff"] = _coerce_positive_float(
            body["retry_backoff"], "retry_backoff", 2.0
        )
    if "enabled" in body:
        if not isinstance(body["enabled"], bool):
            raise _BadRequest("enabled must be a boolean")
        changes["enabled"] = body["enabled"]
    if "description" in body:
        description = body["description"]
        if description is not None and not isinstance(description, str):
            raise _BadRequest("description must be a string or null")
        changes["description"] = description

    updated = queue.update_webhook(subscription_id, **changes)
    return _serialize(updated)


def handle_delete_webhook(queue: Queue, subscription_id: str) -> dict[str, bool]:
    removed = queue.remove_webhook(subscription_id)
    if not removed:
        raise _NotFound(f"webhook '{subscription_id}' not found")
    return {"deleted": True}


def handle_rotate_secret(queue: Queue, subscription_id: str) -> dict[str, Any]:
    if WebhookSubscriptionStore(queue).get(subscription_id) is None:
        raise _NotFound(f"webhook '{subscription_id}' not found")
    secret = queue.rotate_webhook_secret(subscription_id)
    return {"id": subscription_id, "secret": secret}


def handle_test_webhook(queue: Queue, subscription_id: str) -> dict[str, Any]:
    """Synchronously POST a synthetic event and return the result inline."""
    sub = WebhookSubscriptionStore(queue).get(subscription_id)
    if sub is None:
        raise _NotFound(f"webhook '{subscription_id}' not found")

    from flexiq.webhooks import WebhookManager

    runtime = WebhookManager._subscription_to_runtime(sub)
    payload = {
        "event": "test.ping",
        "task_name": None,
        "subscription_id": sub.id,
        "message": "synthetic test event from dashboard",
    }
    status = queue._webhook_manager.deliver_now(runtime, payload)
    return {"status": status, "delivered": status is not None and status < 400}


def handle_list_event_types(_queue: Queue, _qs: dict) -> list[str]:
    return sorted(e.value for e in EventType)
