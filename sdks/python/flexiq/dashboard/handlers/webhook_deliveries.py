"""Webhook delivery log endpoints (list / get / replay)."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Any

from flexiq.dashboard.delivery_store import DeliveryRecord, DeliveryStatus, DeliveryStore
from flexiq.dashboard.errors import _BadRequest, _NotFound
from flexiq.dashboard.webhook_store import WebhookSubscriptionStore

if TYPE_CHECKING:
    from flexiq.app import Queue


_MAX_PAGE_SIZE = 200


def _serialize(record: DeliveryRecord) -> dict[str, Any]:
    return asdict(record)


def _parse_int_param(qs: dict, name: str, default: int, *, minimum: int = 0) -> int:
    raw = qs.get(name, [None])[0]
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        raise _BadRequest(f"{name} must be an integer") from None
    if value < minimum:
        raise _BadRequest(f"{name} must be >= {minimum}")
    return value


def _ensure_subscription(queue: Queue, subscription_id: str) -> None:
    sub = WebhookSubscriptionStore(queue).get(subscription_id)
    if sub is None:
        raise _NotFound(f"webhook '{subscription_id}' not found")


def handle_list_deliveries(queue: Queue, qs: dict, subscription_id: str) -> dict[str, Any]:
    """List recent deliveries for a subscription. Supports ``status``,
    ``event``, ``limit``, and ``offset`` query parameters."""
    _ensure_subscription(queue, subscription_id)

    status_raw = qs.get("status", [None])[0]
    status: DeliveryStatus | None = None
    if status_raw:
        try:
            status = DeliveryStatus(status_raw)
        except ValueError:
            valid = ", ".join(s.value for s in DeliveryStatus)
            raise _BadRequest(f"status must be one of: {valid}") from None
    event = qs.get("event", [None])[0]

    limit = min(_parse_int_param(qs, "limit", 50, minimum=1), _MAX_PAGE_SIZE)
    offset = _parse_int_param(qs, "offset", 0)

    store = DeliveryStore(queue)
    items = store.list_for(subscription_id, status=status, event=event, limit=limit, offset=offset)
    return {
        "items": [_serialize(r) for r in items],
        "limit": limit,
        "offset": offset,
        "total": store.count_for(subscription_id),
    }


def handle_get_delivery(
    queue: Queue, _qs: dict, sub_and_delivery_id: tuple[str, str]
) -> dict[str, Any]:
    subscription_id, delivery_id = sub_and_delivery_id
    record = DeliveryStore(queue).get(subscription_id, delivery_id)
    if record is None:
        raise _NotFound(f"delivery '{delivery_id}' not found")
    return _serialize(record)


def handle_replay_delivery(queue: Queue, sub_and_delivery_id: tuple[str, str]) -> dict[str, Any]:
    """Re-enqueue a stored delivery's original payload as a fresh attempt.

    The replay creates a NEW delivery record on top of the existing one
    so the audit trail is preserved. Returns the new delivery's id and
    the synchronous HTTP status from the first attempt.
    """
    subscription_id, delivery_id = sub_and_delivery_id
    sub = WebhookSubscriptionStore(queue).get(subscription_id)
    if sub is None:
        raise _NotFound(f"webhook '{subscription_id}' not found")
    record = DeliveryStore(queue).get(subscription_id, delivery_id)
    if record is None:
        raise _NotFound(f"delivery '{delivery_id}' not found")

    from flexiq.webhooks import WebhookManager

    runtime = WebhookManager._subscription_to_runtime(sub)
    payload = {**record.payload, "replay_of": record.id}
    status = queue._webhook_manager.deliver_now(runtime, payload)
    # deliver_now does NOT write to the log. Record a replay entry so the
    # operator can see it appear in the deliveries list.
    DeliveryStore(queue).record_attempt(
        subscription_id,
        event=str(payload.get("event", record.event)),
        payload=payload,
        status=(
            DeliveryStatus.DELIVERED
            if status is not None and status < 400
            else DeliveryStatus.FAILED
        ),
        attempts=1,
        response_code=status,
        task_name=record.task_name,
        job_id=record.job_id,
    )
    return {
        "replayed_of": record.id,
        "status": status,
        "delivered": status is not None and status < 400,
    }
