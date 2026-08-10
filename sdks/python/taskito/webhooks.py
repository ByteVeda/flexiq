"""Webhook delivery for job events.

The manager keeps an in-memory snapshot of the active subscriptions for
fast dispatch and rehydrates that snapshot from
:class:`~taskito.dashboard.webhook_store.WebhookSubscriptionStore` on
start (and on demand via :meth:`reload`). All add/update/delete writes
go through the DB-backed store so changes survive restarts and propagate
to every worker.

In-memory subscriptions registered through the legacy
``add_webhook(url, ...)`` API continue to work but are not persisted —
that path is kept for backward compatibility with code that constructs
a ``Queue`` without a settings store yet (rare in practice).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import queue
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import TYPE_CHECKING, Any

from taskito.dashboard.delivery_store import DeliveryStatus, DeliveryStore
from taskito.dashboard.url_safety import UnsafeWebhookUrl, validate_webhook_url
from taskito.events import EventType

if TYPE_CHECKING:
    from taskito.app import Queue
    from taskito.dashboard.webhook_store import WebhookSubscription

logger = logging.getLogger("taskito.webhooks")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Redirect handler that refuses to follow 3xx responses.

    Without this, a destination that passed the SSRF check could 30x-bounce
    delivery to a private/metadata address that the validator never saw.
    """

    def redirect_request(self, *_args: Any, **_kwargs: Any) -> None:
        return None


# Module-level opener reused across deliveries — it follows no redirects.
_NO_REDIRECT_OPENER = urllib.request.build_opener(_NoRedirect)


class WebhookManager:
    """Delivers webhook POST requests for job events.

    Uses a background daemon thread with a queue for non-blocking delivery.
    Each webhook is retried up to its configured ``max_retries`` with
    exponential backoff.
    """

    def __init__(self, queue_ref: Queue | None = None, preload: bool = True) -> None:
        # ``queue_ref`` is the parent :class:`taskito.app.Queue`. Optional
        # so legacy in-process tests can construct a bare manager. ``preload``
        # is off for a queue whose schema is gated behind an explicit migrate —
        # there is nothing to read until then.
        self._queue: Queue | None = queue_ref
        # In-memory subscription list. Each entry is a dict shaped like a
        # legacy ``add_webhook`` call so both code paths share a single
        # delivery loop.
        self._webhooks: list[dict[str, Any]] = []
        self._delivery_queue: queue.Queue[tuple[dict[str, Any], dict[str, Any]]] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        if queue_ref is not None and preload:
            self.reload()

    # ── Snapshot management ───────────────────────────────────────

    def reload(self) -> None:
        """Refresh the in-memory snapshot from the persistent store."""
        if self._queue is None:
            return
        from taskito.dashboard.webhook_store import WebhookSubscriptionStore

        store = WebhookSubscriptionStore(self._queue)
        snapshot = [self._subscription_to_runtime(s) for s in store.list_all()]
        with self._lock:
            self._webhooks = snapshot
        if snapshot:
            self._ensure_thread()

    @staticmethod
    def _subscription_to_runtime(sub: WebhookSubscription) -> dict[str, Any]:
        return {
            "subscription_id": sub.id,
            "url": sub.url,
            "events": set(sub.events) if sub.events else None,
            "task_filter": set(sub.task_filter) if sub.task_filter is not None else None,
            "headers": dict(sub.headers),
            "secret": sub.secret.encode() if sub.secret else None,
            "max_retries": sub.max_retries,
            "timeout": sub.timeout_seconds,
            "retry_backoff": sub.retry_backoff,
            "enabled": sub.enabled,
        }

    # ── Public API (legacy + new) ─────────────────────────────────

    def add_webhook(
        self,
        url: str,
        events: list[EventType] | None = None,
        headers: dict[str, str] | None = None,
        secret: str | None = None,
        max_retries: int = 3,
        timeout: float = 10.0,
        retry_backoff: float = 2.0,
    ) -> None:
        """Register a webhook endpoint (in-memory; not persisted).

        Prefer :meth:`Queue.create_webhook` for new code — it persists
        through the dashboard-managed store and survives restarts.
        """
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            raise ValueError(f"Webhook URL must use http:// or https://, got {parsed.scheme!r}")
        with self._lock:
            self._webhooks.append(
                {
                    "subscription_id": None,
                    "url": url,
                    "events": {e.value for e in events} if events else None,
                    "task_filter": None,
                    "headers": headers or {},
                    "secret": secret.encode() if secret else None,
                    "max_retries": max_retries,
                    "timeout": timeout,
                    "retry_backoff": retry_backoff,
                    "enabled": True,
                }
            )
        self._ensure_thread()

    def notify(self, event_type: EventType, payload: dict[str, Any]) -> None:
        """Queue an event for delivery to matching webhooks."""
        # Fast path: no subscriptions → skip the lock and list copy entirely.
        # The unlocked read is a benign race; a webhook added concurrently is
        # picked up by the next event.
        if not self._webhooks:
            return
        with self._lock:
            webhooks = list(self._webhooks)
        task_name = payload.get("task_name")
        wire_event = event_type.value
        for wh in webhooks:
            if not wh.get("enabled", True):
                continue
            if wh["events"] is not None and wire_event not in wh["events"]:
                continue
            task_filter = wh.get("task_filter")
            if task_filter is not None and task_name not in task_filter:
                continue
            self._delivery_queue.put((wh, {"event": wire_event, **payload}))

    def deliver_now(self, wh: dict[str, Any], payload: dict[str, Any]) -> int | None:
        """Synchronously deliver one payload. Returns the final HTTP status or
        ``None`` if every attempt failed at the transport level.

        Used by the dashboard "send test event" endpoint so the operator
        sees the result inline. Does NOT add to the retry queue.
        """
        return self._send(wh, payload, write_to_log=False)

    # ── Delivery loop ─────────────────────────────────────────────

    def _ensure_thread(self) -> None:
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._deliver_loop, daemon=True, name="taskito-webhooks"
                )
                self._thread.start()

    def _deliver_loop(self) -> None:
        while True:
            try:
                wh, payload = self._delivery_queue.get(timeout=10)
                self._send(wh, payload)
            except queue.Empty:
                continue
            except Exception:
                logger.exception("Webhook delivery error")

    def _send(
        self, wh: dict[str, Any], payload: dict[str, Any], *, write_to_log: bool = True
    ) -> int | None:
        """Deliver ``payload`` to ``wh`` with retries. Returns the last HTTP
        status code observed (after retries) or ``None`` if every attempt
        failed at the transport level.

        When ``write_to_log`` is true AND the subscription is persisted
        (``wh["subscription_id"]`` is not ``None``), a record of the final
        outcome is appended to the delivery log so the dashboard can
        replay it later.
        """
        body = json.dumps(payload, default=str).encode("utf-8")

        headers: dict[str, str] = {
            "Content-Type": "application/json",
            **wh["headers"],
        }

        if wh["secret"]:
            sig = hmac.new(wh["secret"], body, hashlib.sha256).hexdigest()
            headers["X-Taskito-Signature"] = f"sha256={sig}"

        max_retries: int = wh.get("max_retries", 3)
        timeout: float = wh.get("timeout", 10.0)
        retry_backoff: float = wh.get("retry_backoff", 2.0)

        last_status: int | None = None
        last_response_body: str | None = None
        last_error: str | None = None
        started_at = time.monotonic()
        attempt_count = 0

        for attempt in range(max_retries):
            attempt_count = attempt + 1
            try:
                # Re-validate at delivery time: the URL passed the check when
                # the webhook was registered, but DNS may have been rebound
                # since (or between retries) to a private/loopback/metadata
                # address. A residual micro-race remains between this resolve
                # and the socket connect below, but this closes the wide
                # registration-to-delivery rebinding window.
                validate_webhook_url(wh["url"])
                req = urllib.request.Request(wh["url"], data=body, headers=headers, method="POST")
                with _NO_REDIRECT_OPENER.open(req, timeout=timeout) as resp:
                    last_status = int(resp.status)
                    last_response_body = self._read_response_body(resp)
                    if last_status < 400:
                        self._record(
                            wh,
                            payload,
                            status=DeliveryStatus.DELIVERED,
                            attempts=attempt_count,
                            response_code=last_status,
                            response_body=last_response_body,
                            latency_ms=int((time.monotonic() - started_at) * 1000),
                            write_to_log=write_to_log,
                        )
                        return last_status
                    if write_to_log:
                        logger.warning(
                            "Webhook %s returned server error %d", wh["url"], resp.status
                        )
            except urllib.error.HTTPError as e:
                last_status = e.code
                last_response_body = self._read_response_body(e)
                if e.code < 500:
                    if write_to_log:
                        logger.warning(
                            "Webhook %s returned client error %d, not retrying",
                            wh["url"],
                            e.code,
                        )
                    self._record(
                        wh,
                        payload,
                        status=DeliveryStatus.FAILED,
                        attempts=attempt_count,
                        response_code=last_status,
                        response_body=last_response_body,
                        latency_ms=int((time.monotonic() - started_at) * 1000),
                        write_to_log=write_to_log,
                    )
                    return e.code
                if write_to_log:
                    logger.warning("Webhook %s returned server error %d", wh["url"], e.code)
            except UnsafeWebhookUrl as e:
                # The target now resolves somewhere we won't deliver to.
                # Don't retry — record once and stop.
                last_error = f"blocked unsafe webhook URL: {e}"
                if write_to_log:
                    logger.warning("Webhook %s blocked: %s", wh["url"], e)
                self._record(
                    wh,
                    payload,
                    status=DeliveryStatus.FAILED,
                    attempts=attempt_count,
                    response_code=None,
                    response_body=None,
                    latency_ms=int((time.monotonic() - started_at) * 1000),
                    error=last_error,
                    write_to_log=write_to_log,
                )
                return None
            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
                if write_to_log:
                    logger.debug(
                        "Webhook %s attempt %d failed",
                        wh["url"],
                        attempt + 1,
                        exc_info=True,
                    )
            if attempt == max_retries - 1:
                if write_to_log:
                    logger.warning(
                        "Webhook delivery failed after %d attempts: %s",
                        max_retries,
                        wh["url"],
                    )
            else:
                time.sleep(retry_backoff**attempt)

        # Out of retries — record as dead.
        self._record(
            wh,
            payload,
            status=DeliveryStatus.DEAD,
            attempts=attempt_count,
            response_code=last_status,
            response_body=last_response_body,
            latency_ms=int((time.monotonic() - started_at) * 1000),
            error=last_error,
            write_to_log=write_to_log,
        )
        return last_status

    def _record(
        self,
        wh: dict[str, Any],
        payload: dict[str, Any],
        *,
        status: DeliveryStatus,
        attempts: int,
        response_code: int | None = None,
        response_body: str | None = None,
        latency_ms: int | None = None,
        error: str | None = None,
        write_to_log: bool = True,
    ) -> None:
        """Persist a delivery outcome to the dashboard log."""
        if not write_to_log:
            return
        subscription_id = wh.get("subscription_id")
        if not subscription_id or self._queue is None:
            return
        try:
            DeliveryStore(self._queue).record_attempt(
                subscription_id,
                event=str(payload.get("event", "")),
                payload=payload,
                status=status,
                attempts=attempts,
                response_code=response_code,
                response_body=response_body,
                latency_ms=latency_ms,
                error=error,
                task_name=payload.get("task_name"),
                job_id=payload.get("job_id"),
            )
        except Exception:
            logger.exception("Failed to record webhook delivery")

    @staticmethod
    def _read_response_body(resp: Any) -> str | None:
        """Read up to a few KiB from a response/HTTPError object."""
        try:
            data = resp.read(4096)  # limit even before truncation in DeliveryStore
        except Exception:
            return None
        if not data:
            return None
        return str(data.decode("utf-8", errors="replace"))
