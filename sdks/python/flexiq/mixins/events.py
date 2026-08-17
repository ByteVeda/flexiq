"""Event bus and webhook registration."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from flexiq.dashboard.url_safety import validate_webhook_url
from flexiq.dashboard.webhook_store import WebhookSubscription, WebhookSubscriptionStore

if TYPE_CHECKING:
    from flexiq.events import EventBus, EventType
    from flexiq.webhooks import WebhookManager


class QueueEventsMixin:
    """Event emission, lifecycle event listeners, and webhook registration."""

    _event_bus: EventBus
    _webhook_manager: WebhookManager

    def _emit_event(self, event_type: EventType, payload: dict[str, Any]) -> None:
        """Emit an event to the event bus and webhook manager."""
        self._event_bus.emit(event_type, payload)
        self._webhook_manager.notify(event_type, payload)

    def on_event(self, event_type: EventType, callback: Callable[..., Any]) -> None:
        """Register a callback for a job lifecycle event.

        Args:
            event_type: The event type to listen for.
            callback: Called with ``(event_type, payload_dict)``.
        """
        self._event_bus.on(event_type, callback)

    # ── Webhook subscriptions (persistent) ────────────────────────

    def add_webhook(
        self,
        url: str,
        events: list[EventType] | None = None,
        headers: dict[str, str] | None = None,
        secret: str | None = None,
        max_retries: int = 3,
        timeout: float = 10.0,
        retry_backoff: float = 2.0,
        task_filter: list[str] | None = None,
        description: str | None = None,
    ) -> WebhookSubscription:
        """Register a webhook endpoint for job events.

        Persisted through the dashboard settings store, so the subscription
        survives restarts and is shared across every worker pointed at the
        same backend.

        Args:
            url: URL to POST event payloads to.
            events: Event types to subscribe to (``None`` = all).
            headers: Extra HTTP headers.
            secret: HMAC-SHA256 signing secret. Stored as plaintext; rotate
                via :meth:`rotate_webhook_secret`.
            max_retries: Maximum delivery attempts.
            timeout: HTTP request timeout in seconds.
            retry_backoff: Base for exponential backoff between retries.
            task_filter: When set, deliver only when the event's
                ``task_name`` is in this list.
            description: Free-form label shown in the dashboard.

        Returns:
            The persisted :class:`WebhookSubscription`.
        """
        validate_webhook_url(url)
        store = WebhookSubscriptionStore(self)  # type: ignore[arg-type]
        sub = store.create(
            url=url,
            events=[e.value for e in events] if events else None,
            task_filter=task_filter,
            headers=headers,
            secret=secret,
            max_retries=max_retries,
            timeout_seconds=timeout,
            retry_backoff=retry_backoff,
            description=description,
        )
        self._webhook_manager.reload()
        return sub

    def list_webhooks(self) -> list[WebhookSubscription]:
        """Return every persisted webhook subscription."""
        return WebhookSubscriptionStore(self).list_all()  # type: ignore[arg-type]

    def get_webhook(self, subscription_id: str) -> WebhookSubscription | None:
        return WebhookSubscriptionStore(self).get(subscription_id)  # type: ignore[arg-type]

    def update_webhook(self, subscription_id: str, **changes: Any) -> WebhookSubscription:
        """Patch fields of an existing subscription. Reloads the manager."""
        if "url" in changes:
            validate_webhook_url(changes["url"])
        store = WebhookSubscriptionStore(self)  # type: ignore[arg-type]
        updated = store.update(subscription_id, **changes)
        self._webhook_manager.reload()
        return updated

    def remove_webhook(self, subscription_id: str) -> bool:
        """Delete a subscription. Returns ``True`` if it existed."""
        store = WebhookSubscriptionStore(self)  # type: ignore[arg-type]
        removed = store.delete(subscription_id)
        if removed:
            self._webhook_manager.reload()
        return removed

    def rotate_webhook_secret(self, subscription_id: str) -> str:
        """Generate a fresh signing secret. Returns the new value."""
        store = WebhookSubscriptionStore(self)  # type: ignore[arg-type]
        secret = store.rotate_secret(subscription_id)
        self._webhook_manager.reload()
        return secret
