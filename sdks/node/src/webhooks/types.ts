import type { EventName } from "../events";

/** Fields accepted when creating/updating a webhook subscription. */
export interface WebhookInput {
  url: string;
  /** Events to deliver. Empty/omitted = all events. */
  events?: EventName[];
  /** HMAC-SHA256 signing key. Sent as `X-Flexiq-Signature: sha256=...`. */
  secret?: string;
  /** Extra request headers. */
  headers?: Record<string, string>;
  /** Restrict to these task names. Empty/omitted = all tasks. */
  taskFilter?: string[];
  description?: string;
  enabled?: boolean;
  maxRetries?: number;
  timeoutMs?: number;
  /** Retry backoff base, in seconds: the Nth wait (counted from zero) is `retryBackoff ** N`. */
  retryBackoff?: number;
}

/** A stored webhook subscription. */
export interface Webhook {
  id: string;
  url: string;
  events: EventName[];
  secret?: string;
  headers: Record<string, string>;
  taskFilter?: string[];
  description?: string;
  enabled: boolean;
  maxRetries: number;
  timeoutMs: number;
  /** Retry backoff base, in seconds: the Nth wait (counted from zero) is `retryBackoff ** N`. */
  retryBackoff: number;
  createdAt: number;
  updatedAt: number;
}

/** Terminal state of a delivery attempt-chain. */
export type DeliveryStatus = "delivered" | "failed" | "dead";

/** The result of a single delivery attempt-chain. */
export interface Delivery {
  id: string;
  webhookId: string;
  event: EventName;
  /** `delivered` on a 2xx; `dead` once every retry is exhausted. */
  status: DeliveryStatus;
  ok: boolean;
  attempts: number;
  /** The JSON body that was POSTed. */
  payload: Record<string, unknown>;
  taskName: string | null;
  jobId: string | null;
  /** Last HTTP status, or `null` when no request completed (network error). */
  responseCode: number | null;
  /** Last response body (truncated), or `null` when unread. */
  responseBody: string | null;
  latencyMs: number;
  error?: string;
  createdAt: number;
  completedAt: number;
}
