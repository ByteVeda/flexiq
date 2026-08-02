// Persistent webhook delivery log. Each subscription keeps a JSON list under
// `webhooks:deliveries:<id>` in the settings store — append-only with FIFO
// eviction at the per-webhook cap, following the cross-SDK row layout
// (snake_case fields, Unix-ms timestamps).

import { type SettingsStore, updateSetting } from "../settingsKv";
import { createLogger } from "../utils";
import type { Delivery, DeliveryStatus } from "./types";

const DELIVERY_PREFIX = "webhooks:deliveries:";
const DEFAULT_MAX_PER_WEBHOOK = 200;

const log = createLogger("webhooks");

/** Persisted snake_case row shape (cross-SDK contract). */
interface DeliveryRow {
  id: string;
  subscription_id: string;
  event: string;
  payload: Record<string, unknown>;
  task_name: string | null;
  job_id: string | null;
  status: string;
  attempts: number;
  response_code: number | null;
  response_body: string | null;
  latency_ms: number | null;
  error: string | null;
  created_at: number;
  completed_at: number | null;
}

export interface DeliveryFilters {
  status?: string;
  event?: string;
  limit?: number;
  offset?: number;
}

/** List/append delivery records keyed by subscription id. */
export class DeliveryLog {
  constructor(
    private readonly native: SettingsStore,
    private readonly maxPerWebhook = DEFAULT_MAX_PER_WEBHOOK,
  ) {}

  private key(subscriptionId: string): string {
    return DELIVERY_PREFIX + subscriptionId;
  }

  private decoder(subscriptionId: string): (raw: string | null) => DeliveryRow[] {
    return (raw) => {
      if (!raw) {
        return [];
      }
      try {
        const data = JSON.parse(raw);
        return Array.isArray(data) ? data : [];
      } catch {
        log.warn(() => `delivery log for ${subscriptionId} is corrupt; resetting`);
        return [];
      }
    };
  }

  private load(subscriptionId: string): DeliveryRow[] {
    return this.decoder(subscriptionId)(this.native.getSetting(this.key(subscriptionId)));
  }

  /**
   * Append a delivery row, trimming to the per-webhook cap.
   *
   * Written conditionally: two deliveries settling at once would otherwise each
   * append to the list they read and the later write would drop the other.
   */
  record(delivery: Delivery): void {
    const row = toRow(delivery);
    updateSetting(
      this.native,
      this.key(delivery.webhookId),
      this.decoder(delivery.webhookId),
      (rows) => {
        rows.push(row);
        if (rows.length > this.maxPerWebhook) {
          rows.splice(0, rows.length - this.maxPerWebhook);
        }
      },
    );
  }

  /** Recent deliveries, newest first, optionally filtered and paginated. */
  listFor(subscriptionId: string, filters: DeliveryFilters = {}): Delivery[] {
    let rows = this.load(subscriptionId).reverse();
    if (filters.status) {
      rows = rows.filter((r) => r.status === filters.status);
    }
    if (filters.event) {
      rows = rows.filter((r) => r.event === filters.event);
    }
    // Clamp so negative values can't trigger Array.slice's negative-index
    // semantics and return the wrong page.
    const offset = Math.max(0, filters.offset ?? 0);
    const limit = Math.max(0, filters.limit ?? 50);
    return rows.slice(offset, offset + limit).map(fromRow);
  }

  get(subscriptionId: string, deliveryId: string): Delivery | undefined {
    const row = this.load(subscriptionId).find((r) => r.id === deliveryId);
    return row ? fromRow(row) : undefined;
  }

  countFor(subscriptionId: string): number {
    return this.load(subscriptionId).length;
  }

  deleteFor(subscriptionId: string): boolean {
    return this.native.deleteSetting(this.key(subscriptionId));
  }
}

function toRow(delivery: Delivery): DeliveryRow {
  return {
    id: delivery.id,
    subscription_id: delivery.webhookId,
    event: delivery.event,
    payload: delivery.payload,
    task_name: delivery.taskName,
    job_id: delivery.jobId,
    status: delivery.status,
    attempts: delivery.attempts,
    response_code: delivery.responseCode,
    response_body: delivery.responseBody,
    latency_ms: delivery.latencyMs,
    error: delivery.error ?? null,
    created_at: delivery.createdAt,
    completed_at: delivery.completedAt,
  };
}

function fromRow(row: DeliveryRow): Delivery {
  return {
    id: String(row.id),
    webhookId: String(row.subscription_id),
    event: row.event as Delivery["event"],
    status: row.status as DeliveryStatus,
    ok: row.status === "delivered",
    attempts: Number(row.attempts) || 0,
    payload: row.payload ?? {},
    taskName: row.task_name ?? null,
    jobId: row.job_id ?? null,
    responseCode: row.response_code ?? null,
    responseBody: row.response_body ?? null,
    latencyMs: Number(row.latency_ms) || 0,
    error: row.error ?? undefined,
    createdAt: Number(row.created_at) || 0,
    completedAt: Number(row.completed_at) || 0,
  };
}
