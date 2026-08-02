import type { EventName } from "../events";
import { type SettingsStore, updateSetting } from "../settingsKv";
import { createLogger } from "../utils";
import type { Webhook } from "./types";

/**
 * Cross-SDK contract: every runtime persists subscriptions as ONE JSON array
 * under this key, with snake_case fields, Unix-ms timestamps and the timeout in
 * seconds. See `crates/taskito-core/BINDING_CONTRACT.md`.
 */
const SUBSCRIPTIONS_KEY = "webhooks:subscriptions";

/** Pre-contract layout this SDK wrote: one camelCase setting per webhook. */
const LEGACY_PREFIX = "webhook:";

const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_TIMEOUT_MS = 10_000;
const DEFAULT_RETRY_BACKOFF = 2;

const log = createLogger("webhooks");

/** The persisted row. Field names and units are the cross-SDK contract. */
interface WebhookRow {
  id: string;
  url: string;
  events: string[];
  task_filter: string[] | null;
  headers: Record<string, string>;
  secret: string | null;
  max_retries: number;
  timeout_seconds: number;
  retry_backoff: number;
  enabled: boolean;
  description: string | null;
  created_at: number;
  updated_at: number;
}

const ROW_FIELDS = new Set<string>([
  "id",
  "url",
  "events",
  "task_filter",
  "headers",
  "secret",
  "max_retries",
  "timeout_seconds",
  "retry_backoff",
  "enabled",
  "description",
  "created_at",
  "updated_at",
]);

function encode(webhook: Webhook): WebhookRow {
  return {
    id: webhook.id,
    url: webhook.url,
    events: webhook.events,
    task_filter: webhook.taskFilter ?? null,
    headers: webhook.headers,
    secret: webhook.secret ?? null,
    max_retries: webhook.maxRetries,
    timeout_seconds: webhook.timeoutMs / 1000,
    retry_backoff: webhook.retryBackoff,
    enabled: webhook.enabled,
    description: webhook.description ?? null,
    created_at: webhook.createdAt,
    updated_at: webhook.updatedAt,
  };
}

function num(value: unknown, fallback: number): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function text(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

/** Decode a row written by any runtime. Returns undefined for unusable rows. */
function decode(row: unknown): Webhook | undefined {
  if (!row || typeof row !== "object") {
    return undefined;
  }
  const raw = row as Record<string, unknown>;
  const id = text(raw.id);
  const url = text(raw.url);
  if (!id || !url) {
    return undefined;
  }
  return {
    id,
    url,
    // Event names another runtime knows about are kept verbatim: an unknown
    // name simply never matches anything this runtime emits.
    events: Array.isArray(raw.events) ? (raw.events as EventName[]) : [],
    taskFilter: Array.isArray(raw.task_filter) ? (raw.task_filter as string[]) : undefined,
    headers:
      raw.headers && typeof raw.headers === "object" ? (raw.headers as Record<string, string>) : {},
    secret: text(raw.secret),
    maxRetries: num(raw.max_retries, DEFAULT_MAX_RETRIES),
    timeoutMs: num(raw.timeout_seconds, DEFAULT_TIMEOUT_MS / 1000) * 1000,
    retryBackoff: num(raw.retry_backoff, DEFAULT_RETRY_BACKOFF),
    enabled: raw.enabled !== false,
    description: text(raw.description),
    createdAt: num(raw.created_at, 0),
    updatedAt: num(raw.updated_at, 0),
  };
}

/** Decode a legacy per-key record, which stored the camelCase runtime shape. */
function decodeLegacy(raw: string): Webhook | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return undefined;
  }
  if (!parsed || typeof parsed !== "object") {
    return undefined;
  }
  const value = parsed as Record<string, unknown>;
  const id = text(value.id);
  const url = text(value.url);
  if (!id || !url) {
    return undefined;
  }
  return {
    id,
    url,
    events: Array.isArray(value.events) ? (value.events as EventName[]) : [],
    taskFilter: Array.isArray(value.taskFilter) ? (value.taskFilter as string[]) : undefined,
    headers:
      value.headers && typeof value.headers === "object"
        ? (value.headers as Record<string, string>)
        : {},
    secret: text(value.secret),
    maxRetries: num(value.maxRetries, DEFAULT_MAX_RETRIES),
    timeoutMs: num(value.timeoutMs, DEFAULT_TIMEOUT_MS),
    retryBackoff: num(value.retryBackoff, DEFAULT_RETRY_BACKOFF),
    enabled: value.enabled !== false,
    description: text(value.description),
    createdAt: num(value.createdAt, 0),
    updatedAt: num(value.updatedAt, 0),
  };
}

/** The stored rows, still raw: a row this build cannot decode stays in the list. */
function decodeRows(raw: string | null): Record<string, unknown>[] {
  if (!raw) {
    return [];
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    log.warn(() => `${SUBSCRIPTIONS_KEY} is not valid JSON; treating as empty`);
    return [];
  }
  if (!Array.isArray(parsed)) {
    return [];
  }
  return parsed.filter(
    (row): row is Record<string, unknown> =>
      !!row && typeof row === "object" && !Array.isArray(row),
  );
}

/** The fields of a stored row this runtime does not model. */
function unmodelledFields(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(row).filter(([field]) => !ROW_FIELDS.has(field)));
}

/** Persists webhook subscriptions in the shared key/value store. */
export class WebhookStore {
  /** Legacy records are folded in once per process, on first read. */
  private legacyMerged = false;
  /**
   * Fields of the last-read rows this runtime does not model, by webhook id.
   * Every write rewrites the whole list, so without this a hook configured by
   * another runtime would silently lose whatever it added.
   */
  private readonly unmodelled = new Map<string, Record<string, unknown>>();

  constructor(private readonly native: SettingsStore) {}

  list(): Webhook[] {
    return this.load();
  }

  get(id: string): Webhook | undefined {
    return this.load().find((webhook) => webhook.id === id);
  }

  /** Insert or replace a subscription, preserving list order. */
  put(webhook: Webhook): void {
    this.load(); // folds in any legacy records before the write
    const encoded = encode(webhook);
    this.updateRows((rows) => {
      const index = rows.findIndex((row) => text(row.id) === webhook.id);
      const existing = rows[index];
      if (existing === undefined) {
        rows.push({ ...this.unmodelled.get(webhook.id), ...encoded });
      } else {
        // Fields this runtime does not model are carried over from the row as
        // it stands *now*, not from the snapshot this call started with.
        rows[index] = { ...unmodelledFields(existing), ...encoded };
      }
    });
  }

  delete(id: string): boolean {
    this.load();
    return this.updateRows((rows) => {
      const index = rows.findIndex((row) => text(row.id) === id);
      if (index === -1) {
        return false;
      }
      rows.splice(index, 1);
      return true;
    });
  }

  private load(): Webhook[] {
    const webhooks = this.readCanonical();
    return this.legacyMerged ? webhooks : this.mergeLegacy(webhooks);
  }

  private readCanonical(): Webhook[] {
    const webhooks: Webhook[] = [];
    for (const row of decodeRows(this.native.getSetting(SUBSCRIPTIONS_KEY))) {
      const webhook = decode(row);
      if (!webhook) {
        continue;
      }
      this.rememberUnmodelled(webhook.id, row);
      webhooks.push(webhook);
    }
    return webhooks;
  }

  private rememberUnmodelled(id: string, row: Record<string, unknown>): void {
    const extra = unmodelledFields(row);
    if (Object.keys(extra).length > 0) {
      this.unmodelled.set(id, extra);
    } else {
      this.unmodelled.delete(id);
    }
  }

  /**
   * Apply `mutate` to the stored rows without losing a concurrent edit.
   *
   * Edited as raw rows rather than decoded webhooks: the whole list lives under
   * one key, so a read-then-write would drop another replica's subscription,
   * and staying raw means a row this build cannot decode survives the write.
   */
  private updateRows<Outcome>(mutate: (rows: Record<string, unknown>[]) => Outcome): Outcome {
    return updateSetting(this.native, SUBSCRIPTIONS_KEY, decodeRows, mutate);
  }

  private save(webhooks: Webhook[]): void {
    const rows = webhooks.map((webhook) => ({
      ...this.unmodelled.get(webhook.id),
      ...encode(webhook),
    }));
    this.native.setSetting(SUBSCRIPTIONS_KEY, JSON.stringify(rows));
  }

  /**
   * Fold records written under the pre-contract per-webhook keys into the
   * canonical list and drop them. A canonical row wins on id collision — it is
   * the one every runtime can already see.
   */
  private mergeLegacy(webhooks: Webhook[]): Webhook[] {
    this.legacyMerged = true;
    const legacyKeys = Object.entries(this.native.listSettings()).filter(([key]) =>
      key.startsWith(LEGACY_PREFIX),
    );
    if (legacyKeys.length === 0) {
      return webhooks;
    }
    const known = new Set(webhooks.map((webhook) => webhook.id));
    const merged = [...webhooks];
    let migrated = 0;
    for (const [, value] of legacyKeys) {
      const webhook = decodeLegacy(value);
      if (webhook && !known.has(webhook.id)) {
        known.add(webhook.id);
        merged.push(webhook);
        migrated += 1;
      }
    }
    this.save(merged);
    for (const [key] of legacyKeys) {
      this.native.deleteSetting(key);
    }
    // Legacy keys are dropped either way; a record already present under its
    // canonical id (or undecodable) is retired, not re-merged — so report the
    // number actually folded in, not the number of keys seen.
    log.info(
      () =>
        `migrated ${migrated} of ${legacyKeys.length} webhook record(s) to ${SUBSCRIPTIONS_KEY}`,
    );
    return merged;
  }
}
