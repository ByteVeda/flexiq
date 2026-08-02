// Persistent task & queue runtime overrides. Operators tune task/queue
// behaviour from the dashboard; decorator/registration values stay the
// defaults and any override recorded here wins. Storage layout follows the
// cross-SDK contract:
//
// - `overrides:task:<task_name>` — JSON of overridden fields for that task
// - `overrides:queue:<queue_name>` — JSON of overridden fields for that queue
//
// Overrides are applied at worker startup; changes do not affect a running
// worker until it restarts (queue `paused` propagates live via pause/resume).

import type { TaskConfigInput } from "../../native";
import { type SettingsStore, updateSetting } from "../../settingsKv";
import { createLogger } from "../../utils";
import { ValidationError } from "../errors";

const TASK_PREFIX = "overrides:task:";
const QUEUE_PREFIX = "overrides:queue:";

const log = createLogger("dashboard");

/** The settings-KV surface both `Queue` and the native handle satisfy. */
export type SettingsAccess = SettingsStore;

/** Fields an operator may override per task (cross-SDK field names). */
export const TASK_OVERRIDE_FIELDS = new Set([
  "rate_limit",
  "max_concurrent",
  "max_retries",
  "retry_backoff",
  "timeout",
  "priority",
  "paused",
]);

/** Fields an operator may override per queue. */
export const QUEUE_OVERRIDE_FIELDS = new Set(["rate_limit", "max_concurrent", "paused"]);

/** An operator-set task override (snake_case, matching the wire contract). */
export interface TaskOverride {
  task_name: string;
  rate_limit: string | null;
  max_concurrent: number | null;
  max_retries: number | null;
  retry_backoff: number | null;
  timeout: number | null;
  priority: number | null;
  paused: boolean;
  updated_at: number;
}

/** An operator-set queue override. */
export interface QueueOverride {
  queue_name: string;
  rate_limit: string | null;
  max_concurrent: number | null;
  paused: boolean;
  updated_at: number;
}

// ── Validation ──────────────────────────────────────────────────────────

function validateCommon(fields: Record<string, unknown>): void {
  const rateLimit = fields.rate_limit;
  if (rateLimit !== undefined && rateLimit !== null) {
    if (typeof rateLimit !== "string" || !rateLimit) {
      throw new ValidationError("rate_limit must be a non-empty string like '100/m'");
    }
    // Cheap shape check; rate-limit parsing happens in the core.
    if (!rateLimit.includes("/")) {
      throw new ValidationError("rate_limit must contain a unit, e.g. '10/s', '100/m', '3600/h'");
    }
  }
  validateInt(fields, "max_concurrent", 0);
  validateBool(fields, "paused");
}

function validateTaskFields(fields: Record<string, unknown>): void {
  const unknown = Object.keys(fields).filter((k) => !TASK_OVERRIDE_FIELDS.has(k));
  if (unknown.length > 0) {
    throw new ValidationError(`unknown task override fields: ${unknown.sort().join(", ")}`);
  }
  validateCommon(fields);
  validateInt(fields, "max_retries", 0);
  validateNumber(fields, "retry_backoff", 0);
  validateInt(fields, "timeout", 1);
  validateInt(fields, "priority");
}

function validateQueueFields(fields: Record<string, unknown>): void {
  const unknown = Object.keys(fields).filter((k) => !QUEUE_OVERRIDE_FIELDS.has(k));
  if (unknown.length > 0) {
    throw new ValidationError(`unknown queue override fields: ${unknown.sort().join(", ")}`);
  }
  validateCommon(fields);
}

function validateInt(fields: Record<string, unknown>, name: string, minimum?: number): void {
  const value = fields[name];
  if (value === undefined || value === null) {
    return;
  }
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new ValidationError(`${name} must be an integer`);
  }
  if (minimum !== undefined && value < minimum) {
    throw new ValidationError(`${name} must be >= ${minimum}`);
  }
}

function validateNumber(fields: Record<string, unknown>, name: string, minimum?: number): void {
  const value = fields[name];
  if (value === undefined || value === null) {
    return;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new ValidationError(`${name} must be a number`);
  }
  if (minimum !== undefined && value < minimum) {
    throw new ValidationError(`${name} must be >= ${minimum}`);
  }
}

function validateBool(fields: Record<string, unknown>, name: string): void {
  const value = fields[name];
  if (value !== undefined && value !== null && typeof value !== "boolean") {
    throw new ValidationError(`${name} must be a boolean`);
  }
}

// ── Store ───────────────────────────────────────────────────────────────

function parseJsonObject(raw: string | null): Record<string, unknown> {
  if (!raw) {
    return {};
  }
  try {
    const data = JSON.parse(raw);
    return data && typeof data === "object" && !Array.isArray(data) ? data : {};
  } catch {
    log.warn(() => "overrides entry is not valid JSON; treating as empty");
    return {};
  }
}

/** CRUD for per-task and per-queue runtime overrides. */
export class OverridesStore {
  constructor(private readonly settings: SettingsAccess) {}

  // ── Tasks ─────────────────────────────────────────────────────────

  listTasks(): Map<string, TaskOverride> {
    const out = new Map<string, TaskOverride>();
    for (const [key, raw] of Object.entries(this.settings.listSettings())) {
      if (key.startsWith(TASK_PREFIX)) {
        const name = key.slice(TASK_PREFIX.length);
        out.set(name, rowToTask(name, parseJsonObject(raw)));
      }
    }
    return out;
  }

  getTask(taskName: string): TaskOverride | undefined {
    const raw = this.settings.getSetting(TASK_PREFIX + taskName);
    return raw ? rowToTask(taskName, parseJsonObject(raw)) : undefined;
  }

  /** Merge `fields` into the stored override; `null` values clear a field. */
  setTask(taskName: string, fields: Record<string, unknown>): TaskOverride {
    validateTaskFields(fields);
    if (!taskName) {
      throw new ValidationError("task_name must not be empty");
    }
    return rowToTask(taskName, this.mergeRow(TASK_PREFIX + taskName, fields));
  }

  clearTask(taskName: string): boolean {
    return this.settings.deleteSetting(TASK_PREFIX + taskName);
  }

  // ── Queues ────────────────────────────────────────────────────────

  listQueues(): Map<string, QueueOverride> {
    const out = new Map<string, QueueOverride>();
    for (const [key, raw] of Object.entries(this.settings.listSettings())) {
      if (key.startsWith(QUEUE_PREFIX)) {
        const name = key.slice(QUEUE_PREFIX.length);
        out.set(name, rowToQueue(name, parseJsonObject(raw)));
      }
    }
    return out;
  }

  getQueue(queueName: string): QueueOverride | undefined {
    const raw = this.settings.getSetting(QUEUE_PREFIX + queueName);
    return raw ? rowToQueue(queueName, parseJsonObject(raw)) : undefined;
  }

  setQueue(queueName: string, fields: Record<string, unknown>): QueueOverride {
    validateQueueFields(fields);
    if (!queueName) {
      throw new ValidationError("queue_name must not be empty");
    }
    return rowToQueue(queueName, this.mergeRow(QUEUE_PREFIX + queueName, fields));
  }

  clearQueue(queueName: string): boolean {
    return this.settings.deleteSetting(QUEUE_PREFIX + queueName);
  }

  /** Patch `fields` into the override at `key` without losing a concurrent edit. */
  private mergeRow(key: string, fields: Record<string, unknown>): Record<string, unknown> {
    return updateSetting(this.settings, key, parseJsonObject, (merged) => {
      delete merged.updated_at;
      for (const [name, value] of Object.entries(fields)) {
        if (value === null || value === undefined) {
          delete merged[name];
        } else {
          merged[name] = value;
        }
      }
      merged.updated_at = Date.now();
      return { ...merged };
    });
  }
}

function rowToTask(taskName: string, row: Record<string, unknown>): TaskOverride {
  return {
    task_name: taskName,
    rate_limit: asStringOrNull(row.rate_limit),
    max_concurrent: asNumberOrNull(row.max_concurrent),
    max_retries: asNumberOrNull(row.max_retries),
    retry_backoff: asNumberOrNull(row.retry_backoff),
    timeout: asNumberOrNull(row.timeout),
    priority: asNumberOrNull(row.priority),
    paused: row.paused === true,
    updated_at: asNumberOrNull(row.updated_at) ?? 0,
  };
}

function rowToQueue(queueName: string, row: Record<string, unknown>): QueueOverride {
  return {
    queue_name: queueName,
    rate_limit: asStringOrNull(row.rate_limit),
    max_concurrent: asNumberOrNull(row.max_concurrent),
    paused: row.paused === true,
    updated_at: asNumberOrNull(row.updated_at) ?? 0,
  };
}

const asStringOrNull = (v: unknown): string | null => (typeof v === "string" && v ? v : null);
const asNumberOrNull = (v: unknown): number | null =>
  typeof v === "number" && Number.isFinite(v) ? v : null;

// ── Worker-startup application ──────────────────────────────────────────

/**
 * Patch worker task configs with stored overrides. Tasks that only exist as
 * an override (registered without options) get a fresh config entry.
 * `retry_backoff` is interpreted as the retry base delay in seconds;
 * `timeout`/`priority` have no worker-config slot here and surface via the
 * dashboard's effective view only.
 */
export function applyTaskOverrides(
  configs: TaskConfigInput[],
  taskNames: Iterable<string>,
  store: OverridesStore,
): TaskConfigInput[] {
  const overrides = store.listTasks();
  if (overrides.size === 0) {
    return configs;
  }
  const byName = new Map(configs.map((config) => [config.name, { ...config }]));
  for (const name of taskNames) {
    const override = overrides.get(name);
    if (!override) {
      continue;
    }
    const config = byName.get(name) ?? { name };
    if (override.rate_limit !== null) {
      config.rateLimit = override.rate_limit;
    }
    if (override.max_concurrent !== null) {
      config.maxConcurrent = override.max_concurrent;
    }
    if (override.max_retries !== null) {
      config.maxRetries = override.max_retries;
    }
    if (override.retry_backoff !== null) {
      config.retryBaseDelayMs = Math.round(override.retry_backoff * 1000);
    }
    byName.set(name, config);
  }
  return [...byName.values()];
}

/** Merge queue overrides into worker queue configs (adding new queues). */
export function applyQueueOverrides(
  configs: Array<{ name: string; maxConcurrent?: number; rateLimit?: string }>,
  store: OverridesStore,
): Array<{ name: string; maxConcurrent?: number; rateLimit?: string }> {
  const overrides = store.listQueues();
  if (overrides.size === 0) {
    return configs;
  }
  const byName = new Map(configs.map((config) => [config.name, { ...config }]));
  for (const [name, override] of overrides) {
    const config = byName.get(name) ?? { name };
    if (override.rate_limit !== null) {
      config.rateLimit = override.rate_limit;
    }
    if (override.max_concurrent !== null) {
      config.maxConcurrent = override.max_concurrent;
    }
    byName.set(name, config);
  }
  return [...byName.values()];
}
