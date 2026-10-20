import type { RetentionWindows } from "./types";

const HOUR_MS = 3_600_000;
const DAY_MS = 24 * HOUR_MS;

/** The server's floor for a dashboard window: anything shorter is refused. */
export const MIN_WINDOW_MS = HOUR_MS;

export type RetentionKey = keyof RetentionWindows;

/** The editor's text per table, in days; an empty string keeps the table forever. */
export type WindowDays = Record<RetentionKey, string>;

export const RETENTION_KEYS: RetentionKey[] = [
  "task_logs_ttl_ms",
  "archived_jobs_ttl_ms",
  "job_errors_ttl_ms",
  "task_metrics_ttl_ms",
  "dead_letter_ttl_ms",
];

/** Windows as editable day counts that parse back to the same milliseconds. */
export function daysFromWindows(windows: RetentionWindows): WindowDays {
  const days = {} as WindowDays;
  for (const key of RETENTION_KEYS) {
    const ms = windows[key];
    days[key] = ms === null ? "" : formatDays(ms);
  }
  return days;
}

/**
 * The fewest decimals that still round-trip, so an untouched window saves
 * unchanged: one hour reads `0.04166667`, not a rounded `0.04` under the floor.
 */
function formatDays(ms: number): string {
  for (let digits = 0; digits < 12; digits++) {
    const text = String(Number((ms / DAY_MS).toFixed(digits)));
    if (Math.round(Number(text) * DAY_MS) === ms) return text;
  }
  return String(ms / DAY_MS);
}

export type ParsedWindows =
  | { ok: true; windows: RetentionWindows }
  | { ok: false; key: RetentionKey; error: string };

/** Day counts back to windows, refusing what the server would refuse. */
export function windowsFromDays(days: WindowDays): ParsedWindows {
  const windows = {} as RetentionWindows;
  for (const key of RETENTION_KEYS) {
    const raw = days[key].trim();
    if (raw === "") {
      windows[key] = null;
      continue;
    }
    const value = Number(raw);
    if (!Number.isFinite(value)) {
      return { ok: false, key, error: "Enter a number of days, or leave empty to keep forever" };
    }
    const ms = Math.round(value * DAY_MS);
    if (ms < MIN_WINDOW_MS) {
      return { ok: false, key, error: "At least one hour (0.05 days)" };
    }
    windows[key] = ms;
  }
  return { ok: true, windows };
}

/**
 * Tables whose history the next sweep would delete sooner under `next` than
 * under `current` — the change worth a confirmation, since it cannot be undone.
 */
export function shortenedTables(current: RetentionWindows, next: RetentionWindows): RetentionKey[] {
  return RETENTION_KEYS.filter((key) => {
    const after = next[key];
    if (after === null) return false;
    const before = current[key];
    return before === null || after < before;
  });
}
