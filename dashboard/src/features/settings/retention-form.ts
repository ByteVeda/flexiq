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

/** Windows as editable day counts — at most two decimals, so hours survive. */
export function daysFromWindows(windows: RetentionWindows): WindowDays {
  const days = {} as WindowDays;
  for (const key of RETENTION_KEYS) {
    const ms = windows[key];
    days[key] = ms === null ? "" : String(Math.round((ms / DAY_MS) * 100) / 100);
  }
  return days;
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
