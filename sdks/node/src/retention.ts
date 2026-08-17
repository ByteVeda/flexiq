// Parses the retention document the elected cleaner publishes. Retention runs
// in the worker process, so this is how any other process — a dashboard, an
// admin script — learns the policy that governs the deletes.
// See `crates/flexiq-core/BINDING_CONTRACT.md`.

import type { EffectiveRetention, RetentionPreview } from "./types";

/** Core's window field for a table, in milliseconds. `null` = keep forever. */
type PublishedWindows = Record<string, number | null | undefined>;

const window = (windows: PublishedWindows, table: string): number | null =>
  windows[`${table}_ttl_ms`] ?? null;

/** Parse the published JSON document into the SDK's camelCase shape. */
export function parseEffectiveRetention(raw: string): EffectiveRetention {
  const doc = JSON.parse(raw) as {
    enabled?: boolean;
    defaulted?: boolean;
    namespace?: string;
    reported_at?: number;
    windows?: PublishedWindows;
  };
  const windows = doc.windows ?? {};
  return {
    enabled: Boolean(doc.enabled),
    defaulted: Boolean(doc.defaulted),
    namespace: doc.namespace ?? "default",
    reportedAt: doc.reported_at ?? 0,
    windows: {
      archivedJobs: window(windows, "archived_jobs"),
      deadLetter: window(windows, "dead_letter"),
      taskLogs: window(windows, "task_logs"),
      taskMetrics: window(windows, "task_metrics"),
      jobErrors: window(windows, "job_errors"),
    },
  };
}

/** Parse the dry-run preview document into the SDK's camelCase shape. */
export function parseRetentionPreview(raw: string): RetentionPreview {
  const doc = JSON.parse(raw) as {
    enabled?: boolean;
    defaulted?: boolean;
    namespace?: string;
    reference_time?: number;
    windows?: PublishedWindows;
    counts?: Record<string, number | undefined>;
    total?: number;
  };
  const windows = doc.windows ?? {};
  const counts = doc.counts ?? {};
  const count = (table: string): number => counts[table] ?? 0;
  return {
    enabled: Boolean(doc.enabled),
    defaulted: Boolean(doc.defaulted),
    namespace: doc.namespace ?? "default",
    referenceTime: doc.reference_time ?? 0,
    windows: {
      archivedJobs: window(windows, "archived_jobs"),
      deadLetter: window(windows, "dead_letter"),
      taskLogs: window(windows, "task_logs"),
      taskMetrics: window(windows, "task_metrics"),
      jobErrors: window(windows, "job_errors"),
    },
    counts: {
      archivedJobs: count("archived_jobs"),
      deadLetter: count("dead_letter"),
      taskLogs: count("task_logs"),
      taskMetrics: count("task_metrics"),
      jobErrors: count("job_errors"),
    },
    total: doc.total ?? 0,
  };
}
