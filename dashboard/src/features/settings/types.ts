/**
 * Dashboard settings types.
 *
 * Settings are stored server-side as a flat key→string map. The UI groups
 * them by category and JSON-encodes structured values (lists, integration
 * blobs) before persisting; the server stores them as opaque strings.
 *
 * See `py_src/flexiq/dashboard/handlers/settings.py` for the REST API.
 */

/** Settings keys used by the dashboard UI. Keep them in one place so the
 * UI and any server-side defaults stay in sync. */
export const SETTING_KEYS = {
  brandTitle: "dashboard.brand.title",
  brandAccent: "dashboard.brand.accent",
  externalLinks: "dashboard.external_links",
  integrationGrafana: "dashboard.integrations.grafana_url",
  integrationSentry: "dashboard.integrations.sentry_url",
  integrationOtel: "dashboard.integrations.otel_url",
} as const;

export type SettingKey = (typeof SETTING_KEYS)[keyof typeof SETTING_KEYS];

/** A user-defined external link rendered in the sidebar. */
export interface ExternalLink {
  label: string;
  url: string;
}

/** Single-URL integration shortcuts surfaced on relevant pages. */
export interface IntegrationUrls {
  grafana: string;
  sentry: string;
  otel: string;
}

/** Branding overrides — empty strings mean "use the default". */
export interface BrandOverrides {
  title: string;
  accent: string;
}

/** Raw key→value snapshot returned by ``GET /api/settings``. */
export type SettingsSnapshot = Record<string, string>;

/** Per-table retention windows in milliseconds; `null` keeps a table forever. */
export interface RetentionWindows {
  task_logs_ttl_ms: number | null;
  archived_jobs_ttl_ms: number | null;
  job_errors_ttl_ms: number | null;
  task_metrics_ttl_ms: number | null;
  dead_letter_ttl_ms: number | null;
}

/**
 * What ``GET /api/retention`` reports: the windows the elected cleaner
 * published, not this process's config. ``reported: false`` means no worker
 * has swept yet — distinct from retention being off (``enabled: false``).
 */
export interface RetentionSnapshot {
  reported: boolean;
  enabled: boolean;
  defaulted: boolean;
  namespace: string | null;
  reported_at: number | null;
  windows: RetentionWindows;
}

/** Rows a purge would remove per table, from ``GET /api/retention/dry-run``. */
export interface RetentionCounts {
  task_logs: number;
  archived_jobs: number;
  job_errors: number;
  task_metrics: number;
  dead_letter: number;
}

/**
 * What ``GET /api/retention/dry-run`` reports: how many rows a purge would
 * remove right now under the previewed windows, without deleting. Computed
 * in-process, so it always answers — never the unreported state ``/api/retention``
 * can return.
 */
export interface RetentionPreview {
  enabled: boolean;
  defaulted: boolean;
  namespace: string;
  reference_time: number;
  windows: RetentionWindows;
  counts: RetentionCounts;
  total: number;
}
