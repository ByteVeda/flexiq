// Retention route handlers. Echoes the windows the elected cleaner published
// for this namespace, so the dashboard can explain why rows disappear from its
// listings, and stores the dashboard's override of the worker's config.
// Retention runs in the worker process, so the echo is never computed from
// local config — an unreported policy is reported as such.

import type { Queue } from "../../index";
import { windowsToContract } from "../../retention";
import type { EffectiveRetention, RetentionOverride, RetentionSource } from "../../types";
import { BadRequestError } from "../errors";

const NULL_WINDOWS = {
  task_logs_ttl_ms: null,
  archived_jobs_ttl_ms: null,
  job_errors_ttl_ms: null,
  task_metrics_ttl_ms: null,
  dead_letter_ttl_ms: null,
};

/** The published retention policy for this queue's namespace. */
export function retention(queue: Queue) {
  const snapshot: EffectiveRetention | null = queue.effectiveRetention();
  return {
    // Distinct from `enabled`: no worker has swept yet, so nothing is known
    // about the policy — not the same as retention being switched off.
    reported: snapshot !== null,
    enabled: snapshot?.enabled ?? false,
    defaulted: snapshot?.defaulted ?? false,
    namespace: snapshot?.namespace ?? null,
    reported_at: snapshot?.reportedAt ?? null,
    windows: snapshot ? windowsToContract(snapshot.windows) : NULL_WINDOWS,
    source: snapshot?.source ?? "code",
    code_windows: snapshot?.codeWindows ? windowsToContract(snapshot.codeWindows) : null,
  };
}

/** Preview what a purge would delete under the policy the next sweep applies,
 *  computed in-process, so it always answers. */
export async function retentionDryRun(queue: Queue) {
  const preview = await queue.dryRunRetention();
  return {
    enabled: preview.enabled,
    defaulted: preview.defaulted,
    namespace: preview.namespace,
    reference_time: preview.referenceTime,
    windows: windowsToContract(preview.windows),
    counts: {
      task_logs: preview.counts.taskLogs,
      archived_jobs: preview.counts.archivedJobs,
      job_errors: preview.counts.jobErrors,
      task_metrics: preview.counts.taskMetrics,
      dead_letter: preview.counts.deadLetter,
    },
    total: preview.total,
  };
}

function overrideToContract(document: RetentionOverride) {
  return {
    source: document.source,
    windows: windowsToContract(document.windows),
    updated_at: document.updatedAt,
  };
}

/** The stored override, or the `code` default when none was saved. */
export function getRetentionOverride(queue: Queue) {
  return overrideToContract(queue.retentionOverride());
}

const SOURCES: readonly RetentionSource[] = ["code", "dashboard"];
const TABLES = {
  task_logs_ttl_ms: "taskLogs",
  archived_jobs_ttl_ms: "archivedJobs",
  job_errors_ttl_ms: "jobErrors",
  task_metrics_ttl_ms: "taskMetrics",
  dead_letter_ttl_ms: "deadLetter",
} as const;

/**
 * Store the dashboard's windows and which source applies. The body is checked
 * here only for shape; the windows themselves are validated by the core, whose
 * refusal is answered as a 400 like this one.
 */
export function putRetentionOverride(queue: Queue, body: unknown) {
  const doc = (body ?? {}) as { source?: unknown; windows?: Record<string, unknown> };
  if (!SOURCES.includes(doc.source as RetentionSource)) {
    throw new BadRequestError("source must be 'code' or 'dashboard'");
  }
  const windows: Record<string, number | null> = {};
  for (const [field, value] of Object.entries(doc.windows ?? {})) {
    const key = TABLES[field as keyof typeof TABLES];
    if (!key) throw new BadRequestError(`unknown retention table '${field}'`);
    if (value !== null && typeof value !== "number") {
      throw new BadRequestError(`${field} must be a number of milliseconds or null`);
    }
    windows[key] = value;
  }
  try {
    return overrideToContract(
      queue.setRetentionOverride({ source: doc.source as RetentionSource, windows }),
    );
  } catch (e) {
    // The core's own refusal of the windows, raised before anything is written.
    if ((e as { code?: string }).code === "InvalidArg") {
      throw new BadRequestError((e as Error).message);
    }
    throw e;
  }
}

/** Forget the override; the worker's config applies from the next sweep. */
export function deleteRetentionOverride(queue: Queue) {
  return { deleted: queue.clearRetentionOverride() };
}
