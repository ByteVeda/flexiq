// Sentry error reporting for FlexiQ. Optional integration — import from
// `flexiq/contrib/sentry`; requires `@sentry/node` as a peer (and `Sentry.init(...)`
// called by your app first).
//
// Register with `queue.use(sentryMiddleware())`. The real exception (with its stack) is
// captured from `onError` and reported when the job dead-letters — one event per dead
// job. Set `captureRetries` to also report each intermediate failure as a warning.

import type { SeverityLevel } from "@sentry/node";
import * as Sentry from "@sentry/node";
import type { OutcomeEvent } from "../events";
import type { Middleware } from "../middleware";

/** Options for {@link sentryMiddleware}. */
export interface SentryMiddlewareOptions {
  /** Prefix for Sentry tag keys (default `"flexiq"`). */
  tagPrefix?: string;
  /** Also capture each retried failure as a `"warning"` event (default false). */
  captureRetries?: boolean;
  /** Severity level for the terminal dead-letter capture (default `"error"`). */
  level?: SeverityLevel;
  /** Extra tags merged onto the captured event. */
  extraTags?: (event: OutcomeEvent) => Record<string, string>;
  /** Only report tasks for which this returns true (default: all). */
  taskFilter?: (taskName: string) => boolean;
}

/**
 * Build {@link Middleware} that reports failing tasks to Sentry. The exception captured
 * in `onError` is held per job and sent when the job dead-letters, so each dead job
 * yields a single event carrying the original stack trace plus job/queue tags.
 */
export function sentryMiddleware(options: SentryMiddlewareOptions = {}): Middleware {
  const prefix = options.tagPrefix ?? "flexiq";
  const deadLevel = options.level ?? "error";
  const captureRetries = options.captureRetries ?? false;
  const tracked = (taskName: string): boolean => options.taskFilter?.(taskName) ?? true;

  // The latest real exception per in-flight job, captured in onError (it carries the
  // stack; the outcome events only carry a message string).
  const lastError = new Map<string, unknown>();

  const tagsFor = (event: OutcomeEvent): Record<string, string> => {
    const tags: Record<string, string> = {
      [`${prefix}.task_name`]: event.taskName,
      [`${prefix}.job_id`]: event.jobId,
    };
    if (event.queue) {
      tags[`${prefix}.queue`] = event.queue;
    }
    if (event.retryCount !== undefined) {
      tags[`${prefix}.retry_count`] = String(event.retryCount);
    }
    if (event.timedOut) {
      tags[`${prefix}.timed_out`] = "true";
    }
    return { ...tags, ...options.extraTags?.(event) };
  };

  const capture = (error: unknown, event: OutcomeEvent, level: SeverityLevel): void => {
    Sentry.withScope((scope) => {
      scope.setLevel(level);
      scope.setTags(tagsFor(event));
      Sentry.captureException(error);
    });
  };

  const errorFor = (event: OutcomeEvent, fallback: string): unknown =>
    lastError.get(event.jobId) ?? new Error(event.error ?? fallback);

  return {
    onError(ctx, error) {
      if (tracked(ctx.taskName)) {
        lastError.set(ctx.jobId, error);
      }
    },
    onRetry(event) {
      if (captureRetries && tracked(event.taskName)) {
        capture(errorFor(event, `${event.taskName} failed`), event, "warning");
      }
    },
    onDeadLetter(event) {
      if (tracked(event.taskName)) {
        capture(errorFor(event, `${event.taskName} dead-lettered`), event, deadLevel);
      }
      lastError.delete(event.jobId);
    },
    onCompleted(event) {
      lastError.delete(event.jobId);
    },
    onCancel(event) {
      lastError.delete(event.jobId);
    },
  };
}
