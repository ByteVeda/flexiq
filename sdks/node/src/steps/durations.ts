/**
 * How long a `ctx.step.sleep` sleeps, and until when.
 *
 * Reuses the duration grammar the debounce windows established (`"500ms"`,
 * `"5m"`, `"2h"`; a bare number is milliseconds) so the SDK has one answer to
 * "how do I write a duration", and adds the one form a sleep specifically
 * invites: an absolute `Date` for `sleepUntil`.
 */

import { QueueError } from "../errors";
import type { Duration } from "../types";
import { parseDuration } from "../utils";

/** What a sleep deadline may be written as: a `Date`, or Unix milliseconds. */
export type SleepDeadline = Date | number;

/** Parse a sleep duration into whole milliseconds. */
export function sleepDurationMs(value: Duration): number {
  const millis = parseDuration(value, "step.sleep", "duration");
  if (millis < 0) {
    throw new QueueError(`step.sleep: duration must not be negative, got ${millis}ms`);
  }
  return millis;
}

/** Parse an absolute wake instant into Unix milliseconds. */
export function sleepDeadlineMs(value: SleepDeadline): number {
  const millis = value instanceof Date ? value.getTime() : value;
  if (typeof millis !== "number" || !Number.isFinite(millis)) {
    throw new QueueError(
      "step.sleepUntil: when must be a Date or a Unix timestamp in milliseconds",
    );
  }
  if (millis <= 0) {
    throw new QueueError(`step.sleepUntil: when must be a positive instant, got ${millis}`);
  }
  return Math.round(millis);
}
