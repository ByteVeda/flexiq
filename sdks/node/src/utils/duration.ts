// The SDK's one answer to "how do I write a duration". Debounce windows
// established the grammar; step sleeps reuse it rather than inventing a second,
// so `"1h"` means the same thing wherever it is written.

import { QueueError } from "../errors";
import type { Duration } from "../types";

const UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

const DURATION_PATTERN = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/;

/** Milliseconds for a {@link Duration} — a raw number passes through as ms. */
export function parseDuration(value: Duration, context: string, field: string): number {
  let milliseconds: number;
  if (typeof value === "number") {
    milliseconds = value;
  } else {
    const match = DURATION_PATTERN.exec(value);
    if (match === null) {
      throw new QueueError(
        `${context}: ${field} "${value}" is not a duration — expected a number of ` +
          'milliseconds or a string like "500ms", "30s", "5m", "2h", "1d"',
      );
    }
    // Both groups exist whenever the pattern matched; the casts keep
    // `noUncheckedIndexedAccess` honest without a runtime branch.
    milliseconds = Number(match[1] as string) * (UNIT_MS[match[2] as string] as number);
  }
  // Checked after the unit multiply, not just on the number path: enough digits
  // overflow to Infinity, which the native i64 boundary turns into 0 — the core
  // then rejects a window the caller never wrote.
  if (!Number.isFinite(milliseconds)) {
    throw new QueueError(`${context}: ${field} must be a finite number of milliseconds`);
  }
  return Math.round(milliseconds);
}
