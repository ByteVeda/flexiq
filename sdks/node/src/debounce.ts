// Debounce: collapse a burst of enqueues for one key into a single run whose
// deadline slides forward with each call. The core owns the storage-side rule
// (`min(now + window, firstSeen + maxWait)`, applied in one transaction); this
// module owns the shell-side one — validating the options once and turning the
// key template into a concrete key per call.

import { QueueError } from "./errors";
import type { EnqueueOptions as NativeEnqueueOptions } from "./native";
import type { DebounceInput, Duration } from "./types";

/** The debounce fields of the native enqueue options, key already resolved. */
export type NativeDebounce = Pick<
  NativeEnqueueOptions,
  "debounceKey" | "debounceWindowMs" | "debounceMaxWaitMs" | "debounceReplacePayload"
>;

const UNIT_MS: Record<string, number> = {
  ms: 1,
  s: 1_000,
  m: 60_000,
  h: 3_600_000,
  d: 86_400_000,
};

const DURATION_PATTERN = /^(\d+(?:\.\d+)?)(ms|s|m|h|d)$/;
const PLACEHOLDER_PATTERN = /\{([^{}]*)\}/g;

/** Milliseconds for a {@link Duration} — a raw number passes through as ms. */
function parseDuration(value: Duration, context: string, field: string): number {
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new QueueError(`${context}: ${field} must be a finite number of milliseconds`);
    }
    return Math.round(value);
  }
  const match = DURATION_PATTERN.exec(value);
  if (match === null) {
    throw new QueueError(
      `${context}: ${field} "${value}" is not a duration — expected a number of ` +
        'milliseconds or a string like "500ms", "30s", "5m", "2h", "1d"',
    );
  }
  // Both groups exist whenever the pattern matched; the casts keep
  // `noUncheckedIndexedAccess` honest without a runtime branch.
  return Math.round(Number(match[1] as string) * (UNIT_MS[match[2] as string] as number));
}

/** Whether any debounce field is set — the signal that an enqueue overrides
 *  the task's registered debounce rather than inheriting it. */
export function hasDebounceInput(input: DebounceInput): boolean {
  return (
    input.debounce !== undefined ||
    input.debounceKey !== undefined ||
    input.debounceMaxWait !== undefined ||
    input.debounceReplacePayload !== undefined
  );
}

/** Layer an enqueue's debounce fields over the task's registered defaults,
 *  field by field — the same merge `maxRetries`/`timeoutMs` already get. */
export function mergeDebounceInput(
  defaults: DebounceInput | undefined,
  override: DebounceInput,
): DebounceInput {
  return {
    debounce: override.debounce ?? defaults?.debounce,
    debounceKey: override.debounceKey ?? defaults?.debounceKey,
    debounceMaxWait: override.debounceMaxWait ?? defaults?.debounceMaxWait,
    debounceReplacePayload: override.debounceReplacePayload ?? defaults?.debounceReplacePayload,
  };
}

/** A plain object whose own properties a `{name}` placeholder may name. */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Render a placeholder's value, rejecting anything that would not survive as
 *  a key. An object stringifies to `[object Object]`, which would silently
 *  collapse every distinct call into one window — the opposite of the
 *  payload-derived key debounce needs. */
function renderPlaceholder(value: unknown, name: string, context: string): string {
  switch (typeof value) {
    case "string":
      return value;
    case "number":
      if (!Number.isFinite(value)) break;
      return String(value);
    case "boolean":
    case "bigint":
      return String(value);
    default:
      break;
  }
  throw new QueueError(
    `${context}: debounceKey placeholder "{${name}}" resolved to ${
      value === null ? "null" : typeof value
    } — only strings, finite numbers, booleans and bigints can key a debounce window`,
  );
}

/**
 * A task's validated debounce policy. Built once at registration (or once per
 * enqueue that overrides it) so the durations are parsed and the rules checked
 * before any job exists — an unbounded debounce starves, and a key that cannot
 * be resolved is a global window nobody asked for.
 */
export class DebounceOptions {
  private constructor(
    /** Collapse window in milliseconds; each enqueue pushes the run this far out. */
    readonly windowMs: number,
    /** The unresolved `debounceKey`, e.g. `"report:{userId}"`. */
    readonly keyTemplate: string,
    /** Ceiling on the total delay, measured from when the window opened. */
    readonly maxWaitMs: number,
    /** Whether a repeat enqueue overwrites the pending job's payload. */
    readonly replacePayload: boolean,
  ) {}

  /**
   * Validate and normalize one set of debounce fields. `undefined` when none
   * are set. `context` names the task in every error message.
   */
  static from(context: string, input: DebounceInput): DebounceOptions | undefined {
    if (input.debounce === undefined) {
      if (hasDebounceInput(input)) {
        throw new QueueError(
          `${context}: debounceKey, debounceMaxWait and debounceReplacePayload require debounce`,
        );
      }
      return undefined;
    }
    const windowMs = parseDuration(input.debounce, context, "debounce");
    if (windowMs <= 0) {
      throw new QueueError(`${context}: debounce must be positive, got ${windowMs}ms`);
    }
    if (input.debounceKey === undefined || input.debounceKey === "") {
      throw new QueueError(
        `${context}: debounce requires a debounceKey — one window per task would collapse ` +
          "every caller's work into one run, so the key is what scopes it",
      );
    }
    if (input.debounceMaxWait === undefined) {
      throw new QueueError(
        `${context}: debounce requires debounceMaxWait — without a ceiling a caller who ` +
          "keeps enqueuing starves the job forever",
      );
    }
    const maxWaitMs = parseDuration(input.debounceMaxWait, context, "debounceMaxWait");
    if (maxWaitMs < windowMs) {
      throw new QueueError(
        `${context}: debounceMaxWait (${maxWaitMs}ms) must be at least debounce (${windowMs}ms)`,
      );
    }
    return new DebounceOptions(
      windowMs,
      input.debounceKey,
      maxWaitMs,
      input.debounceReplacePayload ?? false,
    );
  }

  /**
   * Substitute the template's placeholders against one call's positional args.
   * `{name}` reads that own property off the first arg that is a plain object
   * carrying it; `{0}`, `{1}`, … read a positional arg directly. A placeholder
   * that resolves to nothing throws — a silently global key is the footgun
   * debounce exists to avoid.
   */
  resolveKey(context: string, args: readonly unknown[]): string {
    return this.keyTemplate.replace(PLACEHOLDER_PATTERN, (_match, name: string) => {
      if (name === "") {
        throw new QueueError(`${context}: debounceKey "${this.keyTemplate}" has an empty {}`);
      }
      if (/^\d+$/.test(name)) {
        const index = Number(name);
        if (index >= args.length) {
          throw new QueueError(
            `${context}: debounceKey placeholder "{${name}}" is out of range — the call ` +
              `passed ${args.length} argument(s)`,
          );
        }
        return renderPlaceholder(args[index], name, context);
      }
      const carrier = args.find((arg) => isPlainObject(arg) && name in arg);
      if (carrier === undefined) {
        throw new QueueError(
          `${context}: debounceKey placeholder "{${name}}" matches no argument — name a ` +
            "property of an object argument, or an argument position like {0}",
        );
      }
      return renderPlaceholder((carrier as Record<string, unknown>)[name], name, context);
    });
  }

  /** The native enqueue fields for one call, key already resolved. */
  toNative(context: string, args: readonly unknown[]): NativeDebounce {
    return {
      debounceKey: this.resolveKey(context, args),
      debounceWindowMs: this.windowMs,
      debounceMaxWaitMs: this.maxWaitMs,
      debounceReplacePayload: this.replacePayload,
    };
  }
}
