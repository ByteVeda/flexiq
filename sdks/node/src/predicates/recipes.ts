import { PredicateValidationError } from "../errors";
import { Decision, type EnqueueDecision, type EnqueueGate, type Predicate } from "./decisions";
import {
  envFeatureFlagProvider,
  type FeatureFlagProvider,
  type FlagLookup,
  isTruthy,
  toFlagProvider,
} from "./providers";
import { assertTimeZone, partsInZone, shiftDays, type ZonedParts, zonedInstant } from "./tz";

/**
 * Ready-made gates for common scheduling and filtering policies. Time-based
 * recipes `defer` an out-of-window enqueue to the next open moment; filter
 * recipes `skip` a non-matching one. Every argument is validated when the
 * recipe is built, so a bad time zone or window fails at wiring time rather
 * than on the first enqueue.
 */

const UTC = "UTC";

/** The day names {@link dayOfWeek} accepts. */
export const DAYS_OF_WEEK = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"] as const;

/** One day of the week. Plain strings are accepted at runtime and validated. */
export type DayOfWeek = (typeof DAYS_OF_WEEK)[number];

const WEEKDAYS: readonly DayOfWeek[] = ["mon", "tue", "wed", "thu", "fri"];

/** Shared by every recipe that reads a wall clock. */
export interface ZoneOptions {
  /** IANA zone the policy is expressed in (default `"UTC"`). */
  timeZone?: string;
}

export interface BusinessHoursOptions extends ZoneOptions {
  /** First open hour, 0–23 (default 9). */
  startHour?: number;
  /** First closed hour, 1–24 (default 17). */
  endHour?: number;
  /** Restrict the window to Mon–Fri (default true). */
  weekdaysOnly?: boolean;
}

export interface TimeWindowOptions extends ZoneOptions {
  /** Window start as `"HH:MM"`, inclusive. */
  start: string;
  /** Window end as `"HH:MM"`, exclusive. A value below `start` wraps midnight. */
  end: string;
}

export interface DayOfWeekOptions extends ZoneOptions {
  /** Days the enqueue is allowed on. Empty means never (every enqueue skips). */
  days: readonly DayOfWeek[];
}

/**
 * Allow during business hours in `timeZone`; otherwise defer to the next
 * opening. Defaults to Mon–Fri 09:00–17:00 UTC.
 */
export function businessHours(options: BusinessHoursOptions = {}): EnqueueGate {
  const { timeZone = UTC, startHour = 9, endHour = 17, weekdaysOnly = true } = options;
  assertTimeZone(timeZone);
  assertHours(startHour, endHour);
  const openDays = isoWeekdaysOf(weekdaysOnly ? WEEKDAYS : DAYS_OF_WEEK);
  return (ctx) => {
    const parts = partsInZone(ctx.now, timeZone);
    const openNow =
      openDays.has(parts.isoWeekday) && parts.hour >= startHour && parts.hour < endHour;
    if (openNow) {
      return Decision.allow();
    }
    return deferTo(nextOpening(parts, openDays, startHour, timeZone, ctx.now), ctx.now);
  };
}

/**
 * Allow within a daily `[start, end)` window in `timeZone`; otherwise defer to
 * the next `start`. An `end` earlier than `start` wraps past midnight, so
 * `{ start: "22:00", end: "06:00" }` is the overnight window.
 */
export function timeWindow(options: TimeWindowOptions): EnqueueGate {
  const { start, end, timeZone = UTC } = options;
  assertTimeZone(timeZone);
  const from = parseHhMm(start, "start");
  const to = parseHhMm(end, "end");
  if (from === to) {
    throw new PredicateValidationError(
      `timeWindow: start and end are both ${start}, which is an empty window`,
    );
  }
  return (ctx) => {
    const parts = partsInZone(ctx.now, timeZone);
    const minuteOfDay = parts.hour * 60 + parts.minute;
    const inWindow =
      from < to ? minuteOfDay >= from && minuteOfDay < to : minuteOfDay >= from || minuteOfDay < to;
    if (inWindow) {
      return Decision.allow();
    }
    return deferTo(nextTimeOfDay(parts, from, timeZone, ctx.now), ctx.now);
  };
}

/**
 * Allow on the given days in `timeZone`; otherwise defer to the start of the
 * next allowed day. With no allowed days every enqueue skips.
 */
export function dayOfWeek(options: DayOfWeekOptions): EnqueueGate {
  const { days, timeZone = UTC } = options;
  assertTimeZone(timeZone);
  const allowed = isoWeekdaysOf(days);
  if (allowed.size === 0) {
    return () => Decision.skip("no allowed day of week");
  }
  return (ctx) => {
    const parts = partsInZone(ctx.now, timeZone);
    if (allowed.has(parts.isoWeekday)) {
      return Decision.allow();
    }
    return deferTo(nextOpening(parts, allowed, 0, timeZone, ctx.now), ctx.now);
  };
}

/**
 * Allow only when the value at `path` equals `expected`; otherwise skip.
 *
 * `path` is dotted and rooted at `{ args }`, so `"args.0"` is the first
 * positional argument and `"args.0.tenantId"` a field of it — the same
 * addressing the Python SDK's `payload_matches` uses. Comparison is by
 * identity (`Object.is`): fine for primitives, but two structurally equal
 * objects do not match, so compare a field rather than a whole object.
 */
export function payloadMatches(path: string, expected: unknown): EnqueueGate {
  const segments = parsePath(path);
  return (ctx) => {
    const found = lookup({ args: ctx.args }, segments);
    return Object.is(found, expected)
      ? Decision.allow()
      : Decision.skip(`payload did not match ${path}`);
  };
}

/**
 * Allow only while `flag` is enabled; otherwise skip. Reads `FF_<FLAG>` from
 * the environment unless another provider is passed.
 */
export function featureFlag(
  flag: string,
  provider: FeatureFlagProvider | FlagLookup = envFeatureFlagProvider(),
): EnqueueGate {
  if (!flag) {
    throw new PredicateValidationError("featureFlag: flag must be non-empty");
  }
  const flags = toFlagProvider(provider);
  return (ctx) =>
    flags.isEnabled(flag, ctx) ? Decision.allow() : Decision.skip(`feature '${flag}' disabled`);
}

/**
 * True on Saturday or Sunday in `timeZone`. Usually used inverted —
 * `not(isWeekend())` — to keep weekend enqueues out.
 */
export function isWeekend(options: ZoneOptions = {}): Predicate {
  const { timeZone = UTC } = options;
  assertTimeZone(timeZone);
  return (ctx) => partsInZone(ctx.now, timeZone).isoWeekday >= 6;
}

/** Allow at or after `target`; before that, defer until it. */
export function after(target: Date | string): EnqueueGate {
  const at = parseInstant(target, "after");
  return (ctx) =>
    ctx.now.getTime() >= at ? Decision.allow() : Decision.defer(at - ctx.now.getTime());
}

/** True strictly before `target`. Deferring past a deadline makes no sense, so this rejects. */
export function before(target: Date | string): Predicate {
  const at = parseInstant(target, "before");
  return (ctx) => ctx.now.getTime() < at;
}

/** True when env var `name` is set to `1`/`true`/`yes`/`on`. */
export function envVarTruthy(name: string): Predicate {
  if (!name) {
    throw new PredicateValidationError("envVarTruthy: name must be non-empty");
  }
  return () => isTruthy(process.env[name]);
}

/** Every recipe, bundled — mirrors Java's `Recipes` for cross-SDK reading. */
export const Recipes = {
  after,
  before,
  businessHours,
  dayOfWeek,
  envVarTruthy,
  featureFlag,
  isWeekend,
  payloadMatches,
  timeWindow,
};

function deferTo(target: Date, now: Date): EnqueueDecision {
  return Decision.defer(Math.max(0, target.getTime() - now.getTime()));
}

/**
 * The first instant strictly after `now` that falls on an allowed day at
 * `hour`:00. Scans today plus a full week, which always finds one for a
 * non-empty `allowedDays`.
 */
function nextOpening(
  parts: ZonedParts,
  allowedDays: ReadonlySet<number>,
  hour: number,
  timeZone: string,
  now: Date,
): Date {
  for (let offset = 0; offset <= 7; offset++) {
    const day = offset === 0 ? parts : shiftDays(parts, offset);
    if (!allowedDays.has(day.isoWeekday)) {
      continue;
    }
    const candidate = zonedInstant(
      { year: day.year, month: day.month, day: day.day, hour },
      timeZone,
      now,
    );
    if (candidate.getTime() > now.getTime()) {
      return candidate;
    }
  }
  throw new PredicateValidationError(
    `no allowed day of week within a week of ${now.toISOString()} in time zone ${timeZone}`,
  );
}

/** Today at `minuteOfDay` if that is still ahead, else tomorrow at it. */
function nextTimeOfDay(parts: ZonedParts, minuteOfDay: number, timeZone: string, now: Date): Date {
  const hour = Math.floor(minuteOfDay / 60);
  const minute = minuteOfDay % 60;
  const today = zonedInstant(
    { year: parts.year, month: parts.month, day: parts.day, hour, minute },
    timeZone,
    now,
  );
  if (today.getTime() > now.getTime()) {
    return today;
  }
  const next = shiftDays(parts, 1);
  return zonedInstant(
    { year: next.year, month: next.month, day: next.day, hour, minute },
    timeZone,
    now,
  );
}

function isoWeekdaysOf(days: readonly DayOfWeek[]): ReadonlySet<number> {
  const isoWeekdays = new Set<number>();
  for (const day of days) {
    const index = DAYS_OF_WEEK.indexOf(day);
    if (index < 0) {
      throw new PredicateValidationError(
        `unknown day of week: ${JSON.stringify(day)} (expected one of ${DAYS_OF_WEEK.join(", ")})`,
      );
    }
    isoWeekdays.add(index + 1);
  }
  return isoWeekdays;
}

function assertHours(startHour: number, endHour: number): void {
  if (!Number.isInteger(startHour) || startHour < 0 || startHour > 23) {
    throw new PredicateValidationError(
      `businessHours: startHour must be an integer in 0–23, got ${startHour}`,
    );
  }
  if (!Number.isInteger(endHour) || endHour < 1 || endHour > 24) {
    throw new PredicateValidationError(
      `businessHours: endHour must be an integer in 1–24, got ${endHour}`,
    );
  }
  if (startHour >= endHour) {
    throw new PredicateValidationError(
      `businessHours: startHour (${startHour}) must be before endHour (${endHour})`,
    );
  }
}

/** Parse `"HH:MM"` into minutes since midnight. */
function parseHhMm(value: string, field: string): number {
  const match = /^(\d{1,2}):(\d{2})$/.exec(value);
  if (match === null) {
    throw new PredicateValidationError(
      `timeWindow: ${field} must be "HH:MM", got ${JSON.stringify(value)}`,
    );
  }
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour > 23 || minute > 59) {
    throw new PredicateValidationError(
      `timeWindow: ${field} is out of range: ${JSON.stringify(value)}`,
    );
  }
  return hour * 60 + minute;
}

function parseInstant(target: Date | string, recipe: string): number {
  const at = typeof target === "string" ? Date.parse(target) : target.getTime();
  if (Number.isNaN(at)) {
    throw new PredicateValidationError(`${recipe}: invalid target date ${String(target)}`);
  }
  return at;
}

/** Marks "nothing at this path", distinct from a stored `undefined`. */
const MISSING = Symbol("missing");

function parsePath(path: string): readonly string[] {
  const segments = path.split(".");
  if (path.length === 0 || segments.some((segment) => segment.length === 0)) {
    throw new PredicateValidationError(
      `payloadMatches: path must be dotted and non-empty, got ${JSON.stringify(path)}`,
    );
  }
  return segments;
}

function lookup(root: unknown, segments: readonly string[]): unknown {
  let node: unknown = root;
  for (const segment of segments) {
    node = step(node, segment);
    if (node === MISSING) {
      return MISSING;
    }
  }
  return node;
}

function step(node: unknown, segment: string): unknown {
  if (Array.isArray(node)) {
    const index = Number(segment);
    return Number.isInteger(index) && index >= 0 && index < node.length ? node[index] : MISSING;
  }
  if (typeof node === "object" && node !== null) {
    const record = node as Record<string, unknown>;
    return segment in record ? record[segment] : MISSING;
  }
  return MISSING;
}
