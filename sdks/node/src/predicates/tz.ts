import { PredicateValidationError } from "../errors";

/**
 * Time-zone arithmetic for the time-based recipes, built on `Intl` so the SDK
 * stays dependency-free. Two operations are needed: read an instant's wall
 * clock in a zone, and turn a wall clock in a zone back into an instant.
 */

/** A calendar date in some zone, without a time of day. */
export interface CalendarDay {
  readonly year: number;
  /** 1–12. */
  readonly month: number;
  /** 1–31. */
  readonly day: number;
  /** 1 = Monday … 7 = Sunday (ISO-8601, matching Java's `DayOfWeek`). */
  readonly isoWeekday: number;
}

/** An instant's wall clock in some zone. */
export interface ZonedParts extends CalendarDay {
  /** 0–23. */
  readonly hour: number;
  readonly minute: number;
  readonly second: number;
}

/** A wall-clock target to resolve back into an instant; time defaults to 00:00:00. */
export interface WallClock {
  readonly year: number;
  readonly month: number;
  readonly day: number;
  readonly hour?: number;
  readonly minute?: number;
  readonly second?: number;
}

/** Formatters are expensive to build and immutable once built, so they're cached. */
const formatters = new Map<string, Intl.DateTimeFormat>();

function formatterFor(timeZone: string): Intl.DateTimeFormat {
  const cached = formatters.get(timeZone);
  if (cached !== undefined) {
    return cached;
  }
  // `hourCycle: "h23"` rather than `hour12: false` — the latter reports midnight
  // as hour 24 in some ICU builds.
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    hourCycle: "h23",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
  formatters.set(timeZone, formatter);
  return formatter;
}

/** Reject an unknown IANA zone at recipe-construction time, not on first enqueue. */
export function assertTimeZone(timeZone: string): void {
  try {
    formatterFor(timeZone);
  } catch {
    throw new PredicateValidationError(`unknown time zone: ${JSON.stringify(timeZone)}`);
  }
}

/** Read `instant`'s wall clock in `timeZone`. */
export function partsInZone(instant: Date, timeZone: string): ZonedParts {
  const fields = new Map<string, number>();
  for (const part of formatterFor(timeZone).formatToParts(instant)) {
    if (part.type !== "literal") {
      fields.set(part.type, Number(part.value));
    }
  }
  const read = (name: string): number => {
    const value = fields.get(name);
    if (value === undefined || Number.isNaN(value)) {
      throw new PredicateValidationError(
        `could not read the ${name} of ${instant.toISOString()} in time zone ${timeZone}`,
      );
    }
    return value;
  };
  const year = read("year");
  const month = read("month");
  const day = read("day");
  return {
    year,
    month,
    day,
    isoWeekday: isoWeekdayOf(year, month, day),
    hour: read("hour"),
    minute: read("minute"),
    second: read("second"),
  };
}

/** Shift a calendar day by whole days, rolling over months and years. */
export function shiftDays(day: CalendarDay, days: number): CalendarDay {
  const shifted = new Date(Date.UTC(day.year, day.month - 1, day.day + days));
  return {
    year: shifted.getUTCFullYear(),
    month: shifted.getUTCMonth() + 1,
    day: shifted.getUTCDate(),
    isoWeekday: shifted.getUTCDay() === 0 ? 7 : shifted.getUTCDay(),
  };
}

/**
 * The instant at which `wall` reads on the clock in `timeZone`.
 *
 * The zone's UTC offset is only knowable at a given instant, so this guesses
 * using the offset at `reference`, then re-reads the offset at the guess and
 * corrects once — that second pass is what makes a target on the far side of a
 * DST transition land on the right instant. A target inside a spring-forward
 * gap (a wall clock that never occurs) resolves to just after the gap.
 */
export function zonedInstant(wall: WallClock, timeZone: string, reference: Date): Date {
  const wanted = Date.UTC(
    wall.year,
    wall.month - 1,
    wall.day,
    wall.hour ?? 0,
    wall.minute ?? 0,
    wall.second ?? 0,
  );
  const guess = wanted - offsetMsAt(reference, timeZone);
  return new Date(wanted - offsetMsAt(new Date(guess), timeZone));
}

/** How far `timeZone`'s wall clock runs ahead of UTC at `instant`. */
function offsetMsAt(instant: Date, timeZone: string): number {
  const parts = partsInZone(instant, timeZone);
  const asUtc = Date.UTC(
    parts.year,
    parts.month - 1,
    parts.day,
    parts.hour,
    parts.minute,
    parts.second,
  );
  // Parts have second precision, so compare against a whole-second instant.
  return asUtc - (instant.getTime() - instant.getMilliseconds());
}

function isoWeekdayOf(year: number, month: number, day: number): number {
  const weekday = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
  return weekday === 0 ? 7 : weekday;
}
