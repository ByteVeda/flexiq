import { describe, expect, it } from "vitest";
import {
  businessHours,
  dayOfWeek,
  envVarTruthy,
  featureFlag,
  isWeekend,
  type PredicateContext,
  PredicateValidationError,
  payloadMatches,
  Recipes,
  timeWindow,
} from "../../src/index";

/** A context pinned to `now`, so time recipes are deterministic. */
function at(now: string, args: readonly unknown[] = []): PredicateContext {
  return { taskName: "t", args, now: new Date(now) };
}

const hours = (count: number) => count * 3_600_000;

// 2026-07-22T12:00Z is a Wednesday (08:00 in New York, EDT).
const WED_NOON_UTC = "2026-07-22T12:00:00Z";
const SAT_NOON_UTC = "2026-07-25T12:00:00Z";

describe("businessHours", () => {
  it("allows inside the window", () => {
    expect(businessHours()(at(WED_NOON_UTC))).toEqual({ kind: "allow" });
  });

  it("defers to today's opening when the day has not started", () => {
    expect(businessHours()(at("2026-07-22T08:00:00Z"))).toEqual({
      kind: "defer",
      delayMs: hours(1),
    });
  });

  it("defers to tomorrow's opening after closing time", () => {
    expect(businessHours()(at("2026-07-22T18:00:00Z"))).toEqual({
      kind: "defer",
      delayMs: hours(15),
    });
  });

  it("skips the weekend when weekdaysOnly", () => {
    // Sat 12:00 → Mon 09:00 is a day and 21 hours out.
    expect(businessHours()(at(SAT_NOON_UTC))).toEqual({ kind: "defer", delayMs: hours(45) });
  });

  it("opens on the weekend when weekdaysOnly is off", () => {
    expect(businessHours({ weekdaysOnly: false })(at(SAT_NOON_UTC))).toEqual({ kind: "allow" });
  });

  it("reads the window in the configured zone", () => {
    const gate = businessHours({ timeZone: "America/New_York" });
    // 12:00Z is 08:00 EDT — an hour before opening.
    expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "defer", delayMs: hours(1) });
    expect(gate(at("2026-07-22T14:00:00Z"))).toEqual({ kind: "allow" });
  });

  it("pushes a target inside a spring-forward gap past the gap", () => {
    // New York skips 02:00–03:00 on 2026-03-08, so a window opening at 02:30
    // has no instant. It must resolve forward to 03:30 EDT (07:30Z), never back
    // to 01:30 EST — that would enqueue an hour before the requested time.
    const gate = timeWindow({ start: "02:30", end: "04:00", timeZone: "America/New_York" });
    const now = "2026-03-08T06:00:00Z"; // 01:00 EST, an hour before the gap
    const decision = gate(at(now));
    expect(decision).toMatchObject({ kind: "defer" });
    const runAt = new Date(new Date(now).getTime() + (decision as { delayMs: number }).delayMs);
    expect(runAt.toISOString()).toBe("2026-03-08T07:30:00.000Z");
  });

  it("lands on the right instant across a DST transition", () => {
    // Sat 18:00 EST; New York springs forward on 2026-03-08, so the next
    // opening (Mon 09:00 EDT = 13:00Z) is 38 hours out, not 39.
    expect(businessHours({ timeZone: "America/New_York" })(at("2026-03-07T23:00:00Z"))).toEqual({
      kind: "defer",
      delayMs: hours(38),
    });
  });

  it("honors custom hours", () => {
    const gate = businessHours({ startHour: 0, endHour: 24 });
    expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "allow" });
    expect(gate(at("2026-07-22T23:59:00Z"))).toEqual({ kind: "allow" });
  });

  it("rejects an unknown time zone at build time", () => {
    expect(() => businessHours({ timeZone: "Mars/Phobos" })).toThrow(PredicateValidationError);
  });

  it("rejects an inverted or out-of-range window at build time", () => {
    expect(() => businessHours({ startHour: 17, endHour: 9 })).toThrow(PredicateValidationError);
    expect(() => businessHours({ startHour: -1 })).toThrow(PredicateValidationError);
    expect(() => businessHours({ endHour: 25 })).toThrow(PredicateValidationError);
    expect(() => businessHours({ startHour: 9.5, endHour: 17 })).toThrow(PredicateValidationError);
  });
});

describe("timeWindow", () => {
  it("allows inside a same-day window and defers outside it", () => {
    const gate = timeWindow({ start: "09:00", end: "17:00" });
    expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "allow" });
    expect(gate(at("2026-07-22T18:00:00Z"))).toEqual({ kind: "defer", delayMs: hours(15) });
  });

  it("wraps past midnight", () => {
    const gate = timeWindow({ start: "22:00", end: "06:00" });
    expect(gate(at("2026-07-22T23:00:00Z"))).toEqual({ kind: "allow" });
    expect(gate(at("2026-07-22T03:00:00Z"))).toEqual({ kind: "allow" });
    expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "defer", delayMs: hours(10) });
  });

  it("honors minutes and the configured zone", () => {
    const gate = timeWindow({ start: "08:30", end: "08:45", timeZone: "America/New_York" });
    expect(gate(at("2026-07-22T12:35:00Z"))).toEqual({ kind: "allow" });
    expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "defer", delayMs: 30 * 60_000 });
  });

  it("rejects a malformed or empty window at build time", () => {
    expect(() => timeWindow({ start: "9", end: "17:00" })).toThrow(PredicateValidationError);
    expect(() => timeWindow({ start: "25:00", end: "17:00" })).toThrow(PredicateValidationError);
    expect(() => timeWindow({ start: "09:60", end: "17:00" })).toThrow(PredicateValidationError);
    expect(() => timeWindow({ start: "09:00", end: "09:00" })).toThrow(PredicateValidationError);
  });
});

describe("dayOfWeek", () => {
  it("allows an allowed day", () => {
    expect(dayOfWeek({ days: ["wed"] })(at(WED_NOON_UTC))).toEqual({ kind: "allow" });
  });

  it("defers to the start of the next allowed day", () => {
    expect(dayOfWeek({ days: ["mon", "thu"] })(at(WED_NOON_UTC))).toEqual({
      kind: "defer",
      delayMs: hours(12),
    });
  });

  it("wraps to next week when only earlier days are allowed", () => {
    // Sat 12:00Z → Mon 00:00Z is a day and 12 hours out.
    expect(dayOfWeek({ days: ["mon"] })(at(SAT_NOON_UTC))).toEqual({
      kind: "defer",
      delayMs: hours(36),
    });
  });

  it("skips when no day is allowed", () => {
    expect(dayOfWeek({ days: [] })(at(WED_NOON_UTC))).toEqual({
      kind: "skip",
      reason: "no allowed day of week",
    });
  });

  it("rejects an unknown day name at build time", () => {
    // Plain strings reach this from JavaScript callers, so it is validated.
    expect(() => dayOfWeek({ days: ["funday" as "mon"] })).toThrow(PredicateValidationError);
  });
});

describe("payloadMatches", () => {
  it("allows a match and skips a mismatch", () => {
    const gate = payloadMatches("args.0", "acme");
    expect(gate(at(WED_NOON_UTC, ["acme"]))).toEqual({ kind: "allow" });
    expect(gate(at(WED_NOON_UTC, ["evilcorp"]))).toEqual({
      kind: "skip",
      reason: "payload did not match args.0",
    });
  });

  it("walks into nested objects", () => {
    const gate = payloadMatches("args.1.tenant.id", 7);
    expect(gate(at(WED_NOON_UTC, ["job", { tenant: { id: 7 } }]))).toEqual({ kind: "allow" });
    expect(gate(at(WED_NOON_UTC, ["job", { tenant: { id: 8 } }]))).toMatchObject({ kind: "skip" });
  });

  it("skips when the path is absent rather than matching undefined", () => {
    const gate = payloadMatches("args.0.missing", undefined);
    expect(gate(at(WED_NOON_UTC, [{}]))).toMatchObject({ kind: "skip" });
    expect(payloadMatches("args.9", "x")(at(WED_NOON_UTC, ["x"]))).toMatchObject({ kind: "skip" });
  });

  it("reads own properties only, not inherited ones", () => {
    // `constructor` and `toString` live on the prototype — not "in the payload".
    expect(payloadMatches("args.0.constructor", Object)(at(WED_NOON_UTC, [{}]))).toMatchObject({
      kind: "skip",
    });
    const polluted = Object.create({ tenant: "inherited" }) as Record<string, unknown>;
    expect(
      payloadMatches("args.0.tenant", "inherited")(at(WED_NOON_UTC, [polluted])),
    ).toMatchObject({ kind: "skip" });
    polluted.tenant = "own";
    expect(payloadMatches("args.0.tenant", "own")(at(WED_NOON_UTC, [polluted]))).toEqual({
      kind: "allow",
    });
  });

  it("rejects an empty or malformed path at build time", () => {
    expect(() => payloadMatches("", 1)).toThrow(PredicateValidationError);
    expect(() => payloadMatches("args..0", 1)).toThrow(PredicateValidationError);
  });
});

describe("featureFlag", () => {
  it("allows when enabled and skips when disabled", () => {
    const gate = featureFlag("beta-export", (flag) => flag === "beta-export");
    expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "allow" });
    expect(featureFlag("other", () => false)(at(WED_NOON_UTC))).toEqual({
      kind: "skip",
      reason: "feature 'other' disabled",
    });
  });

  it("accepts the object provider form and sees the context", () => {
    const seen: string[] = [];
    const gate = featureFlag("beta", {
      isEnabled(flag, ctx) {
        seen.push(`${flag}:${ctx.taskName}`);
        return true;
      },
    });
    expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "allow" });
    expect(seen).toEqual(["beta:t"]);
  });

  it("reads FF_<FLAG> from the environment by default", () => {
    const gate = featureFlag("beta-export");
    expect(gate(at(WED_NOON_UTC))).toMatchObject({ kind: "skip" });
    process.env.FF_BETA_EXPORT = "yes";
    try {
      expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "allow" });
    } finally {
      delete process.env.FF_BETA_EXPORT;
    }
  });

  it("rejects an empty flag at build time", () => {
    expect(() => featureFlag("")).toThrow(PredicateValidationError);
  });
});

describe("boolean recipes", () => {
  it("isWeekend reads the configured zone", () => {
    expect(isWeekend()(at(SAT_NOON_UTC))).toBe(true);
    expect(isWeekend()(at(WED_NOON_UTC))).toBe(false);
    // 2026-07-27T02:00Z is Monday in UTC but still Sunday in Los Angeles.
    expect(isWeekend({ timeZone: "America/Los_Angeles" })(at("2026-07-27T02:00:00Z"))).toBe(true);
  });

  // `Recipes.after` / `Recipes.before` rather than the bare imports: a bare
  // `after(...)` / `before(...)` call in a test file reads as a vitest hook.
  it("after allows once the target passes and defers until then", () => {
    const gate = Recipes.after("2026-07-22T13:00:00Z");
    expect(gate(at(WED_NOON_UTC))).toEqual({ kind: "defer", delayMs: hours(1) });
    expect(gate(at("2026-07-22T13:00:00Z"))).toEqual({ kind: "allow" });
    expect(Recipes.after(new Date("2026-07-22T11:00:00Z"))(at(WED_NOON_UTC))).toEqual({
      kind: "allow",
    });
  });

  it("before is true only up to the target", () => {
    expect(Recipes.before("2026-07-22T13:00:00Z")(at(WED_NOON_UTC))).toBe(true);
    expect(Recipes.before("2026-07-22T11:00:00Z")(at(WED_NOON_UTC))).toBe(false);
  });

  it("rejects an unparseable target at build time", () => {
    expect(() => Recipes.after("not a date")).toThrow(PredicateValidationError);
    expect(() => Recipes.before(new Date("nope"))).toThrow(PredicateValidationError);
  });

  it("envVarTruthy reads the process environment", () => {
    const gate = envVarTruthy("FLEXIQ_TEST_FLAG");
    expect(gate(at(WED_NOON_UTC))).toBe(false);
    for (const value of ["1", "true", "YES", " on "]) {
      process.env.FLEXIQ_TEST_FLAG = value;
      expect(gate(at(WED_NOON_UTC))).toBe(true);
    }
    process.env.FLEXIQ_TEST_FLAG = "0";
    expect(gate(at(WED_NOON_UTC))).toBe(false);
    delete process.env.FLEXIQ_TEST_FLAG;
    expect(() => envVarTruthy("")).toThrow(PredicateValidationError);
  });
});

it("exposes every recipe on the Recipes bundle", () => {
  expect(Object.keys(Recipes).sort()).toEqual([
    "after",
    "before",
    "businessHours",
    "dayOfWeek",
    "envVarTruthy",
    "featureFlag",
    "isWeekend",
    "payloadMatches",
    "timeWindow",
  ]);
});
