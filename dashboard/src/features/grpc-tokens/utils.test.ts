import { describe, expect, it } from "vitest";
import type { GrpcScope } from "./types";
import { expiryDays, spellGrants } from "./utils";

const AVAILABLE: GrpcScope[] = [
  { name: "produce", narrowable: true },
  { name: "read", narrowable: true },
  { name: "inspect", narrowable: false },
];

describe("spellGrants", () => {
  it("sends every scope whole when nothing is narrowed", () => {
    expect(spellGrants(["produce", "inspect"], AVAILABLE, { queue: "", task: "" })).toEqual([
      "produce",
      "inspect",
    ]);
  });

  it("narrows only the scopes the server says can be", () => {
    expect(
      spellGrants(["produce", "read", "inspect"], AVAILABLE, {
        queue: " emails-* ",
        task: "send_receipt",
      }),
    ).toEqual([
      "produce:queue=emails-*,task=send_receipt",
      "read:queue=emails-*,task=send_receipt",
      "inspect",
    ]);
  });

  it("carries just the qualifier that was filled in", () => {
    expect(spellGrants(["read"], AVAILABLE, { queue: "", task: "charge" })).toEqual([
      "read:task=charge",
    ]);
  });
});

describe("expiryDays", () => {
  it("converts weeks and months to days", () => {
    expect(expiryDays("90", "days")).toEqual({ ok: true, days: 90 });
    expect(expiryDays("2", "weeks")).toEqual({ ok: true, days: 14 });
    expect(expiryDays(" 12 ", "months")).toEqual({ ok: true, days: 360 });
  });

  it("refuses what the server would", () => {
    for (const amount of ["", "0", "-3", "1.5", "two"]) {
      expect(expiryDays(amount, "days").ok, amount).toBe(false);
    }
    expect(expiryDays("366", "days").ok).toBe(false);
    expect(expiryDays("53", "weeks").ok).toBe(false);
    expect(expiryDays("13", "months").ok).toBe(false);
  });
});
