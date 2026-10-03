import { describe, expect, it } from "vitest";
import type { GrpcScope } from "./types";
import { spellGrants } from "./utils";

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
