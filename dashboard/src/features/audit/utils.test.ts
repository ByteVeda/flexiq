import { describe, expect, it } from "vitest";
import type { AuditRecord } from "./types";
import { countActiveFilters, describePrincipal, parseAuditSearch, toApiParams } from "./utils";

function record(overrides: Partial<AuditRecord>): AuditRecord {
  return {
    id: "r1",
    at: 1,
    principal_kind: "token",
    token_id: "abc123",
    principal: "ci-deployer",
    operation: "flexiq.producer.v1.Producer/Enqueue",
    target_kind: "job",
    target: "j1",
    outcome: "OK",
    ...overrides,
  };
}

describe("parseAuditSearch", () => {
  it("keeps every valid filter", () => {
    expect(
      parseAuditSearch({
        tokenId: " abc123 ",
        principalKind: "token",
        targetKind: "job",
        target: "j1",
        since: 10,
        until: "20",
      }),
    ).toEqual({
      tokenId: "abc123",
      principalKind: "token",
      targetKind: "job",
      target: "j1",
      since: 10,
      until: 20,
    });
  });

  it("drops what a hand-edited link got wrong rather than refusing the page", () => {
    expect(
      parseAuditSearch({ tokenId: "  ", principalKind: "robot", since: -5, until: "soon" }),
    ).toEqual({
      tokenId: undefined,
      principalKind: undefined,
      targetKind: undefined,
      target: undefined,
      since: undefined,
      until: undefined,
    });
  });

  it("reads zero as a bound, not as no bound", () => {
    expect(parseAuditSearch({ since: 0 }).since).toBe(0);
  });
});

describe("toApiParams", () => {
  it("spells filters the way the server reads them", () => {
    expect(
      toApiParams(
        {
          tokenId: "abc",
          principalKind: "user",
          targetKind: "queue",
          target: "q",
          since: 1,
          until: 2,
        },
        50,
        "cursor",
      ),
    ).toEqual({
      token_id: "abc",
      principal_kind: "user",
      target_kind: "queue",
      target: "q",
      since: 1,
      until: 2,
      limit: 50,
      after: "cursor",
    });
  });
});

describe("countActiveFilters", () => {
  it("counts only the filters that are set", () => {
    expect(countActiveFilters({})).toBe(0);
    expect(countActiveFilters({ tokenId: undefined, since: 0, target: "q" })).toBe(2);
  });
});

describe("describePrincipal", () => {
  it("names a token by its name and its id", () => {
    expect(describePrincipal(record({}))).toBe("ci-deployer (abc123)");
  });

  it("names a user by their username", () => {
    expect(
      describePrincipal(record({ principal_kind: "user", token_id: "ops", principal: "ops" })),
    ).toBe("ops");
  });

  it("names nobody when the kind carries no id", () => {
    expect(
      describePrincipal(record({ principal_kind: "cli", token_id: "", principal: "cli" })),
    ).toBeNull();
  });
});
