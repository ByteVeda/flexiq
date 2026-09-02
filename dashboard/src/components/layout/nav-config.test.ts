import { describe, expect, it } from "vitest";
import { NAV, visibleNav } from "./nav-config";

function labels(groups: ReturnType<typeof visibleNav>): string[] {
  return groups.flatMap((group) => group.items.map((item) => item.label));
}

describe("visibleNav", () => {
  it("hides an optional route until the server confirms it", () => {
    expect(labels(visibleNav({}))).not.toContain("Executors");
    expect(labels(visibleNav({ "/executors": undefined }))).not.toContain("Executors");
    expect(labels(visibleNav({ "/executors": false }))).not.toContain("Executors");
    expect(labels(visibleNav({ "/executors": true }))).toContain("Executors");
  });

  it("hides gRPC tokens until the server confirms it serves them", () => {
    // The SPA is served by the SDK dashboards too, and they answer 404 for
    // /api/grpc-tokens — a nav entry that leads to an error page is worse than
    // one that appears a moment late.
    expect(labels(visibleNav({}))).not.toContain("gRPC tokens");
    expect(labels(visibleNav({ "/grpc-tokens": undefined }))).not.toContain("gRPC tokens");
    expect(labels(visibleNav({ "/grpc-tokens": false }))).not.toContain("gRPC tokens");
    expect(labels(visibleNav({ "/grpc-tokens": true }))).toContain("gRPC tokens");
  });

  it("leaves every unconditional route alone", () => {
    const always = NAV.flatMap((group) => group.items)
      .filter((item) => !item.optional)
      .map((item) => item.label);
    expect(labels(visibleNav({}))).toEqual(always);
  });

  it("drops a group that an unsupported route left empty", () => {
    const groups = visibleNav({});
    for (const group of groups) {
      expect(group.items.length).toBeGreaterThan(0);
    }
  });
});
