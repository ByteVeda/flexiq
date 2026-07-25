import { expect, it } from "vitest";
import { buildRestRoutes, flattenQueryParams } from "../../src/contrib/rest";
import type { ReadinessReport } from "../../src/health";
import type { Queue } from "../../src/index";

it("rejects mutually exclusive include/exclude route options", () => {
  expect(() => buildRestRoutes({ includeRoutes: ["stats"], excludeRoutes: ["jobs"] })).toThrow(
    /mutually exclusive/,
  );
  expect(buildRestRoutes({ includeRoutes: ["stats"] }).map((r) => r.name)).toEqual(["stats"]);
});

it("flattens string and first-of-array query values", () => {
  const out = flattenQueryParams({ status: "failed", tag: ["a", "b"], n: 5 });
  expect(out.status).toBe("failed");
  expect(out.tag).toBe("a");
  expect(out.n).toBeUndefined();
});

it("ignores prototype-polluting query keys", () => {
  const out = flattenQueryParams({ __proto__: "x", constructor: "y", limit: "10" });
  expect(out.limit).toBe("10");
  // No pollution of Object.prototype, and the dangerous keys aren't carried over.
  expect(({} as Record<string, unknown>).x).toBeUndefined();
  expect(Object.hasOwn(out, "__proto__")).toBe(false);
});

it("handles a null/undefined query", () => {
  expect(flattenQueryParams(undefined)).toEqual({});
  expect(flattenQueryParams(null)).toEqual({});
});

it("serves the probe routes off the standalone health helpers", async () => {
  const routes = buildRestRoutes();
  expect(routes.map((r) => r.name)).toEqual(
    expect.arrayContaining(["health", "readiness", "resources"]),
  );

  const request = { params: {}, query: {}, body: undefined };
  const queue = {
    stats: async () => {
      throw new Error("db gone");
    },
    listWorkers: async () => [],
  } as unknown as Queue;

  const health = routes.find((r) => r.name === "health");
  expect(await health?.handle(queue, request)).toEqual({ status: 200, body: { status: "ok" } });

  // Degraded must answer 503 so orchestrators drain the instance.
  const readiness = routes.find((r) => r.name === "readiness");
  const response = await readiness?.handle(queue, request);
  expect(response?.status).toBe(503);
  expect((response?.body as ReadinessReport).status).toBe("degraded");
});
