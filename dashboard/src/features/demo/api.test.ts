import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError } from "@/lib/api-client";
import { enqueueDemoJob } from "./api";

function respond(status: number, body: unknown) {
  const fetchMock = vi.fn().mockResolvedValue(
    new Response(JSON.stringify(body), {
      status,
      headers: { "content-type": "application/json" },
    }),
  );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("enqueueDemoJob", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("posts an empty JSON event to the configured path, query intact", async () => {
    const fetchMock = respond(202, { jobs: [{ id: "j1", deduplicated: false }] });

    const result = await enqueueDemoJob("/t/demo?token=public");

    expect(result.jobs[0]?.id).toBe("j1");
    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(url).toBe("/t/demo?token=public");
    expect(init.method).toBe("POST");
    expect(init.body).toBe("{}");
    expect(init.credentials).toBe("same-origin");
  });

  it("surfaces the shared rate limit as a 429", async () => {
    respond(429, { error: "rate_limited" });

    await expect(enqueueDemoJob("/t/demo")).rejects.toSatisfy(
      (error: unknown) => error instanceof ApiError && error.status === 429,
    );
  });
});
