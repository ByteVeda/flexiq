import { describe, expect, it } from "vitest";
import { ApiError } from "./api-client";
import { asError, isBackendUnreachable, isNetworkError, isServerUnavailable } from "./errors";

describe("isNetworkError", () => {
  it("matches Chrome's 'Failed to fetch'", () => {
    expect(isNetworkError(new TypeError("Failed to fetch"))).toBe(true);
  });

  it("matches Firefox's 'NetworkError when attempting to fetch resource'", () => {
    expect(isNetworkError(new TypeError("NetworkError when attempting to fetch resource."))).toBe(
      true,
    );
  });

  it("matches Safari's 'Load failed'", () => {
    expect(isNetworkError(new TypeError("Load failed"))).toBe(true);
  });

  it("rejects unrelated TypeErrors", () => {
    expect(isNetworkError(new TypeError("Cannot read property 'foo' of undefined"))).toBe(false);
  });

  it("rejects non-Error inputs", () => {
    expect(isNetworkError(null)).toBe(false);
    expect(isNetworkError(undefined)).toBe(false);
    expect(isNetworkError("Failed to fetch")).toBe(false);
    expect(isNetworkError(new Error("Failed to fetch"))).toBe(false);
  });
});

describe("isServerUnavailable", () => {
  it("returns true for 502/503/504 ApiErrors", () => {
    expect(isServerUnavailable(new ApiError("bad gateway", 502, null))).toBe(true);
    expect(isServerUnavailable(new ApiError("unavailable", 503, null))).toBe(true);
    expect(isServerUnavailable(new ApiError("timeout", 504, null))).toBe(true);
  });

  it("returns false for other ApiError statuses", () => {
    expect(isServerUnavailable(new ApiError("not found", 404, null))).toBe(false);
    expect(isServerUnavailable(new ApiError("server error", 500, null))).toBe(false);
    expect(isServerUnavailable(new ApiError("teapot", 418, null))).toBe(false);
  });

  it("returns false for non-ApiError inputs", () => {
    expect(isServerUnavailable(new Error("oops"))).toBe(false);
    expect(isServerUnavailable(new TypeError("Failed to fetch"))).toBe(false);
    expect(isServerUnavailable(null)).toBe(false);
  });
});

describe("isBackendUnreachable", () => {
  it("is true for network errors", () => {
    expect(isBackendUnreachable(new TypeError("Failed to fetch"))).toBe(true);
  });

  it("is true for 503 ApiErrors", () => {
    expect(isBackendUnreachable(new ApiError("down", 503, null))).toBe(true);
  });

  it("is false for 4xx ApiErrors", () => {
    expect(isBackendUnreachable(new ApiError("forbidden", 403, null))).toBe(false);
  });

  it("is false for plain errors", () => {
    expect(isBackendUnreachable(new Error("some other failure"))).toBe(false);
  });
});

describe("asError", () => {
  it("returns the same instance for an Error", () => {
    const err = new ApiError("down", 503, null);
    expect(asError(err)).toBe(err);
  });

  it("wraps a thrown string", () => {
    expect(asError("boom").message).toBe("boom");
  });

  it("wraps a thrown non-Error object", () => {
    expect(asError({ code: 42 })).toBeInstanceOf(Error);
    expect(asError(undefined).message).toBe("undefined");
  });
});
