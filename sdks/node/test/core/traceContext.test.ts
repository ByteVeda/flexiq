// The metadata merge rule for W3C trace context, pinned to the core's vectors.
import { describe, expect, it } from "vitest";
import { mergeTraceCarrier, traceCarrier } from "../../src/trace-context";

const PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const CARRIER = { traceparent: PARENT, tracestate: "a=1" };

describe("mergeTraceCarrier", () => {
  it("turns absent or empty metadata into the carrier", () => {
    const expected = `{"traceparent":"${PARENT}","tracestate":"a=1"}`;
    expect(mergeTraceCarrier(undefined, CARRIER)).toBe(expected);
    expect(mergeTraceCarrier(" ", CARRIER)).toBe(expected);
  });

  it("adds the keys to an object and keeps its own bytes", () => {
    const carrier = { traceparent: PARENT };
    expect(mergeTraceCarrier('  { "user" : 1 }', carrier)).toBe(
      `  {"traceparent":"${PARENT}", "user" : 1 }`,
    );
    expect(mergeTraceCarrier("{ }", carrier)).toBe(`{"traceparent":"${PARENT}" }`);
  });

  it.each([
    '{"traceparent":"mine"}',
    '{"tracestate":"mine=1"}',
  ])("lets the caller's own context win whole: %s", (own) => {
    expect(mergeTraceCarrier(own, CARRIER)).toBe(own);
  });

  it.each([
    "[1,2]",
    '"text"',
    "42",
    "not json",
    "{broken",
    "null",
  ])("leaves metadata that is not an object alone: %s", (other) => {
    expect(mergeTraceCarrier(other, CARRIER)).toBe(other);
  });

  it("never overwrites a key the caller already set", () => {
    const merged = mergeTraceCarrier('{"baggage":"mine"}', { ...CARRIER, baggage: "theirs" });
    expect(JSON.parse(merged ?? "")).toEqual({ ...CARRIER, baggage: "mine" });
  });

  it("changes nothing for an empty carrier", () => {
    expect(mergeTraceCarrier(undefined, {})).toBeUndefined();
    expect(mergeTraceCarrier("plain", {})).toBe("plain");
  });

  it("JSON-escapes values", () => {
    const merged = mergeTraceCarrier(undefined, { tracestate: 'k="v"' });
    expect(JSON.parse(merged ?? "")).toEqual({ tracestate: 'k="v"' });
  });
});

describe("traceCarrier", () => {
  it("is the object's string members", () => {
    expect(traceCarrier(`{"traceparent":"${PARENT}","n":1}`)).toEqual({ traceparent: PARENT });
    for (const metadata of [undefined, "", "[1]", "not json", "null"]) {
      expect(traceCarrier(metadata)).toEqual({});
    }
  });
});
