import { beforeEach, expect, it } from "vitest";
import { createLogger, setLogLevel, setLogSink } from "../../src/index";

let lines: string[];

beforeEach(() => {
  lines = [];
  setLogSink((_level, line) => lines.push(line));
  setLogLevel("debug");
});

it("emits namespaced, leveled lines", () => {
  createLogger("worker").info("hello");
  expect(lines).toHaveLength(1);
  expect(lines[0]).toContain("[flexiq:worker]");
  expect(lines[0]).toContain("INFO");
  expect(lines[0]).toContain("hello");
});

it("drops messages below the threshold without evaluating the thunk", () => {
  setLogLevel("warn");
  let built = false;
  createLogger("x").debug(() => {
    built = true;
    return "expensive";
  });
  expect(built).toBe(false);
  expect(lines).toHaveLength(0);
});

it("nests child namespaces", () => {
  createLogger("a").child("b").error("boom");
  expect(lines[0]).toContain("[flexiq:a:b]");
});

it("serializes Error meta", () => {
  createLogger("e").warn("failed", new Error("kaboom"));
  expect(lines[0]).toContain("kaboom");
});

it("silences all output at the silent level", () => {
  setLogLevel("silent");
  createLogger("s").error("nope");
  expect(lines).toHaveLength(0);
});
