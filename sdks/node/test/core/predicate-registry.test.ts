import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import {
  Decision,
  defaultRegistry,
  type EnqueueGate,
  PredicateRegistry,
  PredicateRejectedError,
  PredicateValidationError,
  Queue,
  registerPredicate,
} from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-preg-")), "q.db") });
}

const positive: EnqueueGate = ({ args }) => (args[0] as number) > 0;

// The default registry is process-wide, so names registered here must not leak
// into other test files running in the same worker.
afterEach(() => {
  defaultRegistry().clear();
});

it("registers, looks up, and lists gates", () => {
  const registry = new PredicateRegistry();
  registry.register("positive", positive);
  registry.register("always", () => Decision.allow());

  expect(registry.lookup("positive")).toBe(positive);
  expect(registry.has("positive")).toBe(true);
  expect(registry.has("missing")).toBe(false);
  expect(registry.names()).toEqual(["always", "positive"]);
});

it("refuses to overwrite a name without replace", () => {
  const registry = new PredicateRegistry();
  registry.register("positive", positive);

  // Re-registering the same gate is a no-op, not a conflict.
  registry.register("positive", positive);
  expect(() => registry.register("positive", () => true)).toThrow(PredicateValidationError);
  registry.register("positive", () => true, { replace: true });
  expect(registry.lookup("positive")).not.toBe(positive);
});

it("rejects an empty name and reports what is registered", () => {
  const registry = new PredicateRegistry();
  expect(() => registry.register("", positive)).toThrow(PredicateValidationError);
  expect(() => registry.lookup("nope")).toThrow(/registered: <none>/);
  registry.register("positive", positive);
  expect(() => registry.lookup("nope")).toThrow(/registered: positive/);
});

it("clears every registration", () => {
  const registry = new PredicateRegistry();
  registry.register("positive", positive);
  registry.clear();
  expect(registry.names()).toEqual([]);
});

it("gates a task by registered name", () => {
  registerPredicate("positive", positive);
  const queue = newQueue().task("charge", (n: number) => n);
  queue.gate("charge", "positive");

  expect(typeof queue.enqueue("charge", [5])).toBe("string");
  expect(() => queue.enqueue("charge", [-1])).toThrow(PredicateRejectedError);
});

it("rejects an unknown name when the gate is registered, not at enqueue", () => {
  const queue = newQueue().task("charge", (n: number) => n);
  expect(() => queue.gate("charge", "no-such-predicate")).toThrow(PredicateValidationError);
  // Nothing was registered, so the task still enqueues freely.
  expect(typeof queue.enqueue("charge", [-1])).toBe("string");
});
