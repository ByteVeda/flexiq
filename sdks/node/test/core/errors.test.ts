import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import {
  FlexiQError,
  NotesValidationError,
  Queue,
  QueueError,
  ResourceError,
  ResourceNotFoundError,
  ResourceScopeError,
  ResultTimeoutError,
  SerializationError,
  SignedSerializer,
  type Worker,
  WorkflowError,
} from "../../src/index";

let worker: Worker | undefined;
afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-err-")), "q.db") });
}

it("specific errors extend the right bases and carry a name", () => {
  for (const err of [
    new QueueError("x"),
    new NotesValidationError("x"),
    new SerializationError("x"),
    new WorkflowError("x"),
    new ResultTimeoutError("id", 1),
    new ResourceError("x"),
  ]) {
    expect(err).toBeInstanceOf(FlexiQError);
  }
  // resource errors nest under ResourceError
  expect(new ResourceNotFoundError("db")).toBeInstanceOf(ResourceError);
  expect(new ResourceScopeError("db")).toBeInstanceOf(ResourceError);
  expect(new ResourceNotFoundError("db")).toBeInstanceOf(FlexiQError);

  expect(new QueueError("x").name).toBe("QueueError");
  expect(new ResourceNotFoundError("db").resourceName).toBe("db");
  expect(new ResultTimeoutError("j", 50).timeoutMs).toBe(50);
});

it("Queue construction for a non-sqlite backend without a dsn throws QueueError", () => {
  expect(() => new Queue({ backend: "postgres" })).toThrow(QueueError);
});

it("invalid notes throw NotesValidationError", () => {
  const queue = newQueue();
  queue.task("noop", () => undefined);
  expect(() => queue.enqueue("noop", undefined, { notes: { blob: "x".repeat(5000) } })).toThrow(
    NotesValidationError,
  );
});

it("SignedSerializer surfaces SerializationError", () => {
  const s = new SignedSerializer("k");
  expect(() => s.deserialize(new Uint8Array(4))).toThrow(SerializationError);
});

it("result() times out with ResultTimeoutError", async () => {
  const queue = newQueue();
  queue.task("slow", () => 1);
  const id = queue.enqueue("slow"); // no worker started → never runs
  await expect(queue.result(id, { timeoutMs: 120, pollMs: 20 })).rejects.toBeInstanceOf(
    ResultTimeoutError,
  );
});

it("duplicate workflow step throws WorkflowError", () => {
  const queue = newQueue();
  expect(() => queue.workflows.define("w").step("a", "t").step("a", "t")).toThrow(WorkflowError);
});
