import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import {
  FlexiQError,
  JsonSerializer,
  MsgpackSerializer,
  Queue,
  SignedSerializer,
  type Worker,
} from "../../src/index";

let worker: Worker | undefined;
afterEach(() => {
  worker?.stop();
  worker = undefined;
});

it("round-trips a signed payload", () => {
  const s = new SignedSerializer("secret");
  const bytes = s.serialize({ a: 1, b: "x", c: [1, 2, 3] });
  expect(s.deserialize(bytes)).toEqual({ a: 1, b: "x", c: [1, 2, 3] });
});

it("prefixes a 32-byte HMAC tag", () => {
  const s = new SignedSerializer("secret");
  const body = new JsonSerializer().serialize({ a: 1 });
  expect(s.serialize({ a: 1 }).length).toBe(32 + body.length);
});

it("rejects a tampered payload", () => {
  const s = new SignedSerializer("secret");
  const bytes = Buffer.from(s.serialize({ a: 1 }));
  const last = bytes.length - 1;
  bytes[last] = (bytes[last] ?? 0) ^ 0xff; // flip a body byte
  expect(() => s.deserialize(bytes)).toThrow(/signature mismatch/);
});

it("rejects a payload signed with a different secret", () => {
  const a = new SignedSerializer("secret-a");
  const b = new SignedSerializer("secret-b");
  expect(() => b.deserialize(a.serialize({ a: 1 }))).toThrow(/signature mismatch/);
});

it("rejects a payload too short to be signed", () => {
  const s = new SignedSerializer("secret");
  expect(() => s.deserialize(new Uint8Array(8))).toThrow(/too short/);
});

it("requires a non-empty secret", () => {
  expect(() => new SignedSerializer("")).toThrow(FlexiQError);
});

it("wraps any inner serializer", () => {
  const s = new SignedSerializer("secret", new MsgpackSerializer());
  expect(s.deserialize(s.serialize({ a: 1 }))).toEqual({ a: 1 });
});

it("works end to end as the queue serializer", async () => {
  const queue = new Queue({
    dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-signed-")), "q.db"),
    serializer: new SignedSerializer("shared-key"),
  });
  queue.task("echo", (x: number) => x + 1);

  const id = queue.enqueue("echo", [41]);
  worker = queue.runWorker();

  expect(await queue.result(id)).toBe(42);
});
