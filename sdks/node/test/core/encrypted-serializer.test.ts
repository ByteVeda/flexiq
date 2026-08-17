import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import {
  EncryptedSerializer,
  MsgpackSerializer,
  Queue,
  SerializationError,
  type Worker,
} from "../../src/index";

let worker: Worker | undefined;
afterEach(() => {
  worker?.stop();
  worker = undefined;
});

it("round-trips an encrypted payload", () => {
  const s = new EncryptedSerializer("secret");
  const bytes = s.serialize({ a: 1, b: "x", c: [1, 2, 3] });
  expect(s.deserialize(bytes)).toEqual({ a: 1, b: "x", c: [1, 2, 3] });
});

it("hides the plaintext in the ciphertext", () => {
  const s = new EncryptedSerializer("secret");
  const bytes = Buffer.from(s.serialize({ password: "hunter2" }));
  expect(bytes.toString("utf8")).not.toContain("hunter2");
});

it("uses a fresh IV per call (same value → different bytes)", () => {
  const s = new EncryptedSerializer("secret");
  const a = Buffer.from(s.serialize({ a: 1 }));
  const b = Buffer.from(s.serialize({ a: 1 }));
  expect(a.equals(b)).toBe(false);
});

it("rejects a tampered payload", () => {
  const s = new EncryptedSerializer("secret");
  const bytes = Buffer.from(s.serialize({ a: 1 }));
  const last = bytes.length - 1;
  bytes[last] = (bytes[last] ?? 0) ^ 0xff;
  expect(() => s.deserialize(bytes)).toThrow(SerializationError);
});

it("rejects a payload encrypted with a different secret", () => {
  const a = new EncryptedSerializer("secret-a");
  const b = new EncryptedSerializer("secret-b");
  expect(() => b.deserialize(a.serialize({ a: 1 }))).toThrow(SerializationError);
});

it("rejects a payload too short to be encrypted", () => {
  const s = new EncryptedSerializer("secret");
  expect(() => s.deserialize(new Uint8Array(8))).toThrow(/too short/);
});

it("requires a non-empty secret", () => {
  expect(() => new EncryptedSerializer("")).toThrow(SerializationError);
});

it("wraps any inner serializer", () => {
  const s = new EncryptedSerializer("secret", new MsgpackSerializer());
  expect(s.deserialize(s.serialize({ a: 1 }))).toEqual({ a: 1 });
});

it("works end to end as the queue serializer", async () => {
  const queue = new Queue({
    dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-enc-")), "q.db"),
    serializer: new EncryptedSerializer("shared-key"),
  });
  queue.task("echo", (x: number) => x + 1);

  const id = queue.enqueue("echo", [41]);
  worker = queue.runWorker();

  expect(await queue.result(id)).toBe(42);
});
