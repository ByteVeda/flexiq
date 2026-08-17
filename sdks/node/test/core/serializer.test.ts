import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { JsonSerializer, MsgpackSerializer, Queue, type Worker } from "../../src/index";

describe("JsonSerializer", () => {
  it("round-trips structured values", () => {
    const serializer = new JsonSerializer();
    const value = { a: 1, b: [2, 3], c: "x" };
    expect(serializer.deserialize(serializer.serialize(value))).toEqual(value);
  });

  it("encodes undefined as null", () => {
    const serializer = new JsonSerializer();
    expect(serializer.deserialize(serializer.serialize(undefined))).toBeNull();
  });
});

describe("MsgpackSerializer", () => {
  it("round-trips structured values", () => {
    const serializer = new MsgpackSerializer();
    const value = { a: 1, b: [2, 3], c: "x", d: true };
    expect(serializer.deserialize(serializer.serialize(value))).toEqual(value);
  });
});

describe("Queue with a custom serializer", () => {
  let worker: Worker | undefined;
  afterEach(() => {
    worker?.stop();
    worker = undefined;
  });

  it("runs a job end-to-end using msgpack", async () => {
    const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
    const queue = new Queue({ dbPath, serializer: new MsgpackSerializer() });
    queue.task("concat", (a: string, b: string) => a + b);

    const id = queue.enqueue("concat", ["foo", "bar"]);
    worker = queue.runWorker();
    expect(await queue.result(id)).toBe("foobar");
  });
});
