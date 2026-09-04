import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gunzipSync } from "node:zlib";
import { afterEach, describe, expect, it } from "vitest";
import {
  AesGcmCodec,
  CodecSerializer,
  CryptoError,
  GzipCodec,
  HmacCodec,
  JsonSerializer,
  MsgpackSerializer,
  type PayloadCodec,
  Queue,
  SerializationError,
  type Worker,
} from "../../src/index";

let worker: Worker | undefined;

const HMAC_KEY = Buffer.from("codec-hmac-secret");
const AES_KEY = Buffer.from("0123456789abcdef0123456789abcdef"); // 32 bytes -> AES-256

function flipLastByte(bytes: Uint8Array): Buffer {
  const buf = Buffer.from(bytes);
  buf.writeUInt8(buf.readUInt8(buf.length - 1) ^ 0xff, buf.length - 1);
  return buf;
}

describe("GzipCodec", () => {
  it("round-trips and compresses", () => {
    const codec = new GzipCodec();
    const data = Buffer.from("hello codec world".repeat(100));
    const encoded = codec.encode(data);
    expect(encoded.length).toBeLessThan(data.length);
    expect(Buffer.from(codec.decode(encoded)).equals(data)).toBe(true);
  });

  it("round-trips an empty payload", () => {
    const codec = new GzipCodec();
    expect(codec.decode(codec.encode(Buffer.alloc(0))).length).toBe(0);
  });

  it("rejects decompression exceeding the cap", () => {
    const encoded = new GzipCodec().encode(Buffer.alloc(1024, "a"));
    expect(() => new GzipCodec(16).decode(encoded)).toThrow(SerializationError);
    expect(() => new GzipCodec(16).decode(encoded)).toThrow(/exceeds the 16-byte limit/);
  });

  it("decodes a payload exactly at the cap", () => {
    const data = Buffer.alloc(64, "a");
    const encoded = new GzipCodec().encode(data);
    expect(Buffer.from(new GzipCodec(64).decode(encoded)).equals(data)).toBe(true);
  });

  it("rejects a corrupt stream", () => {
    expect(() => new GzipCodec().decode(Buffer.from("not gzip data"))).toThrow(SerializationError);
  });

  it("rejects a non-positive cap", () => {
    expect(() => new GzipCodec(0)).toThrow(SerializationError);
    expect(() => new GzipCodec(-1)).toThrow(SerializationError);
  });
});

describe("AesGcmCodec", () => {
  it("round-trips and hides the plaintext", () => {
    const codec = new AesGcmCodec(AES_KEY);
    const data = Buffer.from("secret payload");
    const encoded = Buffer.from(codec.encode(data));
    expect(encoded.includes(data)).toBe(false);
    expect(Buffer.from(codec.decode(encoded)).equals(data)).toBe(true);
  });

  it("lays out [iv | ciphertext | tag] — the cross-SDK contract", () => {
    const codec = new AesGcmCodec(AES_KEY);
    const encoded = codec.encode(Buffer.alloc(10, "x"));
    expect(encoded.length).toBe(12 + 10 + 16);
  });

  it("uses a fresh IV per call", () => {
    const codec = new AesGcmCodec(AES_KEY);
    const a = Buffer.from(codec.encode(Buffer.from("same")));
    const b = Buffer.from(codec.encode(Buffer.from("same")));
    expect(a.equals(b)).toBe(false);
  });

  it("supports AES-128 and AES-192 key lengths", () => {
    for (const length of [16, 24]) {
      const codec = new AesGcmCodec(Buffer.alloc(length, 7));
      const data = Buffer.from("payload");
      expect(Buffer.from(codec.decode(codec.encode(data))).equals(data)).toBe(true);
    }
  });

  it("rejects a tampered payload", () => {
    const codec = new AesGcmCodec(AES_KEY);
    const encoded = flipLastByte(codec.encode(Buffer.from("secret payload")));
    expect(() => codec.decode(encoded)).toThrow(CryptoError);
  });

  it("rejects a short payload", () => {
    expect(() => new AesGcmCodec(AES_KEY).decode(Buffer.from("short"))).toThrow(/too short/);
  });

  it("rejects a bad key length", () => {
    expect(() => new AesGcmCodec(Buffer.from("short-key"))).toThrow(/16, 24, or 32/);
  });
});

describe("HmacCodec", () => {
  it("round-trips with the mac prefixed", () => {
    const codec = new HmacCodec(HMAC_KEY);
    const data = Buffer.from("authenticated payload");
    const encoded = Buffer.from(codec.encode(data));
    expect(encoded.subarray(32).equals(data)).toBe(true); // [32B mac][body]
    expect(Buffer.from(codec.decode(encoded)).equals(data)).toBe(true);
  });

  it("rejects a tampered payload", () => {
    const codec = new HmacCodec(HMAC_KEY);
    const encoded = flipLastByte(codec.encode(Buffer.from("authenticated payload")));
    expect(() => codec.decode(encoded)).toThrow(CryptoError);
  });

  it("rejects a wrong key", () => {
    const encoded = new HmacCodec(HMAC_KEY).encode(Buffer.from("payload"));
    expect(() => new HmacCodec(Buffer.from("different-key")).decode(encoded)).toThrow(
      /signature mismatch/,
    );
  });

  it("rejects a short payload", () => {
    expect(() => new HmacCodec(HMAC_KEY).decode(Buffer.from("short"))).toThrow(/too short/);
  });

  it("rejects an empty key", () => {
    expect(() => new HmacCodec(new Uint8Array())).toThrow(/must not be empty/);
  });
});

describe("CodecSerializer", () => {
  it.each([
    ["json", new JsonSerializer()],
    ["msgpack", new MsgpackSerializer()],
  ])("chain [gzip, aes, hmac] is reversible over %s", (_name, delegate) => {
    const chain: PayloadCodec[] = [
      new GzipCodec(),
      new AesGcmCodec(AES_KEY),
      new HmacCodec(HMAC_KEY),
    ];
    const serializer = new CodecSerializer(delegate, chain);
    const value = { numbers: [1, 2, 3], text: "hello".repeat(50) };
    expect(serializer.deserialize(serializer.serialize(value))).toEqual(value);
  });

  it("frames the encoded bytes with the codec output", () => {
    const serializer = new CodecSerializer(new JsonSerializer(), [new GzipCodec()]);
    const encoded = Buffer.from(serializer.serialize({ key: "value" }));
    expect(encoded.subarray(0, 2).equals(Buffer.from([0x1f, 0x8b]))).toBe(true); // gzip magic
    expect(
      gunzipSync(encoded).equals(Buffer.from(new JsonSerializer().serialize({ key: "value" }))),
    ).toBe(true);
  });

  it("detects tampering before decompression (decode runs in reverse)", () => {
    const serializer = new CodecSerializer(new JsonSerializer(), [
      new GzipCodec(),
      new HmacCodec(HMAC_KEY),
    ]);
    const encoded = flipLastByte(serializer.serialize("payload"));
    expect(() => serializer.deserialize(encoded)).toThrow(/signature mismatch/);
  });

  it("is transparent with an empty chain", () => {
    const serializer = new CodecSerializer(new JsonSerializer(), []);
    const value = [1, "two", [3]];
    expect(serializer.deserialize(serializer.serialize(value))).toEqual(value);
  });
});

describe("Queue codec integration", () => {
  function tempDb(): string {
    return join(mkdtempSync(join(tmpdir(), "flexiq-codecs-")), "queue.db");
  }

  async function waitFor<T>(read: () => T | undefined, timeoutMs = 20_000): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const value = read();
      if (value !== undefined) {
        return value;
      }
      await new Promise((resolve) => setTimeout(resolve, 25));
    }
    throw new Error("timed out waiting for job result");
  }

  afterEach(() => {
    worker?.stop();
    worker = undefined;
  });

  it("global chain round-trips payload and result through a worker", async () => {
    const queue = new Queue({
      dbPath: tempDb(),
      codec: [new GzipCodec(), new HmacCodec(HMAC_KEY)],
    }).task("codecs.double", (x: number) => x * 2);

    const id = queue.enqueue("codecs.double", [21]);
    worker = queue.runWorker();
    await expect(waitFor(() => queue.getResult(id))).resolves.toBe(42);

    // The stored result is codec-framed: [32B mac][gzip(json)].
    const stored = queue.getJob(id)?.result;
    expect(stored).toBeDefined();
    const body = gunzipSync(Buffer.from(new HmacCodec(HMAC_KEY).decode(stored as Buffer)));
    expect(JSON.parse(body.toString("utf8"))).toBe(42);
  });

  it("per-task named codecs round-trip; results skip them", async () => {
    const queue = new Queue({
      dbPath: tempDb(),
      codecs: { gzip: new GzipCodec() },
    }).task("codecs.shout", (text: string) => text.toUpperCase(), { codecs: ["gzip"] });

    const id = queue.enqueue("codecs.shout", ["hello"]);
    worker = queue.runWorker();
    await expect(waitFor(() => queue.getResult(id))).resolves.toBe("HELLO");

    // The result is plain serializer output — per-task codecs never touch it.
    const stored = queue.getJob(id)?.result;
    expect(JSON.parse(Buffer.from(stored as Buffer).toString("utf8"))).toBe("HELLO");
  });

  it("enqueueMany applies per-task codecs to every entry", async () => {
    const queue = new Queue({
      dbPath: tempDb(),
      codecs: { gzip: new GzipCodec() },
    }).task("codecs.add", (a: number, b: number) => a + b, { codecs: ["gzip"] });

    const ids = queue.enqueueMany("codecs.add", [{ args: [1, 2] }, { args: [3, 4] }]);
    worker = queue.runWorker();
    await expect(waitFor(() => queue.getResult(ids[0] as string))).resolves.toBe(3);
    await expect(waitFor(() => queue.getResult(ids[1] as string))).resolves.toBe(7);
  });

  it("throws at enqueue for an unregistered codec name", () => {
    const queue = new Queue({ dbPath: tempDb() }).task("codecs.orphan", () => null, {
      codecs: ["nope"],
    });
    expect(() => queue.enqueue("codecs.orphan", [])).toThrow(/no codec registered named "nope"/);
  });
});
