// Asserts the shared cross-SDK wire vectors.
//
// `contracts/wire-vectors.json` pins the bytes of the CBOR call envelope. Every
// SDK runs this same file against its own serializer, so an encoding change
// fails the runtime that made it instead of quietly producing payloads its peers
// cannot read.

import { existsSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { CborSerializer } from "../../src/serializers";

interface EncodeCase {
  name: string;
  args: unknown[];
  kwargs: Record<string, unknown>;
  hex: string;
}

interface DecodeCase {
  name: string;
  hex: string;
  args?: unknown[];
  kwargs?: Record<string, unknown>;
  round_trip_only?: boolean;
}

/** Walk up to the repository root rather than counting directories. */
function repoRoot(): string {
  let dir = dirname(fileURLToPath(import.meta.url));
  for (;;) {
    if (existsSync(join(dir, "contracts", "wire-vectors.json"))) {
      return dir;
    }
    const parent = resolve(dir, "..");
    if (parent === dir) {
      throw new Error("contracts/wire-vectors.json not found above this test");
    }
    dir = parent;
  }
}

const ROOT = repoRoot();
const vectors: { encode: EncodeCase[]; decode_only: DecodeCase[] } = JSON.parse(
  readFileSync(join(ROOT, "contracts", "wire-vectors.json"), "utf8"),
);
const BINDING_CONTRACT = join(ROOT, "crates", "taskito-core", "BINDING_CONTRACT.md");

// A run of space-separated hex byte pairs inside backticks, as the doc writes them.
const DOCUMENTED_BYTES = /`((?:[0-9a-f]{2} )+[0-9a-f]{2})`/;

const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");

/**
 * JavaScript has no keyword arguments, so `serializeCall` always writes an empty
 * kwargs map and cannot produce these cases. Decoding them is still required —
 * a producer in a runtime that does have keyword arguments can enqueue one.
 */
const encodable = vectors.encode.filter((c) => Object.keys(c.kwargs).length === 0);

describe("cross-SDK wire vectors", () => {
  it.each(
    encodable.map((c) => [c.name, c] as const),
  )("encodes %s to the pinned bytes", (_name, testCase) => {
    expect(hex(new CborSerializer().serializeCall(testCase.args))).toBe(testCase.hex);
  });

  it.each(
    vectors.encode.map((c) => [c.name, c] as const),
  )("decodes %s from the pinned bytes", (_name, testCase) => {
    const decoded = new CborSerializer().deserialize(Buffer.from(testCase.hex, "hex"));
    expect(decoded).toEqual([testCase.args, testCase.kwargs]);
  });

  it.each(vectors.decode_only.map((c) => [c.name, c] as const))("decodes %s", (_name, testCase) => {
    const serializer = new CborSerializer();
    const raw = Buffer.from(testCase.hex, "hex");
    const [args, kwargs] = serializer.deserialize(raw) as [unknown[], Record<string, unknown>];

    if (testCase.round_trip_only) {
      // The value has no lossless JSON form, so re-encoding is the assertion.
      expect(kwargs).toEqual({});
      expect(hex(serializer.serializeCall(args))).toBe(testCase.hex);
    } else {
      expect(args).toEqual(testCase.args);
      expect(kwargs).toEqual(testCase.kwargs);
    }
  });

  // BINDING_CONTRACT.md restates the call vector; drift there is a silent contract change.
  it("keeps the vector quoted in BINDING_CONTRACT.md", () => {
    const quoted = readFileSync(BINDING_CONTRACT, "utf8")
      .split("\n")
      .find((line) => line.includes('call `f(1, "a")`'));
    if (quoted === undefined) {
      throw new Error(`no documented call vector in ${BINDING_CONTRACT}`);
    }

    const documented = DOCUMENTED_BYTES.exec(quoted)?.[1];
    if (documented === undefined) {
      throw new Error(`no hex byte run in: ${quoted}`);
    }

    const shared = vectors.encode.find((c) => c.name === "contract-vector");
    expect(documented.replaceAll(" ", "")).toBe(shared?.hex);
  });
});
