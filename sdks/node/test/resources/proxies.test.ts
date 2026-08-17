import { mkdirSync, mkdtempSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import {
  canonicalJson,
  FileProxyHandler,
  FileReference,
  Proxies,
  ProxyError,
  type ProxyHandler,
  type ProxyRef,
  type ProxySession,
} from "../../src/index";

const KEY = Buffer.from("proxy-secret-key");

function fileProxies(allowedRoots: readonly string[] = []): Proxies {
  return new Proxies(KEY).register(new FileProxyHandler(allowedRoots));
}

function tempDir(): string {
  return mkdtempSync(join(tmpdir(), "flexiq-proxies-"));
}

/** Simulate the wire: serialize the ref to JSON and parse it back. */
function overTheWire(ref: ProxyRef): ProxyRef {
  return JSON.parse(JSON.stringify(ref)) as ProxyRef;
}

describe("Proxies", () => {
  it("round-trips a file ref across the wire", () => {
    const proxies = fileProxies();
    const path = join(tempDir(), "data.txt");
    const ref = overTheWire(proxies.deconstruct(new FileReference(path)));
    expect(proxies.resolve<FileReference>(ref).path).toBe(resolve(path));
  });

  it("rejects a tampered ref", () => {
    const proxies = fileProxies();
    const ref = proxies.deconstruct(new FileReference(join(tempDir(), "a")));
    const tampered: ProxyRef = { ...ref, reference: { path: "/etc/passwd" } };
    expect(() => proxies.reconstruct(tampered)).toThrow(ProxyError);
    expect(() => proxies.reconstruct(tampered)).toThrow(/signature mismatch/);
  });

  it("enforces the allowlist", () => {
    const dir = tempDir();
    const proxies = fileProxies([dir]);
    const inside = join(dir, "ok.txt");
    expect(
      proxies.resolve<FileReference>(proxies.deconstruct(new FileReference(inside))).path,
    ).toBe(resolve(inside));
    const outside = join(dir, "..", "outside.txt");
    const ref = proxies.deconstruct(new FileReference(outside));
    expect(() => proxies.reconstruct(ref)).toThrow(/not in allowlist/);
  });

  it("rejects a symlinked-ancestor escape from the allowlist", () => {
    const dir = tempDir();
    const allowed = join(dir, "allowed");
    const secret = join(dir, "secret");
    for (const d of [allowed, secret]) {
      writeFileSync(join(mkdirp(d), ".keep"), "");
    }
    writeFileSync(join(secret, "data.txt"), "top secret");
    // A symlink inside the allowed root pointing at the secret dir: lexically
    // under the allowlist, but its real target is not.
    symlinkSync(secret, join(allowed, "link"), "dir");

    const proxies = fileProxies([allowed]);
    const ref = proxies.deconstruct(new FileReference(join(allowed, "link", "data.txt")));
    expect(() => proxies.reconstruct(ref)).toThrow(/not in allowlist/);
  });

  it("rejects a value with no handler", () => {
    expect(() => fileProxies().deconstruct({ not: "a file" })).toThrow(/no proxy handler/);
  });

  it("rejects an unknown handler on reconstruct", () => {
    const proxies = fileProxies();
    const ref = proxies.deconstruct(new FileReference("/tmp/x"));
    expect(() => proxies.reconstruct({ ...ref, handler: "nope" })).toThrow(/unknown proxy handler/);
  });

  it("rejects a null value on deconstruct", () => {
    expect(() => fileProxies().deconstruct(null)).toThrow(/cannot deconstruct null/);
  });

  it("rejects a duplicate handler id", () => {
    expect(() => fileProxies().register(new FileProxyHandler())).toThrow(/already registered/);
  });

  it("rejects an empty key", () => {
    expect(() => new Proxies(new Uint8Array())).toThrow(/must not be empty/);
  });

  it("round-trips within the TTL", () => {
    const proxies = fileProxies();
    const path = join(tempDir(), "ttl.txt");
    const ref = proxies.deconstruct(new FileReference(path), { ttlMs: 60_000 });
    expect(proxies.resolve<FileReference>(ref).path).toBe(resolve(path));
  });

  it("rejects an expired ref", async () => {
    const proxies = fileProxies();
    const ref = proxies.deconstruct(new FileReference("/tmp/x"), { ttlMs: 1 });
    await new Promise((r) => setTimeout(r, 20));
    expect(() => proxies.reconstruct(ref)).toThrow(/expired/);
  });

  it("rejects a tampered expiry", () => {
    const proxies = fileProxies();
    const ref = proxies.deconstruct(new FileReference("/tmp/x"), { ttlMs: 1 });
    const tampered: ProxyRef = { ...ref, expiresAtMs: Date.now() + 3_600_000 };
    expect(() => proxies.reconstruct(tampered)).toThrow(/signature mismatch/);
  });

  it("enforces the purpose when requested", () => {
    const proxies = fileProxies();
    const path = join(tempDir(), "p.txt");
    const ref = proxies.deconstruct(new FileReference(path), { purpose: "emails" });
    expect(proxies.resolve<FileReference>(ref, "emails").path).toBe(resolve(path));
    expect(proxies.resolve<FileReference>(ref).path).toBe(resolve(path)); // unchecked
    expect(() => proxies.reconstruct(ref, "billing")).toThrow(/purpose mismatch/);
  });

  it("dispatches to the first handler that accepts, in registration order", () => {
    const calls: string[] = [];
    const handler = (id: string): ProxyHandler<string> => ({
      id,
      handles: (value) => typeof value === "string",
      deconstruct: (value) => {
        calls.push(id);
        return { value };
      },
      reconstruct: (reference) => String(reference.value),
    });
    const proxies = new Proxies(KEY).register(handler("first")).register(handler("second"));
    expect(proxies.deconstruct("hello").handler).toBe("first");
    expect(calls).toEqual(["first"]);
  });
});

describe("canonicalJson", () => {
  it("sorts keys recursively and stays compact", () => {
    expect(canonicalJson({ b: { d: 1, c: 2 }, a: [3, { z: 4, y: 5 }] })).toBe(
      '{"a":[3,{"y":5,"z":4}],"b":{"c":2,"d":1}}',
    );
  });

  it("sorts integer-like keys lexically, not numerically", () => {
    expect(canonicalJson({ "10": "ten", "2": "two" })).toBe('{"10":"ten","2":"two"}');
  });

  it("drops undefined object values and nullifies undefined array items", () => {
    expect(canonicalJson({ a: undefined, b: 1 })).toBe('{"b":1}');
    expect(canonicalJson([undefined, 1])).toBe("[null,1]");
  });

  it("rejects non-safe-integer numbers", () => {
    expect(() => canonicalJson({ a: 1.5 })).toThrow(ProxyError);
    expect(() => canonicalJson({ a: Number.MAX_SAFE_INTEGER + 1 })).toThrow(/safe integers/);
  });
});

describe("cross-SDK contract", () => {
  it("produces the contract vector signature", () => {
    // A verbatim handler keeps the reference platform-independent — the file
    // handler would resolve the path against the local filesystem root.
    const verbatim: ProxyHandler<string> = {
      id: "file",
      handles: (value) => typeof value === "string",
      deconstruct: (value) => ({ path: value }),
      reconstruct: (reference) => String(reference.path),
    };
    const proxies = new Proxies(KEY).register(verbatim);
    const ref = proxies.deconstruct("/tmp/data.txt");
    expect(ref.signature).toBe("FgmudNqaGsUBFsIKC4uBgtfZ+IAHrzBT+xRjUWGePyQ=");
    expect(proxies.resolve<string>(ref)).toBe("/tmp/data.txt");
  });
});

/** Counts handler invocations and records cleaned values, for session tests. */
class CountingFileHandler implements ProxyHandler<FileReference> {
  private readonly delegate = new FileProxyHandler();
  readonly id = this.delegate.id;
  deconstructs = 0;
  reconstructs = 0;
  readonly cleaned: FileReference[] = [];

  handles(value: unknown): boolean {
    return this.delegate.handles(value);
  }

  deconstruct(value: FileReference): Record<string, unknown> {
    this.deconstructs += 1;
    return this.delegate.deconstruct(value);
  }

  reconstruct(reference: Record<string, unknown>): FileReference {
    this.reconstructs += 1;
    return this.delegate.reconstruct(reference);
  }

  cleanup(value: FileReference): void {
    this.cleaned.push(value);
  }
}

describe("ProxySession", () => {
  it("dedups the same instance on deconstruct", () => {
    const handler = new CountingFileHandler();
    const proxies = new Proxies(KEY).register(handler);
    const file = new FileReference(join(tempDir(), "dedup.txt"));
    const session = proxies.session();
    expect(session.deconstruct(file)).toBe(session.deconstruct(file));
    session.close();
    expect(handler.deconstructs).toBe(1);
  });

  it("keeps purposes distinct", () => {
    const proxies = fileProxies();
    const file = new FileReference(join(tempDir(), "purpose.txt"));
    const session = proxies.session();
    const emails = session.deconstruct(file, { purpose: "emails" });
    const billing = session.deconstruct(file, { purpose: "billing" });
    expect(emails).not.toBe(billing);
    expect(proxies.resolve<FileReference>(emails, "emails").path).toBe(resolve(file.path));
    expect(proxies.resolve<FileReference>(billing, "billing").path).toBe(resolve(file.path));
    session.close();
  });

  it("memoizes reconstruct by signature", () => {
    const handler = new CountingFileHandler();
    const proxies = new Proxies(KEY).register(handler);
    const ref = proxies.deconstruct(new FileReference(join(tempDir(), "memo.txt")));
    const session = proxies.session();
    expect(session.reconstruct(ref)).toBe(session.reconstruct(ref));
    session.close();
    expect(handler.reconstructs).toBe(1);
  });

  it("runs cleanup once per instance, LIFO", () => {
    const handler = new CountingFileHandler();
    const proxies = new Proxies(KEY).register(handler);
    const dir = tempDir();
    const refA = proxies.deconstruct(new FileReference(join(dir, "a.txt")));
    const refB = proxies.deconstruct(new FileReference(join(dir, "b.txt")));
    const session = proxies.session();
    const a = session.resolve<FileReference>(refA);
    const b = session.resolve<FileReference>(refB);
    session.resolve(refA); // memo hit — must not add a second cleanup
    session.close();
    session.close(); // idempotent
    expect(handler.cleaned).toEqual([b, a]);
  });

  it("isolates sessions from each other", () => {
    const handler = new CountingFileHandler();
    const proxies = new Proxies(KEY).register(handler);
    const ref = proxies.deconstruct(new FileReference(join(tempDir(), "iso.txt")));
    const one = proxies.session();
    const two = proxies.session();
    const fromOne = one.resolve<FileReference>(ref);
    const fromTwo = two.resolve<FileReference>(ref);
    expect(fromOne).not.toBe(fromTwo);
    one.close();
    expect(handler.cleaned).toEqual([fromOne]); // closing one leaves the other live
    two.close();
    expect(handler.cleaned).toEqual([fromOne, fromTwo]);
  });

  it("re-verifies on a memo hit", async () => {
    const proxies = fileProxies();
    const ref = proxies.deconstruct(new FileReference(join(tempDir(), "ttl.txt")), { ttlMs: 30 });
    const session = proxies.session();
    session.resolve(ref);
    await new Promise((r) => setTimeout(r, 80));
    expect(() => session.resolve(ref)).toThrow(ProxyError);
    expect(() => session.resolve(ref)).toThrow(/expired/);
    session.close();
  });

  it("drains the remaining cleanups when one throws", () => {
    const cleaned: FileReference[] = [];
    const delegate = new FileProxyHandler();
    const proxies = new Proxies(KEY).register({
      id: delegate.id,
      handles: (value) => delegate.handles(value),
      deconstruct: (value: FileReference) => delegate.deconstruct(value),
      reconstruct: (reference) => delegate.reconstruct(reference),
      cleanup: (value: FileReference) => {
        cleaned.push(value);
        if (value.path.endsWith("boom.txt")) {
          throw new Error("cleanup failed");
        }
      },
    });
    const dir = tempDir();
    const refA = proxies.deconstruct(new FileReference(join(dir, "a.txt")));
    const refBoom = proxies.deconstruct(new FileReference(join(dir, "boom.txt")));
    const session = proxies.session();
    session.resolve(refA);
    session.resolve(refBoom); // cleaned last-in-first-out → throws first
    expect(() => session.close()).not.toThrow();
    expect(cleaned).toHaveLength(2); // the failure must not abandon the rest
  });

  it("rejects use after close", () => {
    const proxies = fileProxies();
    const file = new FileReference(join(tempDir(), "closed.txt"));
    const ref = proxies.deconstruct(file);
    const session: ProxySession = proxies.session();
    session.close();
    expect(() => session.deconstruct(file)).toThrow(ProxyError);
    expect(() => session.deconstruct(file)).toThrow(/session is closed/);
    expect(() => session.reconstruct(ref)).toThrow(/session is closed/);
    expect(() => session.resolve(ref)).toThrow(/session is closed/);
  });
});

function mkdirp(dir: string): string {
  mkdirSync(dir, { recursive: true });
  return dir;
}
