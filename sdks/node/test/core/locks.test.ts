import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { LockNotAcquiredError, Queue } from "../../src/index";

function freshQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-lock-")), "queue.db");
  return new Queue({ dbPath });
}

it("grants the lock to one owner at a time", () => {
  const queue = freshQueue();
  const a = queue.lock("resource", { autoExtend: false });
  const b = queue.lock("resource", { autoExtend: false });

  expect(a.acquire()).toBe(true);
  expect(b.acquire()).toBe(false);
  expect(queue.lock("resource").info()?.ownerId).toBe(a.ownerId);

  expect(a.release()).toBe(true);
  expect(b.acquire()).toBe(true);
  b.release();
});

it("runs withLock then releases", async () => {
  const queue = freshQueue();
  let ran = false;
  const result = await queue.withLock(
    "job",
    () => {
      ran = true;
      expect(queue.lock("job").info()).toBeDefined();
      return 42;
    },
    { autoExtend: false },
  );

  expect(ran).toBe(true);
  expect(result).toBe(42);
  expect(queue.lock("job").info()).toBeUndefined();
});

it("rejects withLock when the lock is held", async () => {
  const queue = freshQueue();
  const holder = queue.lock("busy", { autoExtend: false });
  holder.acquire();

  await expect(queue.withLock("busy", () => 1, { autoExtend: false })).rejects.toBeInstanceOf(
    LockNotAcquiredError,
  );
  holder.release();
});

it("extends the expiry", () => {
  const queue = freshQueue();
  const lock = queue.lock("ext", { ttlMs: 1000, autoExtend: false });
  lock.acquire();

  const before = queue.lock("ext").info()?.expiresAt ?? 0;
  expect(lock.extend(60_000)).toBe(true);
  const after = queue.lock("ext").info()?.expiresAt ?? 0;
  expect(after).toBeGreaterThan(before);
  lock.release();
});

it("releases via Symbol.dispose", () => {
  const queue = freshQueue();
  const lock = queue.lock("disp", { autoExtend: false });
  lock.acquire();
  expect(queue.lock("disp").info()).toBeDefined();

  lock[Symbol.dispose]();
  expect(queue.lock("disp").info()).toBeUndefined();
});

it("rejects a non-positive or non-finite ttl at construction", () => {
  const queue = freshQueue();
  expect(() => queue.lock("bad", { ttlMs: 0 })).toThrow(RangeError);
  expect(() => queue.lock("bad", { ttlMs: -5 })).toThrow(RangeError);
  expect(() => queue.lock("bad", { ttlMs: Number.NaN })).toThrow(RangeError);
});
