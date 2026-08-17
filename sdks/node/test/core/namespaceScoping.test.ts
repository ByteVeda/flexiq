// A namespace is a tenancy boundary: a caller scoped to one must learn nothing
// about ids outside it — not through a read, and not through the effect of a
// write. `undefined` stays unscoped and addresses every namespace.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue } from "../../src/index";

function sharedDb(): string {
  return join(mkdtempSync(join(tmpdir(), "flexiq-ns-")), "queue.db");
}

it("scopes the id-addressed surface to the queue's namespace", async () => {
  const dbPath = sharedDb();
  const a = new Queue({ dbPath, namespace: "ns-a" });
  const b = new Queue({ dbPath, namespace: "ns-b" });
  const unscoped = new Queue({ dbPath });

  a.task("work", () => undefined);
  const id = a.enqueue("work", []);

  expect(a.getJob(id)).not.toBeNull();
  expect(b.getJob(id)).toBeNull();
  expect(unscoped.getJob(id)).not.toBeNull();

  expect(b.cancelJob(id)).toBe(false);
  expect(b.requestCancel(id)).toBe(false);
  expect(a.getJob(id)).not.toBeNull();

  expect(await b.getJobErrors(id)).toEqual([]);
  expect(b.taskLogs(id)).toEqual([]);

  expect(a.cancelJob(id)).toBe(true);
});

it("scopes the archive listing", async () => {
  const dbPath = sharedDb();
  const a = new Queue({ dbPath, namespace: "ns-a" });
  const b = new Queue({ dbPath, namespace: "ns-b" });
  const unscoped = new Queue({ dbPath });

  for (const queue of [a, b]) {
    queue.task("work", () => undefined);
    queue.cancelJob(queue.enqueue("work", []));
  }

  expect(await a.listArchived()).toHaveLength(1);
  expect((await a.listArchived())[0]?.namespace).toBe("ns-a");
  expect(await unscoped.listArchived()).toHaveLength(2);
  expect((await a.listArchivedAfter()).items).toHaveLength(1);
});
