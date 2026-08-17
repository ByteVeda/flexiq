import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { type Job, type Page, Queue } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-pg-")), "q.db") });
}

/** Walk every page, collecting ids, so a dropped or repeated row shows up. */
async function walk(
  fetch: (after?: string) => Promise<Page<Job>>,
): Promise<{ ids: string[]; pages: number }> {
  const ids: string[] = [];
  let cursor: string | undefined;
  let pages = 0;
  do {
    const page: Page<Job> = await fetch(cursor);
    ids.push(...page.items.map((job) => job.id));
    cursor = page.nextCursor ?? undefined;
    pages += 1;
    if (pages > 20) {
      throw new Error("pagination did not terminate");
    }
  } while (cursor !== undefined);
  return { ids, pages };
}

it("walks every job exactly once across pages", async () => {
  const queue = newQueue();
  queue.task("p", (n: number) => n);
  const enqueued: string[] = [];
  for (let i = 0; i < 5; i += 1) {
    enqueued.push(queue.enqueue("p", [i]));
  }

  const { ids, pages } = await walk((after) => queue.listJobsAfter({ limit: 2 }, after));

  expect(pages).toBeGreaterThan(1);
  expect(ids).toHaveLength(5);
  expect(new Set(ids).size).toBe(5);
  expect([...ids].sort()).toEqual([...enqueued].sort());
});

it("ends the walk with a null cursor rather than looping", async () => {
  const queue = newQueue();
  queue.task("p", (n: number) => n);
  await queue.enqueue("p", [1]);

  // A page shorter than the limit is the last one.
  const page = await queue.listJobsAfter({ limit: 10 });
  expect(page.items).toHaveLength(1);
  expect(page.nextCursor).toBeNull();
});

it("carries the wider filter across pages", async () => {
  const queue = newQueue();
  queue.task("p", (n: number) => n);
  for (let i = 0; i < 4; i += 1) {
    await queue.enqueue("p", [i], { metadata: i < 3 ? "keep" : "drop" });
  }

  const { ids } = await walk((after) =>
    queue.listJobsFilteredAfter({ metadataLike: "keep", limit: 2 }, after),
  );
  expect(ids).toHaveLength(3);
});

it("rejects an offset rather than silently returning a different page", async () => {
  // Offset and cursor paging do not compose — a cursor already says where to
  // resume — so honouring one and ignoring the other would quietly hand back a
  // page the caller did not ask for. The TS type omits the field; this covers a
  // caller who reaches past it.
  const queue = newQueue();
  await expect(queue.listJobsAfter({ limit: 2, offset: 10 } as never)).rejects.toThrow(
    /offset is not supported/,
  );
});

it("rejects a malformed cursor rather than restarting", async () => {
  const queue = newQueue();
  await expect(queue.listJobsAfter({ limit: 2 }, "nocolon")).rejects.toThrow(/cursor/);
  await expect(queue.deadLettersAfter(2, "-1:abc")).rejects.toThrow(/cursor/);
});
