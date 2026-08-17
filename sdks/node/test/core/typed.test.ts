import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

it("infers enqueue argument types from the registered task", async () => {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-typed-")), "q.db");
  const queue = new Queue({ dbPath })
    .task("add", (a: number, b: number) => a + b)
    .task("greet", (name: string) => `hi ${name}`);

  const id = queue.enqueue("add", [2, 3]);
  queue.enqueue("greet", ["world"]);

  // @ts-expect-error wrong argument types for a registered task
  queue.enqueue("add", ["x", "y"]);

  worker = queue.runWorker();
  expect(await queue.result(id)).toBe(5);
});
