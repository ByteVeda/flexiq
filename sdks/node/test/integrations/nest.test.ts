import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Test, type TestingModule } from "@nestjs/testing";
import { afterEach, expect, it } from "vitest";
import { FlexiQModule, FlexiQService } from "../../src/contrib/nest";
import { Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;
let moduleRef: TestingModule | undefined;

afterEach(async () => {
  worker?.stop();
  worker = undefined;
  await moduleRef?.close();
  moduleRef = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-nest-")), "q.db") });
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 4000,
): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return false;
}

it("injects FlexiQService bound to the queue", async () => {
  const queue = newQueue();
  queue.task("add", (a: number, b: number) => a + b);

  moduleRef = await Test.createTestingModule({
    imports: [FlexiQModule.forRoot(queue)],
  }).compile();
  const service = moduleRef.get(FlexiQService);

  expect(service.queue).toBe(queue);

  const id = service.enqueue("add", [6, 7]);
  worker = queue.runWorker();
  expect(await waitFor(async () => (await queue.stats()).completed >= 1)).toBe(true);

  expect(await service.result(id)).toBe(13);
  expect((await service.stats()).completed).toBeGreaterThanOrEqual(1);
});
