// `enqueuedBy` names the token a server door authenticated. An in-process
// enqueue presents no token, so nothing may claim a submitter for it.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { jobToContract } from "../../src/dashboard/handlers/contract";
import { Queue } from "../../src/index";

it("leaves an in-process job without a submitter", () => {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-enqueued-by-")), "queue.db");
  const queue = new Queue({ dbPath });
  queue.task("work", () => undefined);

  const job = queue.getJob(queue.enqueue("work", []));
  if (!job) throw new Error("the job was just enqueued");
  expect(job.enqueuedBy ?? null).toBeNull();
  // The dashboard contract carries the key either way, as every server does.
  expect(jobToContract(job)).toHaveProperty("enqueued_by", null);
});
