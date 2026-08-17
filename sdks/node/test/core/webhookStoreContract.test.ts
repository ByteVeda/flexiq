import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue } from "../../src/index";

// The cross-SDK layout: one JSON array, snake_case fields, timeout in seconds.
const SUBSCRIPTIONS_KEY = "webhooks:subscriptions";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-whs-")), "q.db") });
}

function rows(queue: Queue): Record<string, unknown>[] {
  return JSON.parse(queue.getSetting(SUBSCRIPTIONS_KEY) ?? "[]");
}

it("persists subscriptions as one contract-shaped list", () => {
  const queue = newQueue();
  const created = queue.webhooks.create({
    url: "https://example.com/hook",
    events: ["job.completed"],
    taskFilter: ["send_email"],
    headers: { "x-api-key": "k" },
    maxRetries: 5,
    timeoutMs: 2_500,
    retryBackoff: 3,
    description: "billing",
  });

  const persisted = rows(queue);
  expect(persisted).toHaveLength(1);
  expect(persisted[0]).toEqual({
    id: created.id,
    url: "https://example.com/hook",
    events: ["job.completed"],
    task_filter: ["send_email"],
    headers: { "x-api-key": "k" },
    secret: null,
    max_retries: 5,
    timeout_seconds: 2.5,
    retry_backoff: 3,
    enabled: true,
    description: "billing",
    created_at: created.createdAt,
    updated_at: created.updatedAt,
  });
  // No per-webhook keys are written any more.
  expect(Object.keys(queue.listSettings()).filter((key) => key.startsWith("webhook:"))).toEqual([]);
});

it("reads a subscription written by another runtime", () => {
  const queue = newQueue();
  queue.setSetting(
    SUBSCRIPTIONS_KEY,
    JSON.stringify([
      {
        id: "abc",
        url: "https://example.com/other",
        events: [],
        task_filter: null,
        headers: {},
        secret: "s3cr3t",
        max_retries: 7,
        timeout_seconds: 30,
        retry_backoff: 1.5,
        enabled: false,
        description: null,
        created_at: 1,
        updated_at: 2,
      },
    ]),
  );

  const webhook = queue.webhooks.get("abc");
  expect(webhook).toMatchObject({
    id: "abc",
    url: "https://example.com/other",
    events: [],
    taskFilter: undefined,
    secret: "s3cr3t",
    maxRetries: 7,
    timeoutMs: 30_000,
    retryBackoff: 1.5,
    enabled: false,
    description: undefined,
  });
  expect(queue.webhooks.list()).toHaveLength(1);
});

it("round-trips the expanded event names", () => {
  const queue = newQueue();
  const events = [
    "job.enqueued",
    "worker.online",
    "queue.paused",
    "workflow.gate_reached",
    "predicate.rejected",
  ] as const;
  const created = queue.webhooks.create({
    url: "https://example.com/hook",
    events: [...events],
  });

  expect(rows(queue)[0]?.events).toEqual([...events]);
  expect(queue.webhooks.get(created.id)?.events).toEqual([...events]);
});

it("keeps event names it does not model", () => {
  const queue = newQueue();
  queue.setSetting(
    SUBSCRIPTIONS_KEY,
    JSON.stringify([
      { id: "abc", url: "https://example.com/h", events: ["workflow.completed"], enabled: true },
    ]),
  );

  expect(queue.webhooks.get("abc")?.events).toEqual(["workflow.completed"]);
});

it("carries unmodelled fields through a rewrite", () => {
  const queue = newQueue();
  queue.setSetting(
    SUBSCRIPTIONS_KEY,
    JSON.stringify([
      { id: "abc", url: "https://example.com/h", enabled: true, future_field: { keep: true } },
    ]),
  );

  queue.webhooks.update("abc", { description: "renamed" });

  const persisted = rows(queue);
  expect(persisted[0]?.future_field).toEqual({ keep: true });
  expect(persisted[0]?.description).toBe("renamed");
});

it("migrates legacy per-webhook keys into the list", () => {
  const queue = newQueue();
  queue.setSetting(
    "webhook:legacy-1",
    JSON.stringify({
      id: "legacy-1",
      url: "https://example.com/legacy",
      events: ["job.dead"],
      headers: {},
      taskFilter: ["report"],
      enabled: true,
      maxRetries: 4,
      timeoutMs: 5_000,
      createdAt: 10,
      updatedAt: 11,
    }),
  );

  const migrated = queue.webhooks.get("legacy-1");
  expect(migrated).toMatchObject({
    url: "https://example.com/legacy",
    events: ["job.dead"],
    taskFilter: ["report"],
    maxRetries: 4,
    timeoutMs: 5_000,
    retryBackoff: 2,
  });
  expect(rows(queue)).toHaveLength(1);
  expect(queue.getSetting("webhook:legacy-1")).toBeNull();
});

it("prefers the contract row when a legacy key duplicates its id", () => {
  const queue = newQueue();
  queue.setSetting(
    SUBSCRIPTIONS_KEY,
    JSON.stringify([{ id: "dup", url: "https://example.com/current", enabled: true }]),
  );
  queue.setSetting(
    "webhook:dup",
    JSON.stringify({ id: "dup", url: "https://example.com/stale", enabled: true }),
  );

  expect(queue.webhooks.get("dup")?.url).toBe("https://example.com/current");
  expect(queue.webhooks.list()).toHaveLength(1);
});

it("survives a corrupt list and a corrupt row", () => {
  const corrupt = newQueue();
  corrupt.setSetting(SUBSCRIPTIONS_KEY, "{not json");
  expect(corrupt.webhooks.list()).toEqual([]);

  const partial = newQueue();
  partial.setSetting(
    SUBSCRIPTIONS_KEY,
    JSON.stringify([{ url: "https://example.com/no-id" }, { id: "ok", url: "https://e.com/h" }]),
  );
  expect(partial.webhooks.list().map((webhook) => webhook.id)).toEqual(["ok"]);
});

it("updates and deletes in place without disturbing siblings", () => {
  const queue = newQueue();
  const first = queue.webhooks.create({ url: "https://example.com/a" });
  const second = queue.webhooks.create({ url: "https://example.com/b" });

  queue.webhooks.update(first.id, { enabled: false });
  expect(rows(queue).map((row) => row.id)).toEqual([first.id, second.id]);

  expect(queue.webhooks.delete(first.id)).toBe(true);
  expect(queue.webhooks.list().map((webhook) => webhook.id)).toEqual([second.id]);
  expect(queue.webhooks.delete(first.id)).toBe(false);
});
