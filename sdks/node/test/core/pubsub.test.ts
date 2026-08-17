import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, expect, it } from "vitest";
import { Queue, type Worker } from "../../src/index";
import type { NativeQueue } from "../../src/native";

let worker: Worker | undefined;
afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-pubsub-")), "q.db") });
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
    await new Promise((r) => setTimeout(r, 20));
  }
  return false;
}

it("declares and lists subscriptions", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  queue.subscriber("orders", "track_order", () => undefined, { subscriptionName: "analytics" });
  await queue.declareSubscriptions();

  const subs = await queue.listSubscriptions("orders");
  expect(new Set(subs.map((s) => s.subscriptionName))).toEqual(new Set(["email", "analytics"]));
  expect(subs.every((s) => s.active && s.durable)).toBe(true);
  expect(await queue.listTopics()).toEqual(["orders"]);
});

it("subscription name defaults to the task name", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "handle_order", () => undefined);
  await queue.declareSubscriptions();

  const [sub] = await queue.listSubscriptions("orders");
  expect(sub?.subscriptionName).toBe("handle_order");
  expect(sub?.taskName).toBe("handle_order");
});

it("redeclaring updates the subscription instead of duplicating", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, {
    subscriptionName: "email",
    queue: "mail",
  });
  await queue.declareSubscriptions();
  await queue.declareSubscriptions();

  const subs = await queue.listSubscriptions();
  expect(subs).toHaveLength(1);
  expect(subs[0]?.queue).toBe("mail");
});

it("re-registering the same (topic, name) replaces the pending entry", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, {
    subscriptionName: "email",
    queue: "mail",
  });
  queue.subscriber("orders", "send_email_v2", () => undefined, {
    subscriptionName: "email",
    queue: "mail-v2",
  });
  await queue.declareSubscriptions();

  const subs = await queue.listSubscriptions("orders");
  expect(subs).toHaveLength(1);
  expect(subs[0]?.taskName).toBe("send_email_v2");
  expect(subs[0]?.queue).toBe("mail-v2");
});

it("subscriber rejects per-task codecs", () => {
  const queue = newQueue();
  expect(() =>
    queue.subscriber("orders", "send_email", () => undefined, { codecs: ["gzip"] }),
  ).toThrow(/per-task codecs do not apply/);
});

it("registering an ephemeral subscription without an owner rejects", async () => {
  const queue = newQueue();
  const native = (queue as unknown as { native: NativeQueue }).native;
  await expect(
    native.registerSubscription("orders", "eph", "eph_task", "default", false, undefined),
  ).rejects.toThrow(/ephemeral subscription \(durable=false\) requires ownerWorkerId/);
});

it("persists a subscriber's delivery settings for a producer-only publisher", async () => {
  // Two Queue instances on the same DB: the consumer registers the subscriber
  // (persisting its maxRetries on the subscription row), the producer only
  // publishes and never registers the task — yet the delivery must honor 7.
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-pubsub-")), "q.db");
  const consumer = new Queue({ dbPath });
  consumer.subscriber("orders", "send_email", () => undefined, {
    subscriptionName: "email",
    maxRetries: 7,
  });
  await consumer.declareSubscriptions();

  const producer = new Queue({ dbPath });
  const [job] = await producer.publish("orders", [1]);
  expect(job).toBeDefined();
  expect(producer.getJob(job?.id ?? "")?.maxRetries).toBe(7);
});

it("unsubscribe removes the subscription and reports a missing one", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  await queue.declareSubscriptions();

  expect(await queue.unsubscribe("orders", "email")).toBe(true);
  expect(await queue.unsubscribe("orders", "email")).toBe(false);
  expect(await queue.listSubscriptions("orders")).toEqual([]);
});

it("a later declareSubscriptions does not resurrect an unsubscribed subscriber", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  await queue.declareSubscriptions();

  expect(await queue.unsubscribe("orders", "email")).toBe(true);
  await queue.declareSubscriptions();
  expect(await queue.listSubscriptions("orders")).toEqual([]);
});

it("redeclaring keeps a paused subscription paused", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  await queue.declareSubscriptions();

  expect(await queue.pauseSubscription("orders", "email")).toBe(true);
  await queue.declareSubscriptions();
  expect(await queue.publish("orders", [1])).toEqual([]);
});

it("pause blocks deliveries and resume restores them", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  await queue.declareSubscriptions();

  expect(await queue.pauseSubscription("orders", "email")).toBe(true);
  expect(await queue.publish("orders", [1])).toEqual([]);
  expect(await queue.resumeSubscription("orders", "email")).toBe(true);
  expect(await queue.publish("orders", [2])).toHaveLength(1);
});

it("publishing to a topic without subscribers is a no-op", async () => {
  const queue = newQueue();
  expect(await queue.publish("ghost-topic", [1])).toEqual([]);
});

it("fans a publish out to every subscriber", async () => {
  const queue = newQueue();
  const received = new Map<string, number>();
  queue.subscriber("orders", "send_email", (orderId: number) => {
    received.set("email", orderId);
  });
  queue.subscriber("orders", "track_order", (orderId: number) => {
    received.set("analytics", orderId);
  });
  await queue.declareSubscriptions();

  const jobs = await queue.publish("orders", [42]);
  expect(jobs).toHaveLength(2);

  worker = queue.runWorker();
  expect(await waitFor(() => received.size === 2)).toBe(true);
  expect(received.get("email")).toBe(42);
  expect(received.get("analytics")).toBe(42);
});

it("a failing subscriber does not affect its sibling", async () => {
  const queue = newQueue();
  const outcomes: string[] = [];
  queue.subscriber(
    "orders",
    "flaky",
    () => {
      throw new Error("boom");
    },
    { maxRetries: 0 },
  );
  queue.subscriber("orders", "steady", () => {
    outcomes.push("steady");
  });
  await queue.declareSubscriptions();

  const jobs = await queue.publish("orders", [7]);
  expect(jobs).toHaveLength(2);

  worker = queue.runWorker();
  expect(await waitFor(() => outcomes.length === 1)).toBe(true);

  const statusOf = (taskName: string): string | undefined =>
    queue.getJob(jobs.find((job) => job.taskName === taskName)?.id ?? "")?.status;
  expect(await waitFor(() => statusOf("steady") === "complete")).toBe(true);
  expect(await waitFor(() => ["failed", "dead"].includes(statusOf("flaky") ?? ""))).toBe(true);
});

it("idempotencyKey dedupes per subscriber across republishes", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  queue.subscriber("orders", "track_order", () => undefined, { subscriptionName: "analytics" });
  await queue.declareSubscriptions();

  const first = await queue.publish("orders", [42], { idempotencyKey: "evt-42" });
  const second = await queue.publish("orders", [42], { idempotencyKey: "evt-42" });
  expect(first).toHaveLength(2);
  expect(second).toHaveLength(2);
  expect(new Set(second.map((job) => job.id))).toEqual(new Set(first.map((job) => job.id)));
});

it("deliveries carry topic and subscription notes", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  await queue.declareSubscriptions();

  const [job] = await queue.publish("orders", [1], { notes: { tenant: "acme" } });
  const notes = JSON.parse(job?.notes ?? "{}");
  expect(notes.topic).toBe("orders");
  expect(notes.subscription).toBe("email");
  expect(notes.tenant).toBe("acme");
});

it("declareSubscriptions skips ephemeral subscriptions", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "durable_task", () => undefined);
  queue.subscriber("orders", "ephemeral_task", () => undefined, { durable: false });
  await queue.declareSubscriptions();

  const subs = await queue.listSubscriptions("orders");
  expect(subs.map((s) => s.subscriptionName)).toEqual(["durable_task"]);
});

it("reapEphemeralSubscriptions spares fresh rows even with a dead owner", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "durable_task", () => undefined);
  queue.subscriber("orders", "ephemeral_task", () => undefined, { durable: false });

  // A running worker flushes both, owning the ephemeral one.
  worker = queue.runWorker();
  expect(await waitFor(async () => (await queue.listSubscriptions("orders")).length === 2)).toBe(
    true,
  );
  // Owner alive: the reap must not touch its ephemeral subscription.
  expect(await queue.reapEphemeralSubscriptions()).toBe(0);

  // Stop the worker (unregisters it). The orphaned row is younger than the
  // startup grace window, so it survives — a starting worker inserts its
  // ephemeral subscriptions before its first heartbeat, and another worker's
  // reap tick must not race that gap. Aged reaping is covered in core tests.
  worker.stop();
  worker = undefined;
  expect(await waitFor(async () => (await queue.listWorkers()).length === 0)).toBe(true);
  expect(await queue.reapEphemeralSubscriptions()).toBe(0);

  const remaining = await queue.listSubscriptions("orders");
  expect(new Set(remaining.map((s) => s.subscriptionName))).toEqual(
    new Set(["durable_task", "ephemeral_task"]),
  );
});

it("topicStats reports one backlog row per subscription", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  queue.subscriber("orders", "track_order", () => undefined, { subscriptionName: "analytics" });
  await queue.declareSubscriptions();
  await queue.publish("orders", [1]);
  await queue.publish("orders", [2]);

  const stats = new Map((await queue.topicStats("orders")).map((s) => [s.subscription, s]));
  expect(new Set(stats.keys())).toEqual(new Set(["email", "analytics"]));
  const email = stats.get("email");
  expect(email?.taskName).toBe("send_email");
  expect(email?.queue).toBe("default");
  expect(email?.active).toBe(true);
  expect(email?.durable).toBe(true);
  expect(email?.pending).toBe(2);
  expect(email?.running).toBe(0);
  expect(email?.dead).toBe(0);
  expect(email?.oldestPendingAgeMs).toBeGreaterThanOrEqual(0);
  expect(stats.get("analytics")?.pending).toBe(2);
});

it("topicStats reports zeros for an idle subscription", async () => {
  const queue = newQueue();
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  await queue.declareSubscriptions();

  const [stat] = await queue.topicStats("orders");
  expect(stat?.pending).toBe(0);
  expect(stat?.running).toBe(0);
  expect(stat?.dead).toBe(0);
  expect(stat?.oldestPendingAgeMs).toBeUndefined();
});

it("topicStats counts a failed delivery as dead", async () => {
  const queue = newQueue();
  queue.subscriber(
    "orders",
    "flaky",
    () => {
      throw new Error("boom");
    },
    { subscriptionName: "flaky", maxRetries: 0 },
  );
  await queue.declareSubscriptions();
  await queue.publish("orders", [1]);

  worker = queue.runWorker();
  const deadCounted = await waitFor(async () => {
    const [stat] = await queue.topicStats("orders");
    return stat?.dead === 1 && stat.pending === 0;
  });
  expect(deadCounted).toBe(true);
});

it("topicStats filters by topic and ignores non-pubsub jobs", async () => {
  const queue = newQueue();
  queue.task("plain", () => undefined);
  queue.subscriber("orders", "send_email", () => undefined, { subscriptionName: "email" });
  await queue.declareSubscriptions();
  queue.enqueue("plain");

  expect((await queue.topicStats()).map((s) => s.subscription)).toEqual(["email"]);
  expect(await queue.topicStats("other-topic")).toEqual([]);
});
