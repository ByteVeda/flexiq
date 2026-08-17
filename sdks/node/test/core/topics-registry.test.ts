// Topics registry: declaring a log topic retains its publishes even with zero
// subscribers, removing the late-join boundary the ad-hoc log path imposes.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-topics-")), "q.db") });
}

it("retains publishes to a declared log topic with no subscriber", async () => {
  const queue = newQueue();
  await queue.declareTopic("events");

  // Publish before any subscriber exists — no fan-out jobs, but the messages
  // are retained because the topic is declared.
  expect(await queue.publish("events", [1])).toEqual([]);
  expect(await queue.publish("events", [2])).toEqual([]);

  // A consumer that subscribes later still sees the earlier messages.
  await queue.subscribeLog("events", "late");
  const msgs = await queue.readTopic("events", "late");
  expect(msgs.map((m) => m.args)).toEqual([[1], [2]]);
});

it("lists declared topics and round-trips retention", async () => {
  const queue = newQueue();
  await queue.declareTopic("plain");
  await queue.declareTopic("bounded", { retention: 1.5 });

  const byName = new Map((await queue.listDeclaredTopics()).map((t) => [t.name, t]));

  expect(byName.get("plain")?.mode).toBe("log");
  expect(byName.get("plain")?.retentionMs).toBeUndefined();
  expect(byName.get("bounded")?.mode).toBe("log");
  expect(byName.get("bounded")?.retentionMs).toBe(1500);
});
