// Per-message ack: lease / ack / nack / redelivery over a log topic. Unlike the
// S28 cursor read, each message is leased and tracked individually, so a nack or
// an expired lease redelivers just that message without blocking its siblings.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { Queue } from "../../src/index";

function newQueue(): Queue {
  return new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-per-msg-ack-")), "q.db") });
}

/** Narrow an array element to non-undefined (strict indexed access). */
function must<T>(value: T | undefined): T {
  if (value === undefined) throw new Error("expected a value");
  return value;
}

it("leases available messages and skips in-flight ones", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "c");
  for (let i = 0; i < 3; i++) await queue.publish("events", [i]);

  // First lease takes the two oldest; they are now in-flight for 30s.
  const first = await queue.leaseTopic("events", "c", { limit: 2 });
  expect(first.map((m) => m.args[0])).toEqual([0, 1]);

  // Second lease sees only the still-available third message.
  const second = await queue.leaseTopic("events", "c");
  expect(second.map((m) => m.args[0])).toEqual([2]);
});

it("redelivers a nacked message but not an acked one", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "c");
  await queue.publish("events", ["a"]);
  await queue.publish("events", ["b"]);

  const leased = await queue.leaseTopic("events", "c");
  expect(leased.map((m) => m.args[0])).toEqual(["a", "b"]);

  // Ack the first (done forever), nack the second (redeliver now).
  expect(await queue.ackMessage("events", "c", must(leased[0]).id)).toBe(true);
  expect(await queue.nackMessage("events", "c", must(leased[1]).id)).toBe(true);

  const redelivered = await queue.leaseTopic("events", "c");
  expect(redelivered.map((m) => m.args[0])).toEqual(["b"]);
});

it("redelivers after a short visibility lease expires", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "c");
  await queue.publish("events", ["x"]);

  // Lease with a 100ms visibility; it is in-flight immediately after.
  expect(
    (await queue.leaseTopic("events", "c", { visibility: 0.1 })).map((m) => m.args[0]),
  ).toEqual(["x"]);
  expect(await queue.leaseTopic("events", "c", { visibility: 0.1 })).toEqual([]);

  // Once the lease expires the un-acked message becomes available again.
  await new Promise((r) => setTimeout(r, 150));
  expect(
    (await queue.leaseTopic("events", "c", { visibility: 0.1 })).map((m) => m.args[0]),
  ).toEqual(["x"]);
});

it("returns false when there is nothing to ack or nack", async () => {
  const queue = newQueue();
  await queue.subscribeLog("events", "c");
  await queue.publish("events", ["x"]);

  // No lease has been taken, so the message id is unknown to ack/nack.
  const msg = must((await queue.readTopic("events", "c"))[0]);
  expect(await queue.ackMessage("events", "c", msg.id)).toBe(false);
  expect(await queue.nackMessage("events", "c", msg.id)).toBe(false);
});
