// Concurrent edits to the settings-backed feature stores must not be lost.
//
// Every store keeps a whole JSON document under one settings key. A
// read-then-write drops a concurrent edit wholesale, and more than one
// dashboard replica against one backend is a supported deployment — so each
// store writes conditionally on the value it read and retries when it loses.
//
// The races here are deterministic: `racing()` runs a supplied writer
// immediately after a read, which is exactly the window a read-then-write loses.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeEach, describe, expect, it } from "vitest";
import { AuthStore } from "../../src/dashboard/auth/store";
import { MiddlewareDisableStore } from "../../src/dashboard/stores/middlewareDisables";
import { OverridesStore } from "../../src/dashboard/stores/overrides";
import { Queue } from "../../src/index";
import { MAX_ATTEMPTS, SettingConflictError, updateSetting } from "../../src/settingsKv";
import { DeliveryLog } from "../../src/webhooks/deliveryLog";
import { WebhookStore } from "../../src/webhooks/store";
import type { Delivery, Webhook } from "../../src/webhooks/types";

let queue: Queue;
let reads = 0;

beforeEach(() => {
  queue = new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "taskito-cas-")), "q.db") });
  reads = 0;
});

/** A queue proxy that lets another writer in right after each settings read. */
function racing<T extends object>(target: T, ...interlopers: (() => unknown)[]): T {
  return new Proxy(target, {
    get(source, property, receiver) {
      if (property !== "getSetting") {
        return Reflect.get(source, property, receiver);
      }
      return (key: string) => {
        const value = (source as { getSetting(k: string): string | null }).getSetting(key);
        reads += 1;
        interlopers.shift()?.();
        return value;
      };
    },
  }) as T;
}

function webhook(id: string, url: string): Webhook {
  return {
    id,
    url,
    events: [],
    headers: {},
    maxRetries: 3,
    timeoutMs: 10_000,
    retryBackoff: 2,
    enabled: true,
    createdAt: 1,
    updatedAt: 1,
  };
}

function delivery(id: string): Delivery {
  return {
    id,
    webhookId: "hook",
    event: "job.completed",
    status: "delivered",
    ok: true,
    attempts: 1,
    payload: {},
    taskName: null,
    jobId: null,
    responseCode: 200,
    responseBody: null,
    latencyMs: 1,
    createdAt: 1,
    completedAt: 1,
  };
}

describe("the storage primitive", () => {
  it("refuses a stale expectation", () => {
    queue.setSetting("k", "v1");

    expect(queue.setSettingIf("k", "stale", "v2")).toBe(false);
    expect(queue.getSetting("k")).toBe("v1");

    expect(queue.setSettingIf("k", "v1", "v2")).toBe(true);
    expect(queue.getSetting("k")).toBe("v2");
  });

  it("inserts exactly once when the key is expected unset", () => {
    expect(queue.setSettingIf("k", null, "first")).toBe(true);
    expect(queue.setSettingIf("k", null, "second")).toBe(false);
    expect(queue.getSetting("k")).toBe("first");
  });

  it("does not insert when a value is expected but the key is missing", () => {
    expect(queue.setSettingIf("missing", "anything", "v")).toBe(false);
    expect(queue.getSetting("missing")).toBeNull();
  });
});

describe("the retry helper", () => {
  const loadList = (raw: string | null): unknown[] => (raw ? JSON.parse(raw) : []);

  it("writes nothing when a mutation on a missing key changes nothing", () => {
    // The skip compares the new encoding against the *document as loaded*, not
    // the raw stored string: on a missing key the raw is null while the
    // encoding is `[]`, so comparing to the raw would write a row for it.
    const changed = updateSetting(queue, "missing", loadList, (names) => {
      const before = names.length;
      names.splice(0, names.length, ...names.filter((n) => n !== "absent"));
      return names.length !== before;
    });

    expect(changed).toBe(false);
    expect(queue.getSetting("missing")).toBeNull();
  });

  it("retries until it wins", () => {
    const target = racing(queue, () => queue.setSetting("k", '["interloper"]'));

    updateSetting(target, "k", loadList, (names) => names.push("mine"));

    expect(reads).toBe(2);
    expect(queue.getSetting("k")).toBe('["interloper","mine"]');
  });

  it("gives up after the attempt bound", () => {
    // A *different* value on every read, so no attempt can ever win.
    let tick = 0;
    const interlopers = Array.from({ length: MAX_ATTEMPTS + 5 }, () => () => {
      queue.setSetting("k", `[${tick++}]`);
    });
    const target = racing(queue, ...interlopers);

    expect(() => updateSetting(target, "k", loadList, (names) => names.push("mine"))).toThrow(
      SettingConflictError,
    );
    expect(reads).toBe(MAX_ATTEMPTS);
  });
});

describe("the stores", () => {
  it("keeps both users created concurrently", async () => {
    const quiet = new AuthStore(queue);
    const contender = new AuthStore(racing(queue, () => quiet.createUser("first", "password123")));

    await contender.createUser("second", "password123");

    expect(
      quiet
        .listUsers()
        .map((user) => user.username)
        .sort(),
    ).toEqual(["first", "second"]);
  });

  it("does not resurrect a user deleted mid-authenticate", async () => {
    const quiet = new AuthStore(queue);
    await quiet.createUser("alice", "password123");
    const contender = new AuthStore(racing(queue, () => quiet.deleteUser("alice")));

    // The read that fed the password check saw the row, so the login stands —
    // but stamping last_login_at must not write the whole document back and
    // bring the deleted account with it.
    expect(await contender.authenticate("alice", "password123")).toBeDefined();
    expect(quiet.getUser("alice")).toBeUndefined();
  });

  it("keeps both webhooks created concurrently", () => {
    const quiet = new WebhookStore(queue);
    const contender = new WebhookStore(
      racing(queue, () => quiet.put(webhook("first", "https://example.test/first"))),
    );

    contender.put(webhook("second", "https://example.test/second"));

    expect(
      quiet
        .list()
        .map((hook) => hook.id)
        .sort(),
    ).toEqual(["first", "second"]);
  });

  it("writes nothing when deleting an unknown webhook", () => {
    const store = new WebhookStore(queue);

    expect(store.delete("nope")).toBe(false);
    expect(queue.getSetting("webhooks:subscriptions")).toBeNull();
  });

  it("keeps both deliveries recorded concurrently", () => {
    const quiet = new DeliveryLog(queue);
    const contender = new DeliveryLog(racing(queue, () => quiet.record(delivery("first"))));

    contender.record(delivery("second"));

    expect(
      quiet
        .listFor("hook")
        .map((row) => row.id)
        .sort(),
    ).toEqual(["first", "second"]);
  });

  it("keeps both override edits", () => {
    const quiet = new OverridesStore(queue);
    const contender = new OverridesStore(
      racing(queue, () => quiet.setTask("send_email", { max_retries: 7 })),
    );

    contender.setTask("send_email", { timeout: 30 });

    const merged = quiet.getTask("send_email");
    expect(merged?.max_retries).toBe(7);
    expect(merged?.timeout).toBe(30);
  });

  it("leaves a row rather than a delete when a disable list empties", () => {
    // Deleting sat outside the compare-and-set, so a concurrent writer's entry
    // could be added between the swap and the delete and then removed by it.
    const store = new MiddlewareDisableStore(queue);
    store.setDisabled("send_email", "RetryLogger", true);

    expect(store.setDisabled("send_email", "RetryLogger", false)).toEqual([]);
    expect(queue.getSetting("middleware:disabled:send_email")).toBe("[]");
    expect(store.getFor("send_email")).toEqual([]);
    expect(store.listAll()).toEqual({});
  });

  it("keeps both middleware toggles", () => {
    const quiet = new MiddlewareDisableStore(queue);
    const contender = new MiddlewareDisableStore(
      racing(queue, () => quiet.setDisabled("send_email", "Tracing", true)),
    );

    expect(contender.setDisabled("send_email", "Metrics", true).sort()).toEqual([
      "Metrics",
      "Tracing",
    ]);
  });
});
