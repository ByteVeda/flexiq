// Override keys carry the namespace: the settings KV spans the whole database,
// so two tenants with a same-named task must not share one override. The
// vectors are the cross-SDK contract's; every shell pins the same ones.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { beforeEach, describe, expect, it } from "vitest";
import { OverridesStore, overrideKey, overridePrefix } from "../../src/dashboard/stores/overrides";
import { Queue } from "../../src/index";

describe("override keys", () => {
  it("match the cross-SDK vectors", () => {
    expect(overrideKey("task", undefined, "send")).toBe("overrides:task:send");
    expect(overrideKey("queue", undefined, "emails")).toBe("overrides:queue:emails");
    expect(overrideKey("task", "billing", "send")).toBe("overrides:ns:7:billing:task:send");
    expect(overrideKey("queue", "a:b", "emails")).toBe("overrides:ns:3:a:b:queue:emails");
  });

  it("tell the empty namespace apart from the default one", () => {
    expect(overrideKey("task", "", "send")).toBe("overrides:ns:0::task:send");
  });

  it("measure the namespace in UTF-8 bytes, not UTF-16 units", () => {
    expect(overridePrefix("task", "é")).toBe("overrides:ns:2:é:task:");
  });

  it("never put a namespaced key under the default prefix", () => {
    for (const scope of ["task", "queue"] as const) {
      const fallback = overridePrefix(scope);
      for (const ns of ["task", "queue", "", "x:task:"]) {
        expect(overrideKey(scope, ns, "n").startsWith(fallback)).toBe(false);
      }
    }
  });
});

describe("namespaced override store", () => {
  let db: string;

  beforeEach(() => {
    db = join(mkdtempSync(join(tmpdir(), "flexiq-ovrns-")), "q.db");
  });

  it("writes the namespaced key, invisible to the default namespace", () => {
    const billing = new Queue({ dbPath: db, namespace: "billing" });
    const fallback = new Queue({ dbPath: db });

    billing.setTaskOverride("send", { max_retries: 7 });
    billing.setQueueOverride("emails", { max_concurrent: 2 });

    expect(billing.getSetting("overrides:ns:7:billing:task:send")).not.toBeNull();
    expect(billing.getSetting("overrides:ns:7:billing:queue:emails")).not.toBeNull();
    expect(fallback.getSetting("overrides:task:send")).toBeNull();

    expect(fallback.getTaskOverride("send")).toBeUndefined();
    expect(fallback.listTaskOverrides().size).toBe(0);
    expect(fallback.listQueueOverrides().size).toBe(0);
    expect(billing.listTaskOverrides().get("send")?.max_retries).toBe(7);
    expect(billing.listQueueOverrides().get("emails")?.max_concurrent).toBe(2);
  });

  it("keeps each namespace's override of a same-named task apart", () => {
    const queue = new Queue({ dbPath: db });
    const billing = new OverridesStore(queue, "billing");
    const shipping = new OverridesStore(queue, "shipping");
    const fallback = new OverridesStore(queue);

    billing.setTask("send", { max_retries: 1 });
    shipping.setTask("send", { max_retries: 2 });
    fallback.setTask("send", { max_retries: 3 });

    expect(billing.getTask("send")?.max_retries).toBe(1);
    expect(shipping.getTask("send")?.max_retries).toBe(2);
    expect(fallback.getTask("send")?.max_retries).toBe(3);
    expect([...fallback.listTasks().keys()]).toEqual(["send"]);

    expect(billing.clearTask("send")).toBe(true);
    expect(fallback.getTask("send")?.max_retries).toBe(3);
    expect(shipping.getTask("send")?.max_retries).toBe(2);
  });
});
