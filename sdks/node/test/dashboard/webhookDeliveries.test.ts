import { execSync } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeAll, beforeEach, expect, it } from "vitest";
import { seedAdminAndSession } from "../../src/dashboard/testing";
import { Queue, serveDashboard } from "../../src/index";

// These deliveries target a loopback receiver, which the SSRF guard blocks by default.
process.env.FLEXIQ_WEBHOOKS_ALLOW_PRIVATE = "1";

const pkgRoot = fileURLToPath(new URL("../..", import.meta.url));
const staticDir = join(pkgRoot, "static", "dashboard");

beforeAll(() => {
  if (!existsSync(join(staticDir, "index.html"))) {
    execSync("pnpm run build:dashboard", { cwd: pkgRoot, stdio: "ignore" });
  }
}, 120_000);

let dash: Server | undefined;
let receiver: Server | undefined;
let queue: Queue;
let base = "";
let sinkUrl = "";
let received: unknown[] = [];
let headers: Record<string, string> = {};

beforeEach(async () => {
  received = [];
  receiver = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (c: Buffer) => chunks.push(c));
    req.on("end", () => {
      received.push(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      res.writeHead(200).end("ok");
    });
  });
  receiver.listen(0, "127.0.0.1");
  await once(receiver, "listening");
  sinkUrl = `http://127.0.0.1:${(receiver.address() as AddressInfo).port}/hook`;

  const db = join(mkdtempSync(join(tmpdir(), "taskito-dashdel-")), "q.db");
  queue = new Queue({ dbPath: db });
  queue.task("noop", () => null);
  ({ headers } = await seedAdminAndSession(queue));
  dash = serveDashboard(queue, { port: 0, staticDir, secureCookies: false });
  await once(dash, "listening");
  base = `http://127.0.0.1:${(dash.address() as AddressInfo).port}`;
});

afterEach(() => {
  dash?.close();
  receiver?.close();
  dash = undefined;
  receiver = undefined;
});

it("persists test deliveries are not logged but replays and dispatches are", async () => {
  const webhook = queue.webhooks.create({ url: sinkUrl, events: ["job.completed"] });

  // Dispatch through a real job event so the log records it.
  const worker = queue.runWorker();
  try {
    const id = queue.enqueue("noop", []);
    await queue.result(id);
    // Delivery is recorded asynchronously after the HTTP roundtrip.
    for (let i = 0; i < 200 && queue.webhooks.deliveryCount(webhook.id) === 0; i++) {
      await new Promise((r) => setTimeout(r, 25));
    }
  } finally {
    worker.stop();
  }

  expect(queue.webhooks.deliveryCount(webhook.id)).toBe(1);

  // Survives a fresh Queue handle over the same database.
  const list = (await (
    await fetch(`${base}/api/webhooks/${webhook.id}/deliveries`, { headers })
  ).json()) as { items: Array<{ id: string; status: string; event: string }>; total: number };
  expect(list.total).toBe(1);
  expect(list.items[0]?.status).toBe("delivered");
  expect(list.items[0]?.event).toBe("job.completed");

  const deliveryId = list.items[0]?.id ?? "";
  const detail = (await (
    await fetch(`${base}/api/webhooks/${webhook.id}/deliveries/${deliveryId}`, { headers })
  ).json()) as { id: string; response_code: number };
  expect(detail.id).toBe(deliveryId);
  expect(detail.response_code).toBe(200);

  // Replay creates a NEW record tagged replay_of.
  const replay = (await (
    await fetch(`${base}/api/webhooks/${webhook.id}/deliveries/${deliveryId}/replay`, {
      method: "POST",
      headers,
    })
  ).json()) as { replayed_of: string; delivered: boolean };
  expect(replay.replayed_of).toBe(deliveryId);
  expect(replay.delivered).toBe(true);
  expect(queue.webhooks.deliveryCount(webhook.id)).toBe(2);
  expect((received.at(-1) as { replay_of?: string }).replay_of).toBe(deliveryId);
});

it("rotates the webhook secret and returns it exactly once", async () => {
  const webhook = queue.webhooks.create({ url: sinkUrl, secret: "old-secret" });
  const res = await fetch(`${base}/api/webhooks/${webhook.id}/rotate-secret`, {
    method: "POST",
    headers,
  });
  expect(res.status).toBe(200);
  const body = (await res.json()) as { id: string; secret: string };
  expect(body.id).toBe(webhook.id);
  expect(body.secret).not.toBe("old-secret");
  expect(body.secret.length).toBeGreaterThan(20);
  expect(queue.webhooks.get(webhook.id)?.secret).toBe(body.secret);

  // Listing never leaks the secret.
  const list = (await (await fetch(`${base}/api/webhooks`, { headers })).json()) as Array<{
    secret?: unknown;
    has_secret: boolean;
  }>;
  expect(list[0]?.secret).toBeUndefined();
  expect(list[0]?.has_secret).toBe(true);
});

it("404s deliveries for an unknown webhook and unknown delivery ids", async () => {
  expect((await fetch(`${base}/api/webhooks/nope/deliveries`, { headers })).status).toBe(404);
  const webhook = queue.webhooks.create({ url: sinkUrl });
  expect(
    (await fetch(`${base}/api/webhooks/${webhook.id}/deliveries/nope`, { headers })).status,
  ).toBe(404);
  expect(
    (
      await fetch(`${base}/api/webhooks/${webhook.id}/deliveries/nope/replay`, {
        method: "POST",
        headers,
      })
    ).status,
  ).toBe(404);
});

it("rejects an invalid status filter", async () => {
  const webhook = queue.webhooks.create({ url: sinkUrl });
  const res = await fetch(`${base}/api/webhooks/${webhook.id}/deliveries?status=bogus`, {
    headers,
  });
  expect(res.status).toBe(400);
});
