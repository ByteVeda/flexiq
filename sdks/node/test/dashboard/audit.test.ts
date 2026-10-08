// Dashboard changes land in the audit trail. The row shape is a cross-SDK
// contract, so the test reads `audit_log` straight from the SQLite file
// rather than through any SDK view of it.
import { once } from "node:events";
import { mkdtempSync } from "node:fs";
import type { Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { createDashboardServer } from "../../src/dashboard";
import { seedAdminAndSession } from "../../src/dashboard/testing";
import { Queue } from "../../src/index";

// `node:sqlite` arrived in Node 22; older runtimes skip the trail checks.
const sqlite = await import("node:sqlite").catch(() => undefined);

interface Row {
  namespace: string;
  principal_kind: string;
  token_id: string;
  principal: string;
  operation: string;
  target_kind: string | null;
  target: string | null;
  outcome: string;
  access: string;
}

let server: Server | undefined;

afterEach(() => {
  server?.close();
  server = undefined;
});

async function start(
  authEnabled: boolean,
  auth?: { token: string },
): Promise<{ queue: Queue; db: string; base: string }> {
  const db = join(mkdtempSync(join(tmpdir(), "flexiq-dashaudit-")), "q.db");
  const queue = new Queue({ dbPath: db });
  server = createDashboardServer(queue, tmpdir(), auth ?? { authEnabled, secureCookies: false });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { queue, db, base: `http://127.0.0.1:${(server.address() as AddressInfo).port}` };
}

/** Stop the server, flush the recorder, and read the stored trail. */
async function trail(queue: Queue, db: string): Promise<Row[]> {
  server?.close();
  server = undefined;
  await queue.closeDashboardAudit();
  if (!sqlite) {
    throw new Error("node:sqlite is unavailable");
  }
  const conn = new sqlite.DatabaseSync(db);
  try {
    return conn
      .prepare(
        "SELECT namespace, principal_kind, token_id, principal, operation," +
          " target_kind, target, outcome, access FROM audit_log ORDER BY at_ms, id",
      )
      .all() as unknown as Row[];
  } finally {
    conn.close();
  }
}

describe.skipIf(sqlite === undefined)("dashboard audit trail", () => {
  it("records a signed-in change and a refused one, never one without a session", async () => {
    const { queue, db, base } = await start(true);
    const admin = await seedAdminAndSession(queue, { username: "alice" });
    const viewer = await seedAdminAndSession(queue, { username: "vera", role: "viewer" });

    const paused = await fetch(`${base}/api/queues/emails/pause`, {
      method: "POST",
      headers: admin.headers,
    });
    expect(paused.status).toBe(200);
    await fetch(`${base}/api/queues/paused`, { headers: admin.headers });
    const refused = await fetch(`${base}/api/queues/emails/resume`, {
      method: "POST",
      headers: viewer.headers,
    });
    expect(refused.status).toBe(403);
    const anonymous = await fetch(`${base}/api/queues/emails/resume`, { method: "POST" });
    expect(anonymous.status).toBe(401);

    expect(await trail(queue, db)).toEqual([
      {
        namespace: "default",
        principal_kind: "user",
        token_id: "alice",
        principal: "alice",
        operation: "dashboard POST /api/queues/{queue}/pause",
        target_kind: "queue",
        target: "emails",
        outcome: "OK",
        access: "write",
      },
      {
        namespace: "default",
        principal_kind: "user",
        token_id: "vera",
        principal: "vera",
        operation: "dashboard POST /api/queues/{queue}/resume",
        target_kind: "queue",
        target: "emails",
        outcome: "PERMISSION_DENIED",
        access: "write",
      },
    ]);
  });

  it("records an open dashboard's change as anonymous", async () => {
    const { queue, db, base } = await start(false);
    const purged = await fetch(`${base}/api/dead-letters/purge`, { method: "POST" });
    expect(purged.status).toBe(200);
    // The auth routes are off with auth off; nothing changed.
    expect((await fetch(`${base}/api/auth/logout`, { method: "POST" })).status).toBe(404);

    const rows = await trail(queue, db);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      principal_kind: "anonymous",
      token_id: "",
      operation: "dashboard POST /api/dead-letters/purge",
      target_kind: null,
    });
  });

  it("refuses a retention window under a day", () => {
    const queue = new Queue({ dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-")), "q.db") });
    expect(() => createDashboardServer(queue, tmpdir(), { auditRetentionDays: 0 })).toThrow(
      /auditRetentionDays/,
    );
  });
});
