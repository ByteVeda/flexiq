/**
 * The attached executor dialling a `tls://` scheduler.
 *
 * `FakeScheduler` terminates TLS with `node:tls`, so what passes here is the
 * executor's TLS against an independent implementation rather than against
 * itself. The certificates are the repository's shared test fixtures.
 */

import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, expect, it } from "vitest";
import { type Executor, Queue } from "../../src/index";
import { FakeScheduler } from "./fakeScheduler";

const FIXTURES = resolve(__dirname, "../../../../crates/flexiq-core/tests/fixtures/tls");
const fixture = (name: string): string => join(FIXTURES, name);

let executor: Executor | undefined;
let scheduler: FakeScheduler | undefined;

afterEach(async () => {
  await executor?.stop();
  executor = undefined;
  scheduler?.close();
  scheduler = undefined;
});

function serverTls(requireClientCert: boolean) {
  return {
    cert: readFileSync(fixture("server.pem")),
    key: readFileSync(fixture("server-key.pem")),
    ...(requireClientCert
      ? { ca: readFileSync(fixture("ca.pem")), requestCert: true, rejectUnauthorized: true }
      : {}),
  };
}

function echoQueue(): Queue {
  const queue = new Queue({
    dbPath: join(mkdtempSync(join(tmpdir(), "flexiq-exec-tls-")), "q.db"),
  });
  queue.task("echo", (value: string) => `echo:${value}`);
  return queue;
}

function withEnv(values: Record<string, string>): () => void {
  const previous = Object.fromEntries(Object.keys(values).map((name) => [name, process.env[name]]));
  Object.assign(process.env, values);
  return () => {
    for (const [name, value] of Object.entries(previous)) {
      if (value === undefined) {
        delete process.env[name];
      } else {
        process.env[name] = value;
      }
    }
  };
}

it("attaches to a tls:// scheduler", async () => {
  scheduler = await FakeScheduler.listen({ tls: serverTls(false) });
  executor = await echoQueue().runExecutor({
    // `localhost`, so the name verified is the certificate's.
    attach: `tls://localhost:${scheduler.port}`,
    tls: { ca: fixture("ca.pem") },
  });
  const hello = await scheduler.attached();
  expect(hello.sdk).toBe("node");
  expect(executor.peer).toMatch(/^tls:/);
});

it("reads mTLS material from the environment", async () => {
  scheduler = await FakeScheduler.listen({ tls: serverTls(true) });
  const restore = withEnv({
    FLEXIQ_ATTACH_TLS_CA: fixture("ca.pem"),
    FLEXIQ_ATTACH_TLS_CERT: fixture("client.pem"),
    FLEXIQ_ATTACH_TLS_KEY: fixture("client-key.pem"),
  });
  try {
    executor = await echoQueue().runExecutor({ attach: `tls://localhost:${scheduler.port}` });
    await scheduler.attached();
  } finally {
    restore();
  }
});

it("does not attach to an mTLS scheduler without a certificate", async () => {
  scheduler = await FakeScheduler.listen({ tls: serverTls(true) });
  await expect(
    echoQueue().runExecutor({
      attach: `tls://localhost:${scheduler.port}`,
      tls: { ca: fixture("ca.pem") },
      connectTimeoutMs: 5_000,
    }),
  ).rejects.toThrow();
});

it("refuses TLS options beside a plaintext address", async () => {
  await expect(
    echoQueue().runExecutor({ attach: "127.0.0.1:1", tls: { ca: fixture("ca.pem") } }),
  ).rejects.toThrow(/tls:\/\//);
});

it("treats blank TLS variables as unset", async () => {
  // Compose sets variables to "" freely; that must not read as TLS material.
  scheduler = await FakeScheduler.listen();
  const restore = withEnv({
    FLEXIQ_ATTACH_TLS_CA: "",
    FLEXIQ_ATTACH_TLS_CERT: "",
    FLEXIQ_ATTACH_TLS_KEY: "",
  });
  try {
    executor = await echoQueue().runExecutor({ attach: `127.0.0.1:${scheduler.port}` });
    await scheduler.attached();
  } finally {
    restore();
  }
});
