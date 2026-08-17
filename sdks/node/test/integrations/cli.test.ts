import { type ChildProcess, execSync, spawn } from "node:child_process";
import { existsSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { beforeAll, describe, expect, it } from "vitest";

const pkgRoot = fileURLToPath(new URL("../..", import.meta.url));
const cliPath = fileURLToPath(new URL("../../dist/cli.js", import.meta.url));
const indexUrl = pathToFileURL(fileURLToPath(new URL("../../dist/index.js", import.meta.url))).href;

beforeAll(() => {
  if (!existsSync(cliPath)) {
    execSync("pnpm run build:ts", { cwd: pkgRoot, stdio: "ignore" });
  }
}, 60_000);

function runCli(args: string[]): Promise<{ stdout: string; code: number }> {
  return new Promise((resolve) => {
    const child = spawn(process.execPath, [cliPath, ...args], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.on("close", (code) => resolve({ stdout, code: code ?? 0 }));
  });
}

function tempDb(): string {
  return join(mkdtempSync(join(tmpdir(), "taskito-cli-")), "cli.db");
}

describe("taskito CLI", () => {
  it("enqueues and reports stats and jobs", async () => {
    const db = tempDb();
    await runCli(["--db", db, "enqueue", "add", "[2,3]"]);

    const stats = JSON.parse((await runCli(["--db", db, "--json", "stats"])).stdout);
    expect(stats.pending).toBe(1);

    const jobs = JSON.parse((await runCli(["--db", db, "--json", "jobs"])).stdout);
    expect(jobs[0].taskName).toBe("add");
  });

  it("pauses and lists paused queues", async () => {
    const db = tempDb();
    await runCli(["--db", db, "pause", "emails"]);
    const paused = JSON.parse((await runCli(["--db", db, "--json", "paused"])).stdout);
    expect(paused.paused).toContain("emails");
  });

  it("runs a worker from an app module", async () => {
    const dir = mkdtempSync(join(tmpdir(), "taskito-cli-"));
    const db = join(dir, "cli.db");
    const marker = join(dir, "done.txt");
    const app = join(dir, "app.mjs");
    // Static module body — paths are passed via env, never interpolated into
    // source, so there is no dynamic-code-construction sink.
    writeFileSync(
      app,
      [
        `const { Queue } = await import(process.env.FLEXIQ_INDEX);`,
        `const { writeFileSync } = await import("node:fs");`,
        `const queue = new Queue({ dbPath: process.env.FLEXIQ_DB });`,
        `queue.task("mark", () => { writeFileSync(process.env.FLEXIQ_MARKER, "ok"); return "ok"; });`,
        `queue.enqueue("mark");`,
        `export default queue;`,
      ].join("\n"),
    );

    let child: ChildProcess | undefined;
    try {
      child = spawn(process.execPath, [cliPath, "run", app], {
        stdio: "ignore",
        env: { ...process.env, FLEXIQ_INDEX: indexUrl, FLEXIQ_DB: db, FLEXIQ_MARKER: marker },
      });
      expect(await waitForFile(marker, 25_000)).toBe(true);
    } finally {
      child?.kill("SIGTERM");
    }
    // Spawning a second Node, importing the SDK and opening SQLite before the
    // job can run costs more than the file's default budget on a cold Windows
    // runner, where this timed out at just over six seconds.
  }, 30_000);
});

async function waitForFile(path: string, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (existsSync(path)) {
      return true;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  return false;
}
