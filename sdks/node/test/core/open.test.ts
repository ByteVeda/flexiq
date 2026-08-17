import { existsSync, mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { Queue } from "../../src/index";

describe("Queue construction", () => {
  it("defaults SQLite to .flexiq/flexiq.db and creates the directory", () => {
    const cwd = process.cwd();
    const tmp = mkdtempSync(join(tmpdir(), "taskito-default-"));
    try {
      process.chdir(tmp);
      new Queue();
      expect(existsSync(join(tmp, ".taskito", "flexiq.db"))).toBe(true);
    } finally {
      process.chdir(cwd);
    }
  });

  it("creates missing parent directories for a nested dbPath", () => {
    const dbPath = join(mkdtempSync(join(tmpdir(), "taskito-nested-")), "deep", "sub", "queue.db");
    new Queue({ dbPath });
    expect(existsSync(dbPath)).toBe(true);
  });

  it("requires a dsn for non-sqlite backends", () => {
    expect(() => new Queue({ backend: "postgres" })).toThrow(/dsn/);
    expect(() => new Queue({ backend: "redis" })).toThrow(/dsn/);
  });
});
