import { beforeEach, expect, it } from "vitest";
import { Emitter } from "../../src/events";
import { setLogLevel, setLogSink } from "../../src/index";

let lines: string[];

beforeEach(() => {
  lines = [];
  setLogSink((_level, line) => lines.push(line));
  setLogLevel("debug");
});

it("isolates a throwing listener from its siblings and logs it", () => {
  const emitter = new Emitter();
  const seen: string[] = [];
  emitter.on("job.completed", () => {
    throw new Error("listener boom");
  });
  emitter.on("job.completed", () => seen.push("second"));

  emitter.emit("job.completed", { jobId: "1", taskName: "add" });

  expect(seen).toEqual(["second"]); // the throw didn't stop the fan-out
  expect(lines).toHaveLength(1);
  expect(lines[0]).toContain('listener for "job.completed" threw');
  expect(lines[0]).toContain("listener boom");
});

it("logs an async listener's rejection instead of swallowing it", async () => {
  const emitter = new Emitter();
  emitter.on("job.failed", async () => {
    throw new Error("async boom");
  });

  emitter.emit("job.failed", { jobId: "1", taskName: "add", error: "nope" });
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(lines).toHaveLength(1);
  expect(lines[0]).toContain('listener for "job.failed" rejected');
  expect(lines[0]).toContain("async boom");
});

it("stays silent when listeners behave", async () => {
  const emitter = new Emitter();
  emitter.on("queue.paused", () => undefined);
  emitter.on("queue.paused", async () => undefined);

  emitter.emit("queue.paused", { queue: "default" });
  await new Promise((resolve) => setTimeout(resolve, 0));

  expect(lines).toEqual([]);
});
