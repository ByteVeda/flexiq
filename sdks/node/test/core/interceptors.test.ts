import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { GzipCodec, Interception, InterceptionError, Queue, type Worker } from "../../src/index";

let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
});

function tempDb(): string {
  return join(mkdtempSync(join(tmpdir(), "flexiq-intercept-")), "queue.db");
}

async function waitFor<T>(read: () => T | undefined, timeoutMs = 10_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = read();
    if (value !== undefined) {
      return value;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("timed out waiting for job result");
}

describe("enqueue interceptors", () => {
  it("convert rewrites the args the worker sees", async () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.echo", (x: number) => x)
      .intercept((_taskName, args) => Interception.convert([(args[0] as number) * 10]));

    const id = queue.enqueue("ic.echo", [5]);
    worker = queue.runWorker();
    await expect(waitFor(() => queue.getResult(id))).resolves.toBe(50);
  });

  it("redirect enqueues the other task; the original handler never runs", async () => {
    const ranA: unknown[] = [];
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.a", (x: number) => {
        ranA.push(x);
        return x;
      })
      .task("ic.b", (x: number) => x + 100)
      .intercept((taskName, args) =>
        taskName === "ic.a" ? Interception.redirect("ic.b", args) : Interception.pass(),
      );

    const id = queue.enqueue("ic.a", [1]);
    expect(queue.getJob(id)?.taskName).toBe("ic.b");
    worker = queue.runWorker();
    await expect(waitFor(() => queue.getResult(id))).resolves.toBe(101);
    expect(ranA).toEqual([]);
  });

  it("reject throws InterceptionError and enqueues nothing", async () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.noop", () => null)
      .intercept(() => Interception.reject("blocked by policy"));

    expect(() => queue.enqueue("ic.noop", [])).toThrow(InterceptionError);
    expect(() => queue.enqueue("ic.noop", [])).toThrow(/blocked by policy/);
    expect((await queue.stats()).pending).toBe(0);
  });

  it("a null-ish interceptor result throws InterceptionError", () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.noop", () => null)
      // biome-ignore lint/suspicious/noExplicitAny: deliberately misbehaving interceptor
      .intercept((() => undefined) as any);

    expect(() => queue.enqueue("ic.noop", [])).toThrow(/returned null/);
  });

  it("interceptors chain in registration order", async () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.echo", (x: number) => x)
      .intercept((_name, args) => Interception.convert([(args[0] as number) + 1]))
      .intercept((_name, args) => Interception.convert([(args[0] as number) * 2]));

    const id = queue.enqueue("ic.echo", [3]); // (3 + 1) * 2
    worker = queue.runWorker();
    await expect(waitFor(() => queue.getResult(id))).resolves.toBe(8);
  });

  it("batch enqueue converts per item", async () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.echo", (x: number) => x)
      .intercept((_name, args) => Interception.convert([(args[0] as number) * 10]));

    const ids = queue.enqueueMany("ic.echo", [{ args: [1] }, { args: [2] }]);
    worker = queue.runWorker();
    await expect(waitFor(() => queue.getResult(ids[0] as string))).resolves.toBe(10);
    await expect(waitFor(() => queue.getResult(ids[1] as string))).resolves.toBe(20);
  });

  it("batch reject fails the whole batch — zero jobs stored", async () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.echo", (x: number) => x)
      .intercept((_name, args) =>
        (args[0] as number) < 0 ? Interception.reject("negative") : Interception.pass(),
      );

    expect(() =>
      queue.enqueueMany("ic.echo", [{ args: [1] }, { args: [-2] }, { args: [3] }]),
    ).toThrow(InterceptionError);
    expect((await queue.stats()).pending).toBe(0);
  });

  it("batch redirect is unsupported", () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.a", (x: number) => x)
      .task("ic.b", (x: number) => x)
      .intercept((_name, args) => Interception.redirect("ic.b", args));

    expect(() => queue.enqueueMany("ic.a", [{ args: [1] }])).toThrow(
      /Redirect is not supported for batch/,
    );
  });

  it("redirect of a task with per-task codecs is rejected", () => {
    const queue = new Queue({ dbPath: tempDb(), codecs: { gzip: new GzipCodec() } })
      .task("ic.codec", (x: number) => x, { codecs: ["gzip"] })
      .task("ic.b", (x: number) => x)
      .intercept((_name, args) => Interception.redirect("ic.b", args));

    expect(() => queue.enqueue("ic.codec", [1])).toThrow(
      /Redirect is not supported for a task with payload codecs/,
    );
  });

  it("interceptors run before gates: a gate sees the converted args", async () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.echo", (x: number) => x)
      .gate("ic.echo", ({ args }) => (args[0] as number) > 0)
      .intercept((_name, args) => Interception.convert([Math.abs(args[0] as number)]));

    // Raw -5 would be gated; the interceptor converts to 5 first.
    const id = queue.enqueue("ic.echo", [-5]);
    worker = queue.runWorker();
    await expect(waitFor(() => queue.getResult(id))).resolves.toBe(5);
  });
});

describe("analyzeArguments", () => {
  it("reports the args unchanged when nothing is registered", () => {
    const queue = new Queue({ dbPath: tempDb() }).task("ic.echo", (x: number) => x);

    expect(queue.analyzeArguments("ic.echo", [1])).toEqual({
      taskName: "ic.echo",
      args: [1],
      outcomes: [],
      rejected: false,
    });
  });

  it("reports the converted args and the chain that produced them", () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.echo", (x: number) => x)
      .intercept((_name, args) => Interception.convert([(args[0] as number) * 10]))
      .intercept(() => Interception.pass());

    const analysis = queue.analyzeArguments("ic.echo", [5]);
    expect(analysis.args).toEqual([50]);
    expect(analysis.rejected).toBe(false);
    expect(analysis.outcomes.map((outcome) => outcome.type)).toEqual(["convert", "pass"]);
  });

  it("reports the redirect target", () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.a", (x: number) => x)
      .task("ic.b", (x: number) => x)
      .intercept((_name, args) => Interception.redirect("ic.b", args));

    expect(queue.analyzeArguments("ic.a", [1]).taskName).toBe("ic.b");
  });

  it("reports a rejection instead of throwing", () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.echo", (x: number) => x)
      .intercept(() => Interception.reject("no negatives"));

    const analysis = queue.analyzeArguments("ic.echo", [-1]);
    expect(analysis.rejected).toBe(true);
    expect(analysis.rejectionReason).toMatch(/no negatives/);
    expect(analysis.outcomes.map((outcome) => outcome.type)).toEqual(["reject"]);
  });

  it("enqueues nothing and leaves the interception counters alone", async () => {
    const queue = new Queue({ dbPath: tempDb() })
      .task("ic.echo", (x: number) => x)
      .intercept((_name, args) => Interception.convert(args));

    queue.analyzeArguments("ic.echo", [1]);
    expect((await queue.stats()).pending).toBe(0);
    expect(queue.interceptionStats().total_intercepts).toBe(0);
  });
});
