import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, expect, it } from "vitest";
import {
  DuplicateTaskError,
  Queue,
  TaskDiscoveryError,
  TaskNotBoundError,
  type Worker,
} from "../../src/index";
import { clearPendingTasks } from "../../src/registry";

/**
 * How a fixture module spells `import ... from "@byteveda/flexiq"`. A tmpdir has
 * no `node_modules` to resolve the package name through, and Vitest hands a file
 * URL and a relative specifier for the same file the same module instance — so a
 * fixture importing this URL gets the very registry the test imports.
 */
const SDK = new URL("../../src/index.ts", import.meta.url).href;

const trees: string[] = [];
let worker: Worker | undefined;

afterEach(() => {
  worker?.stop();
  worker = undefined;
  // The pending registry is module-global by design; without this a fixture
  // from one test would be drained by the next test's queue.
  clearPendingTasks();
  for (const dir of trees.splice(0)) {
    rmSync(dir, { recursive: true, force: true });
  }
});

/** Write `files` (path relative to the tree root -> source) into a fresh tmpdir. */
function writeTree(files: Record<string, string>): string {
  const root = mkdtempSync(join(tmpdir(), "flexiq-discover-"));
  trees.push(root);
  for (const [relative, source] of Object.entries(files)) {
    const full = join(root, relative);
    mkdirSync(dirname(full), { recursive: true });
    writeFileSync(full, source);
  }
  return root;
}

/** Import a file from a written tree — the module instance `discover` already ran. */
function loadModule(root: string, relative: string): Promise<Record<string, unknown>> {
  return import(pathToFileURL(join(root, relative)).href) as Promise<Record<string, unknown>>;
}

function newQueue(): Queue {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
  return new Queue({ dbPath });
}

async function waitForStatus(
  queue: Queue,
  id: string,
  predicate: (status: string) => boolean,
  timeoutMs = 5000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const job = queue.getJob(id);
    if (job && predicate(job.status)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error("timed out waiting for job state");
}

it("runs a task tree whose modules never import the module holding the queue", async () => {
  const dbPath = join(mkdtempSync(join(tmpdir(), "flexiq-node-")), "queue.db");
  const root = writeTree({
    // Imports the SDK and nothing else — no path back to app.mjs.
    "tasks/invoices.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      export const sendInvoice = task("invoices.send", (userId) => \`sent:\${userId}\`);
    `,
    "app.mjs": `
      import { fileURLToPath } from "node:url";
      import { Queue } from ${JSON.stringify(SDK)};
      export const queue = new Queue({ dbPath: ${JSON.stringify(dbPath)} });
      export const discovered = await queue.discover(
        fileURLToPath(new URL("./tasks", import.meta.url)),
      );
    `,
  });

  const app = (await loadModule(root, "app.mjs")) as { queue: Queue; discovered: string[] };
  expect(app.discovered).toEqual(["invoices.send"]);

  const { sendInvoice } = (await loadModule(root, "tasks/invoices.mjs")) as {
    sendInvoice: { enqueue: (args: unknown[]) => string };
  };
  const id = sendInvoice.enqueue(["u1"]);
  worker = app.queue.runWorker();

  await waitForStatus(app.queue, id, (status) => status === "complete");
  expect(app.queue.getResult(id)).toBe("sent:u1");
});

it("walks nested directories and skips node_modules and dot-directories", async () => {
  const root = writeTree({
    "tasks/top.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("walk.top", () => "top");
    `,
    "tasks/nested/deep/leaf.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("walk.leaf", () => "leaf");
    `,
    // Both would throw on import if the walk ever reached them.
    "tasks/node_modules/pkg/index.mjs": 'throw new Error("node_modules was walked");\n',
    "tasks/.hidden/skipped.mjs": 'throw new Error("a dot-directory was walked");\n',
  });

  const queue = newQueue();
  await expect(queue.discover(join(root, "tasks"))).resolves.toEqual(["walk.leaf", "walk.top"]);
});

it("fails with the offending path when a task module throws on import", async () => {
  const root = writeTree({
    "tasks/fine.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("broken.fine", () => "fine");
    `,
    "tasks/broken.mjs": 'throw new Error("module is broken");\n',
  });

  const queue = newQueue();
  const error = await queue.discover(join(root, "tasks")).catch((thrown: unknown) => thrown);

  expect(error).toBeInstanceOf(TaskDiscoveryError);
  const discovery = error as TaskDiscoveryError;
  expect(discovery.path).toBe(join(root, "tasks", "broken.mjs"));
  expect(discovery.message).toContain("module is broken");
  expect((discovery.cause as Error).message).toBe("module is broken");
});

it("fails with the resolved path when the task directory cannot be read", async () => {
  const missing = join(writeTree({}), "not-a-directory");
  const queue = newQueue();
  const error = await queue.discover(missing).catch((thrown: unknown) => thrown);

  expect(error).toBeInstanceOf(TaskDiscoveryError);
  expect((error as TaskDiscoveryError).path).toBe(missing);
  expect((error as Error).message).toContain(missing);
});

it("rejects two modules claiming one task name, naming the second file", async () => {
  const root = writeTree({
    "tasks/a.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("dup.name", () => "a");
    `,
    "tasks/b.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("dup.name", () => "b");
    `,
  });

  const queue = newQueue();
  const error = await queue.discover(join(root, "tasks")).catch((thrown: unknown) => thrown);

  // The collision throws inside the import, so discovery reports which file
  // raised it; `cause` keeps the type a caller would branch on.
  expect(error).toBeInstanceOf(TaskDiscoveryError);
  expect((error as TaskDiscoveryError).path).toBe(join(root, "tasks", "b.mjs"));
  expect((error as Error).cause).toBeInstanceOf(DuplicateTaskError);
  expect((error as Error).message).toContain('task "dup.name" is already registered');
});

it("rejects a discovered task colliding with one the queue already registered", async () => {
  const root = writeTree({
    "tasks/late.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("collide", () => "deferred");
    `,
  });

  const queue = newQueue();
  queue.task("collide", () => "bound");

  await expect(queue.discover(join(root, "tasks"))).rejects.toThrow(DuplicateTaskError);
});

it("replays the declared options into the queue, live callbacks included", async () => {
  const root = writeTree({
    "tasks/flaky.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      export const flaky = task("opts.flaky", () => { throw new Error("nope"); }, {
        maxRetries: 5,
        retryOn: () => false,
      });
    `,
  });

  const queue = newQueue();
  await queue.discover(join(root, "tasks"));

  // A budget of 5 with a `retryOn` that refuses every error dead-letters on the
  // first failure. Only a replayed live function can produce that — a dropped
  // `retryOn` would retry five times first.
  const id = queue.enqueue("opts.flaky", []);
  worker = queue.runWorker();
  await waitForStatus(queue, id, (status) => status === "dead");

  const job = queue.getJob(id);
  expect(job?.maxRetries).toBe(5);
  expect(job?.retryCount).toBe(0);
});

it("gives a second queue the same tasks and the handle's binding", async () => {
  const root = writeTree({
    "tasks/greet.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      export const greet = task("two.greet", (who) => \`hi \${who}\`);
    `,
  });

  const first = newQueue();
  await first.discover(join(root, "tasks"));
  const { greet } = (await loadModule(root, "tasks/greet.mjs")) as {
    greet: { enqueue: (args: unknown[]) => string };
  };

  // Constructing the second queue drains the registry again; the latest drain
  // owns the handle, so a first-wins binding would leave it on a dead queue.
  const second = newQueue();
  const id = greet.enqueue(["ada"]);

  expect(second.getJob(id)).toBeDefined();
  expect(first.getJob(id)).toBeNull();
});

it("is a no-op when the same directory is discovered twice", async () => {
  const root = writeTree({
    "tasks/once.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      export const once = task("repeat.once", () => "once");
    `,
  });

  const queue = newQueue();
  const firstNames = await queue.discover(join(root, "tasks"));
  const secondNames = await queue.discover(join(root, "tasks"));

  expect(firstNames).toEqual(["repeat.once"]);
  expect(secondNames).toEqual(firstNames);

  // Another queue takes the binding; re-discovering has to win it back rather
  // than skip the entry it already owns.
  const other = newQueue();
  const { once } = (await loadModule(root, "tasks/once.mjs")) as {
    once: { enqueue: (args: unknown[]) => string };
  };
  expect(other.getJob(once.enqueue([]))).toBeDefined();

  await queue.discover(join(root, "tasks"));
  const id = once.enqueue([]);
  expect(queue.getJob(id)).toBeDefined();
  expect(other.getJob(id)).toBeNull();
});

it("claims tasks imported before the queue existed, with no discover call", async () => {
  const root = writeTree({
    "tasks/early.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("early.run", () => "early");
    `,
  });
  await loadModule(root, "tasks/early.mjs");

  const queue = newQueue();
  const id = queue.enqueue("early.run", []);
  worker = queue.runWorker();

  await waitForStatus(queue, id, (status) => status === "complete");
  expect(queue.getResult(id)).toBe("early");
});

it("claims tasks imported after the queue was built, at worker start", async () => {
  const queue = newQueue();
  const root = writeTree({
    "tasks/late.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      export const late = task("late.run", () => "late");
    `,
  });
  const { late } = (await loadModule(root, "tasks/late.mjs")) as {
    late: { enqueue: (args: unknown[]) => string; bound: boolean };
  };
  expect(late.bound).toBe(false);

  worker = queue.runWorker();
  const id = late.enqueue([]);

  await waitForStatus(queue, id, (status) => status === "complete");
  expect(queue.getResult(id)).toBe("late");
});

it("refuses to enqueue a task no queue has drained", async () => {
  const root = writeTree({
    "tasks/orphan.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      export const orphan = task("orphan.run", () => "orphan");
    `,
  });
  const { orphan } = (await loadModule(root, "tasks/orphan.mjs")) as {
    orphan: { enqueue: (args: unknown[]) => string; bound: boolean };
  };

  expect(orphan.bound).toBe(false);
  expect(() => orphan.enqueue([])).toThrow(TaskNotBoundError);
  expect(() => orphan.enqueue([])).toThrow("queue.discover(...)");
});

it("keeps the handle callable and named, bound or not", async () => {
  const root = writeTree({
    "tasks/callable.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      export const shout = task("callable.shout", (word) => word.toUpperCase());
    `,
  });
  const { shout } = (await loadModule(root, "tasks/callable.mjs")) as {
    shout: ((word: string) => string) & {
      name: string;
      bound: boolean;
      enqueue: (args: unknown[]) => string;
    };
  };

  expect(shout("hi")).toBe("HI");
  expect(shout.name).toBe("callable.shout");

  const queue = newQueue();
  expect(shout.bound).toBe(true);
  expect(shout("hi")).toBe("HI");
  expect(queue.getJob(shout.enqueue(["hi"]))).toBeDefined();
});

it("imports only the extensions it was given, and never a declaration file", async () => {
  const root = writeTree({
    "tasks/kept.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("ext.kept", () => "kept");
    `,
    "tasks/skipped.ts": `
      import { task } from ${JSON.stringify(SDK)};
      task("ext.skipped", () => "skipped");
    `,
    // Declaration files share the `.ts` extension, so a `dist/` tree full of
    // them is what the exclusion is for. Given a task to declare, this one
    // shows up in the result the moment the walk imports it.
    "tasks/types.d.ts": `
      import { task } from ${JSON.stringify(SDK)};
      task("ext.declared", () => "declared");
    `,
  });

  const queue = newQueue();
  await expect(queue.discover(join(root, "tasks"), { extensions: [".mjs"] })).resolves.toEqual([
    "ext.kept",
  ]);
});

it("accepts an extension with or without its leading dot", async () => {
  const root = writeTree({
    "tasks/kept.mjs": `
      import { task } from ${JSON.stringify(SDK)};
      task("dotless.kept", () => "kept");
    `,
    "tasks/skipped.ts": `
      import { task } from ${JSON.stringify(SDK)};
      task("dotless.skipped", () => "skipped");
    `,
  });

  // `extname` always reports a leading dot, so an unnormalized `"mjs"` matches
  // nothing and discovery resolves empty — a misconfiguration that reads as an
  // empty task directory.
  const queue = newQueue();
  await expect(queue.discover(join(root, "tasks"), { extensions: ["mjs"] })).resolves.toEqual([
    "dotless.kept",
  ]);
});

it("imports TypeScript task modules by default", async () => {
  const root = writeTree({
    "tasks/typed.ts": `
      import { task } from ${JSON.stringify(SDK)};
      export const typed = task("ts.typed", (n: number): number => n * 2);
    `,
    "tasks/typed.d.ts": `
      import { task } from ${JSON.stringify(SDK)};
      task("ts.declared", () => "declared");
    `,
  });

  const queue = newQueue();
  await expect(queue.discover(join(root, "tasks"))).resolves.toEqual(["ts.typed"]);
});
