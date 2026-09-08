// Copy + code for the hero and the server fold on the docs index. The marketing
// sections this file used to feed live on flexiq.byteveda.org now; what is left
// is the snippets the root still shows.

import type { Sdk } from "./sdk-registry";

/** A worker-output line: glyph + text, optionally a result value + timing. */
export interface OutLine {
  glyph: string;
  glyphKind: "p" | "g";
  text: string;
  value?: string;
  timing?: string;
}

export interface LangPane {
  /** Which SDK this snippet is for — selecting it sets the global SDK. */
  sdk: Sdk;
  /** Highlighter dialect for the snippet. */
  lang: "py" | "ts" | "java";
  filename: string;
  install: string;
  code: string;
  output: OutLine[];
  docHref: string;
  docLabel: string;
}

/** SDKs shown in the hero tab strip as "Soon" (no pane yet). */
export const HERO_COMING_SOON: string[] = [];

export const HERO_PANES: LangPane[] = [
  {
    sdk: "python",
    lang: "py",
    filename: "tasks.py",
    install: "pip install flexiq",
    code: `from flexiq import Queue

queue = Queue(db_path="tasks.db")

@queue.task(max_retries=3)
def add(a: int, b: int) -> int:
    return a + b

job = add.delay(2, 3)
print(job.result())   # → 5`,
    output: [
      { glyph: "$", glyphKind: "p", text: "flexiq worker --app tasks:queue" },
      {
        glyph: "→",
        glyphKind: "p",
        text: "scheduler online · 6 workers ready",
      },
      {
        glyph: "✓",
        glyphKind: "g",
        text: "add(2, 3) =",
        value: "5",
        timing: "12 ms",
      },
    ],
    docHref: "/python/getting-started/quickstart",
    docLabel: "Read the Python quickstart",
  },
  {
    sdk: "node",
    lang: "ts",
    filename: "tasks.ts",
    install: "pnpm add flexiq",
    code: `import { Queue } from "flexiq";

const queue = new Queue({ dbPath: "flexiq.db" });

queue.task("add", (a: number, b: number) => a + b, {
  maxRetries: 3,
});

const id = queue.enqueue("add", [2, 3]);
queue.runWorker();

console.log(await queue.result(id)); // → 5`,
    output: [
      { glyph: "$", glyphKind: "p", text: "flexiq run ./tasks.js" },
      { glyph: "→", glyphKind: "p", text: "runWorker() · Rust core attached" },
      {
        glyph: "✓",
        glyphKind: "g",
        text: "add(2, 3) =",
        value: "5",
        timing: "9 ms",
      },
    ],
    docHref: "/node/getting-started/quickstart",
    docLabel: "Read the Node.js quickstart",
  },
  {
    sdk: "java",
    lang: "java",
    filename: "Tasks.java",
    install: 'implementation("org.byteveda:flexiq")',
    code: `import org.byteveda.flexiq.*;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;

Task<int[]> add = Task.of("add", int[].class).retries(3);
try (FlexiQ queue = FlexiQ.builder().sqlite("tasks.db").open();
     Worker worker = queue.worker()
         .handle(add, p -> p[0] + p[1])
         .start()) {
  String id = queue.enqueue(add, new int[] {2, 3});
  queue.awaitJob(id, java.time.Duration.ofSeconds(10));
  var sum = queue.getResult(id, Integer.class).orElseThrow();
  System.out.println(sum); // → 5
}`,
    output: [
      { glyph: "$", glyphKind: "p", text: "java -cp app.jar Tasks" },
      {
        glyph: "→",
        glyphKind: "p",
        text: "worker started · Rust core attached",
      },
      {
        glyph: "✓",
        glyphKind: "g",
        text: "add(2, 3) =",
        value: "5",
        timing: "10 ms",
      },
    ],
    docHref: "/java/getting-started/quickstart",
    docLabel: "Read the Java quickstart",
  },
];

/** One terminal card in the server fold: a shell snippet and what running it printed. */
export interface ServerPane {
  /** Filename slot in the card's title bar. */
  filename: string;
  /** Right-hand tag in the title bar, describing what the card is. */
  tag: string;
  code: string;
  output: OutLine[];
}

/**
 * The server fold — the docs index's one mention of the network door.
 *
 * Both snippets are transcripts, not compositions. They were run against a
 * `flexiq-server` built with the `grpc` feature, and the output lines are the
 * log line and the response fields that run produced. That matters more here
 * than anywhere else on the site: `POST /v1/jobs` takes `taskName` and
 * `structured.args` and *refuses* an unknown field, so a body guessed from the
 * proto — `{"task": …, "args": […]}` — comes back 400, and a reader who copies
 * it concludes the door does not work.
 */
export const SERVER_PANES: ServerPane[] = [
  {
    filename: "flexiq-server",
    tag: "holds the credential",
    code: `FLEXIQ_DSN=sqlite:///tmp/flexiq.db \\
FLEXIQ_NAMESPACE=default \\
FLEXIQ_GRPC_LISTEN=127.0.0.1:50051 \\
flexiq-server`,
    output: [
      {
        glyph: "→",
        glyphKind: "p",
        text: "[flexiq] gRPC listener on tcp://127.0.0.1:50051",
      },
    ],
  },
  {
    filename: "any client",
    tag: "no SDK · no CBOR",
    code: `curl -X POST http://localhost:50051/v1/jobs \\
  -H "authorization: Bearer $FLEXIQ_TOKEN" \\
  -H "content-type: application/json" \\
  -d '{"taskName": "send_email",
       "structured": {"args": [{"to": "ada@example.com"}]}}'`,
    output: [
      { glyph: "→", glyphKind: "p", text: "HTTP/1.1 200 OK" },
      {
        glyph: "✓",
        glyphKind: "g",
        text: "job.id",
        value: "01a08003-3a94-74d0-89b7-f1d5d0ca829e",
      },
      {
        glyph: "✓",
        glyphKind: "g",
        text: "job.status",
        value: "JOB_STATUS_PENDING",
      },
    ],
  },
];
