// Copy + code for the hero and the server fold on the docs index. The marketing
// sections this file used to feed live on flexiq.byteveda.org now; what is left
// is the snippets the root still shows.

import { SERVER_TIER, type Tier } from "./tier-registry";

/** A worker-output line: glyph + text, optionally a result value + timing. */
export interface OutLine {
  glyph: string;
  glyphKind: "p" | "g";
  text: string;
  value?: string;
  timing?: string;
}

/** One of the two buttons beside the hero snippet. */
export interface HeroCta {
  href: string;
  label: string;
}

/** One tab in the hero terminal: a tier, the snippet that opens it, and where
 *  its reader goes next. */
export interface HeroPane {
  /** Which tier this tab is. A language tab also sets the global SDK; see
   *  `hero.tsx` for why the tab strip is not simply the SDK switcher. */
  tier: Tier;
  /** Highlighter dialect for the snippet. */
  lang: "py" | "ts" | "java" | "sh";
  filename: string;
  /** Right-hand tag in the terminal title bar. Per pane because not every tab
   *  starts a worker — the server one starts a door and nothing else. */
  runtag: string;
  code: string;
  output: OutLine[];
  /** Both hrefs are absolute and come off the pane rather than the active SDK:
   *  a tier that is not a language has no `/<sdk>/…` page under it, and building
   *  one from the SDK store is the prefix corruption the tier split prevents. */
  primary: HeroCta;
  secondary: HeroCta;
}

/** Tiers shown in the hero tab strip as "Soon" (no pane yet). */
export const HERO_COMING_SOON: string[] = [];

/**
 * The one invocation that opens the network door.
 *
 * Written once because the page shows it twice — the hero's server tab and the
 * first card of the server fold are the same command, and a second copy is a
 * second thing to keep true. It is a transcript: see `SERVER_PANES` below.
 */
const SERVER_START = `FLEXIQ_DSN=sqlite:///tmp/flexiq.db \\
FLEXIQ_NAMESPACE=default \\
FLEXIQ_GRPC_LISTEN=127.0.0.1:50051 \\
flexiq-server`;

export const HERO_PANES: HeroPane[] = [
  {
    tier: "python",
    lang: "py",
    filename: "tasks.py",
    runtag: "worker · live",
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
    primary: {
      href: "/python/getting-started/quickstart",
      label: "Quickstart",
    },
    secondary: { href: "/python/modules", label: "Read Modules" },
  },
  {
    tier: "node",
    lang: "ts",
    filename: "tasks.ts",
    runtag: "worker · live",
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
    primary: { href: "/node/getting-started/quickstart", label: "Quickstart" },
    secondary: { href: "/node/modules", label: "Read Modules" },
  },
  {
    tier: "java",
    lang: "java",
    filename: "Tasks.java",
    runtag: "worker · live",
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
    primary: { href: "/java/getting-started/quickstart", label: "Quickstart" },
    secondary: { href: "/java/modules", label: "Read Modules" },
  },
  // The fourth tab is not a language, so its snippet is not one either: the
  // process that holds the credential, then a producer with no binding at all
  // enqueuing the same `add(2, 3)` the three tabs above define.
  //
  // `grpcurl` and not `curl` on purpose — the server fold further down this page
  // is the same door's HTTP/JSON binding, and printing one request twice teaches
  // less than printing both doors once.
  //
  // Where each line comes from: the invocation is `SERVER_START`, and the call
  // is the shape `examples/polyglot/grpc_producer.sh` sends — the working bash
  // producer `/server/clients` points a reader at. Bare `2` and `3` are legal
  // where that example passes an object: a structured argument is a
  // `google.protobuf.Value`, and the server refuses only an integer past 2^53.
  // The response fields are the fold's transcript, unchanged — both bindings
  // answer with the same `EnqueueResponse`.
  {
    tier: SERVER_TIER,
    lang: "sh",
    filename: "flexiq-server",
    // Not "worker · live": nothing in this snippet starts one. The door is what
    // is live, and whose worker drains the job is deliberately not its business.
    runtag: "door · live",
    code: `# terminal 1 · the door — the one process holding the DSN
${SERVER_START}

# terminal 2 · any producer — no binding, no CBOR library
grpcurl -plaintext -H "authorization: Bearer $FLEXIQ_TOKEN" \\
  -d '{"task_name": "add", "structured": {"args": [2, 3]}}' \\
  localhost:50051 flexiq.v1.ProducerService/Enqueue`,
    output: [
      {
        glyph: "→",
        glyphKind: "p",
        text: "[flexiq] gRPC listener on tcp://127.0.0.1:50051",
      },
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
    primary: { href: "/server", label: "When to use it" },
    secondary: { href: "/server/clients", label: "Write a client" },
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
    code: SERVER_START,
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
