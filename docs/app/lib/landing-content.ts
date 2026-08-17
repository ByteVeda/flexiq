// Landing-page copy + code, ported verbatim from the prototype (index.html / landing.js).

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
    install: "pip install taskito",
    code: `from taskito import Queue

queue = Queue(db_path="tasks.db")

@queue.task(max_retries=3)
def add(a: int, b: int) -> int:
    return a + b

job = add.delay(2, 3)
print(job.result())   # → 5`,
    output: [
      { glyph: "$", glyphKind: "p", text: "taskito worker --app tasks:queue" },
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
    install: "pnpm add taskito",
    code: `import { Queue } from "taskito";

const queue = new Queue({ dbPath: "flexiq.db" });

queue.task("add", (a: number, b: number) => a + b, {
  maxRetries: 3,
});

const id = queue.enqueue("add", [2, 3]);
queue.runWorker();

console.log(await queue.result(id)); // → 5`,
    output: [
      { glyph: "$", glyphKind: "p", text: "taskito run ./tasks.js" },
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
    install: 'implementation("org.byteveda:taskito")',
    code: `import org.byteveda.taskito.*;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;

Task<int[]> add = Task.of("add", int[].class).retries(3);
try (Taskito queue = Taskito.builder().sqlite("tasks.db").open();
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

export interface IconCard {
  icon: string;
  rect?: boolean;
  title: string;
  body: string;
  /** SDK-relative doc path (no leading slash, no `/python|/node` prefix). */
  href?: string;
}

export const FEATURES: IconCard[] = [
  {
    icon: "M13 2L3 14h7l-1 8 10-12h-7l1-8z",
    title: "Brokerless",
    body: "No Redis, no RabbitMQ. Everything in a single SQLite file — queue, results, rate limits, schedules. Just <code>pip install</code> or <code>pnpm add</code> and go.",
  },
  {
    icon: "M9 2v3M15 2v3M9 19v3M15 19v3M2 9h3M2 15h3M19 9h3M19 15h3",
    rect: true,
    title: "Rust-powered",
    body: "The scheduler, dispatcher, and storage engine are all Rust. Tokio runtime, OS-thread worker pool; thin PyO3, napi-rs and JNI boundaries keep per-SDK overhead negligible.",
  },
  {
    icon: "M22 12h-4l-3 9L9 3l-3 9H2",
    title: "One core, native SDKs",
    body: "First-class <b>Python</b>, <b>Node.js</b> and <b>Java</b> clients are peers over the same Rust core and store — enqueue in one runtime, run workers in another. Zero cross-language dependency.",
  },
  {
    icon: "M6 3v12M18 9a3 3 0 1 0 0 6 3 3 0 0 0 0-6zM6 21a3 3 0 1 0 0-6 3 3 0 0 0 0 6zM15 6a9 9 0 0 0-9 9",
    title: "DAG workflows",
    body: "Multi-step pipelines as directed acyclic graphs. Fan-out, fan-in, conditions, approval gates, sub-workflows, incremental re-runs, Mermaid viz.",
  },
  {
    icon: "M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5",
    title: "Resource system",
    body: "Inject database connections, HTTP clients, and cloud SDKs by name. Three-layer pipeline: argument interception, worker DI, transparent proxy.",
  },
  {
    icon: "M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z",
    title: "Production-ready",
    body: "Retries with exponential backoff, dead letter queue, rate limits, circuit breakers, distributed locks, structured logs, OTel/Sentry/Prometheus.",
  },
];

export const USE_CASES: IconCard[] = [
  {
    icon: "M12 2C6.48 2 2 4.02 2 6.5v11C2 19.98 6.48 22 12 22s10-2.02 10-4.5v-11C22 4.02 17.52 2 12 2zM2 6.5C2 8.43 6.48 10 12 10s10-1.57 10-3.5M2 12c0 1.93 4.48 3.5 10 3.5s10-1.57 10-3.5",
    title: "ETL pipelines",
    body: "Chain extract → transform → load as a DAG. Fan out across workers, fan in to aggregate, restart from any node on failure.",
    href: "more/examples/data-pipeline",
  },
  {
    icon: "M4 4h16a2 2 0 0 1 2 2v12a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2zM22 6l-10 7L2 6",
    title: "Email & notifications",
    body: "Bursty SMTP, push, or webhook delivery. Per-task rate limits keep providers happy; retries with backoff handle transient failures.",
    href: "more/examples/notifications",
  },
  {
    icon: "M9 2v3M15 2v3M9 19v3M15 19v3M2 9h3M2 15h3M19 9h3M19 15h3",
    rect: true,
    title: "ML inference & batch",
    body: "Long-running model jobs with progress tracking, soft timeouts, and prefork pools for true CPU parallelism without GIL contention.",
    href: "more/examples/benchmark",
  },
  {
    icon: "M12 6v6l4 2M12 22a10 10 0 1 1 0-20 10 10 0 0 1 0 20z",
    title: "Scheduled jobs",
    body: "Six-field cron syntax down to the second. Periodic tasks live in the scheduler — no separate beat daemon to babysit.",
    href: "guides/core/scheduling",
  },
];

export interface DeltaRow {
  label: string;
  taskito: string;
  celery: string;
}

export const DELTA: DeltaRow[] = [
  {
    label: "Install",
    taskito: "pip install taskito",
    celery: "pip install celery[redis] + run Redis daemon",
  },
  {
    label: "Background services",
    taskito: "<b>1</b> (worker)",
    celery: "3 (worker, beat, Redis)",
  },
  {
    label: "Default storage",
    taskito: "SQLite file <b>(built-in)</b>",
    celery: "Redis (separate daemon)",
  },
  {
    label: "Retry config above",
    taskito: "max_retries=3 <b>decorator arg</b>",
    celery: "try/except + self.retry(exc=…)",
  },
];

export interface IntegrationGroup {
  group: string;
  items: string[];
}

export const INTEGRATIONS: IntegrationGroup[] = [
  { group: "Python frameworks", items: ["Django", "FastAPI", "Flask"] },
  { group: "Node frameworks", items: ["Express", "Fastify", "NestJS"] },
  { group: "Storage", items: ["Postgres", "SQLite", "Redis"] },
  { group: "Observability", items: ["OpenTelemetry", "Sentry", "Prometheus"] },
];

export const CODE_TASKITO = `from taskito import Queue

queue = Queue(db_path="tasks.db")

@queue.task(max_retries=3, rate_limit="100/m")
def send_email(to, subject, body):
    smtp.send(to, subject, body)

# Enqueue + run
send_email.delay("a@x.com", "Hi", "Body")
# $ taskito worker --app tasks:queue`;

export const CODE_CELERY = `from celery import Celery

app = Celery("myapp",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1")
app.conf.task_default_rate_limit = "100/m"

@app.task(bind=True, max_retries=3)
def send_email(self, to, subject, body):
    try:
        smtp.send(to, subject, body)
    except SMTPError as exc:
        raise self.retry(exc=exc, countdown=60)

# $ celery -A myapp worker  (+ Redis)`;
