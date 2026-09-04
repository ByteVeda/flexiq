# FlexiQ Java SDK

A typed Java 17+ client over the FlexiQ Rust core, via a hand-written JNI shell
(`crates/flexiq-java`).

Feature-complete: producer + inspection + admin + logs, worker task execution,
middleware, JSON/signed/encrypted/MessagePack serializers, dashboard, webhooks,
CLI, distributed locks, periodic/cron, and the full workflow engine (DAG,
fan-out/fan-in, gates, conditions, sub-workflows, sagas, analysis +
visualization, canvas). Also: worker resources (DI), enqueue predicates, a KEDA
scaler endpoint, producer batching, in-process autoscaling, observability
middleware (Micrometer Observation + Sentry), and a Spring Boot 3 starter.
Baseline: **Java 17** (`--release 17`); on JDK 22+ hot byte ops take a Panama
(FFM) fast path automatically. The native library ships as a per-platform
classifier artifact next to a native-free main jar — add the classifier for
your platform (`linux-x86_64`, `linux-aarch64`, `osx-x86_64`, `osx-aarch64`,
`windows-x86_64`) and the runtime resolves it from the classpath.

## Install

```kotlin
// Gradle
implementation("org.byteveda:flexiq:1.1.0")
runtimeOnly("org.byteveda:flexiq:1.1.0:linux-x86_64") // native library for your platform
annotationProcessor("org.byteveda:flexiq-processor:1.1.0") // compile-time TaskHandler bindings
```

To pick the classifier automatically, use the
[osdetector](https://github.com/google/osdetector-gradle-plugin) plugin:

```kotlin
plugins { id("com.google.osdetector") version "1.7.3" }

dependencies {
    runtimeOnly("org.byteveda:flexiq:1.1.0:${osdetector.classifier}")
}
```

```xml
<!-- Maven: os-maven-plugin resolves ${os.detected.classifier} -->
<build>
  <extensions>
    <extension>
      <groupId>kr.motd.maven</groupId>
      <artifactId>os-maven-plugin</artifactId>
      <version>1.7.1</version>
    </extension>
  </extensions>
</build>

<dependency>
  <groupId>org.byteveda</groupId>
  <artifactId>flexiq</artifactId>
  <version>1.1.0</version>
</dependency>
<dependency>
  <groupId>org.byteveda</groupId>
  <artifactId>flexiq</artifactId>
  <version>1.1.0</version>
  <classifier>${os.detected.classifier}</classifier>
  <scope>runtime</scope>
</dependency>
```

Deploying one artifact to several platforms? Add multiple classifier
dependencies — each jar carries only its own library, and the loader picks the
right one at runtime. Supplying your own build instead (e.g. a custom feature
set, or a platform with no published artifact such as Windows on ARM): skip the
classifier and point `-Dflexiq.native.lib=/path/to/library` at it — the
classifier-free main jar works with no bundled native. On an unpublished
platform the loader fails at startup naming the platform it detected, rather
than loading a binary built for a different one.

```xml
<!-- Maven: the processor is wired through the compiler plugin, not a dependency -->
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-compiler-plugin</artifactId>
  <configuration>
    <annotationProcessorPaths>
      <path>
        <groupId>org.byteveda</groupId>
        <artifactId>flexiq-processor</artifactId>
        <version>1.1.0</version>
      </path>
    </annotationProcessorPaths>
  </configuration>
</plugin>
```

Companion artifacts: `org.byteveda:flexiq-test` (in-memory backend for unit
tests) and `org.byteveda:flexiq-spring` (Boot 3 starter).

## Migration

**0.18 — source-breaking (pre-1.0):** the client interface was renamed and the
name `Queue` now denotes a single named queue.

- The client you open is now `FlexiQ`, not `Queue`:
  `FlexiQ client = FlexiQ.builder()…open();` (was `Queue queue = …`).
  `FlexiQ.builder()` is unchanged.
- `Queue` is a per-queue handle from `FlexiQ.queue(name)`, exposing
  `pause()` / `resume()` / `isPaused()`.
- `client.pauseQueue("emails")` → `client.queue("emails").pause()` (likewise
  `resume`). `listPausedQueues()` stays on the client as the global view.

## Usage

### Enqueue

```java
import com.fasterxml.jackson.core.type.TypeReference;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobStatus;
import org.byteveda.flexiq.model.QueueStats;
import org.byteveda.flexiq.task.Task;
import java.util.Map;

// TypeReference preserves generics that a Class token can't; fluent options
// replace the EnqueueOptions builder for the common cases.
Task<Map<String, Object>> sendEmail =
        Task.of("send_email", new TypeReference<Map<String, Object>>() {})
                .queue("emails")
                .priority(5);

try (FlexiQ flexiq = FlexiQ.builder().sqlite("flexiq.db").open()) {
    String id = flexiq.enqueue(sendEmail, Map.of("to", "a@b.c"));
    Job job = flexiq.getJob(id).orElseThrow();   // job.status == JobStatus.PENDING
    QueueStats stats = flexiq.stats();
    flexiq.cancel(id);

    // Pause/resume are scoped to one named queue:
    flexiq.queue("emails").pause();
    flexiq.queue("emails").resume();
}
```

`FlexiQ.builder()` also has `.postgres(url)` / `.redis(url)` shortcuts.

### Run a worker

```java
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;

Task<Map> add = Task.of("add", Map.class);

FlexiQ flexiq = FlexiQ.builder().backend("sqlite").url("flexiq.db").open();
Worker worker = flexiq.worker()
        .handle(add, p -> ((Number) p.get("a")).intValue() + ((Number) p.get("b")).intValue())
        .concurrency(4)
        .on(EventName.SUCCESS, e -> System.out.println("done: " + e.jobId))
        .start();

// Close on SIGTERM/Ctrl-C; awaitShutdown() then unblocks. (Don't put the worker
// in try-with-resources AND call awaitShutdown() inside it — the block can't
// exit to trigger close(), so it would deadlock.)
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    worker.close();
    flexiq.close();
}));
worker.awaitShutdown();
```

### Typed tasks from `@TaskHandler` (compile-time, no reflection)

Annotate handler methods; a compile-time processor generates a `<Class>Tasks`
companion with a typed `Task` constant per method (full generics, name declared
once) plus a `bind(...)`. Add the processor with
`annotationProcessor("org.byteveda:flexiq-processor")`.

```java
class EmailTasks {
    @TaskHandler("send_email")          // explicit name
    String send(EmailPayload p) { ... }

    @TaskHandler                        // name defaults to "report"
    Report report(List<Metric> metrics) { ... }
}

// generated EmailTasksTasks:
String id = flexiq.enqueue(EmailTasksTasks.SEND, payload);

flexiq.worker()
        .apply(b -> EmailTasksTasks.bind(b, new EmailTasks()))
        .start();
```

The annotation is source-retention and the processor emits plain code — zero
runtime reflection, GraalVM-native-image friendly.

### Workflows

Define a DAG of tasks; steps run in topological order once every predecessor
finishes. Attach `trackWorkflows()` to the worker so node and run state advance
as jobs complete.

```java
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Worker;
import org.byteveda.flexiq.workflows.Workflow;
import org.byteveda.flexiq.workflows.WorkflowRun;
import org.byteveda.flexiq.workflows.WorkflowStatus;

Task<Integer> extract = Task.of("extract", Integer.class);
Task<Integer> transform = Task.of("transform", Integer.class);
Task<Integer> load = Task.of("load", Integer.class);

Workflow wf = Workflow.named("etl")
        .step("extract", extract, 1)
        .step("transform", transform, 2, "extract")
        .step("load", load, 3, "transform");

WorkflowRun run = flexiq.submitWorkflow(wf);

try (Worker worker = flexiq.worker()
        .handle(extract, p -> p * 10)
        .handle(transform, p -> p + 1)
        .handle(load, p -> p)
        .trackWorkflows()
        .start()) {
    WorkflowStatus status = run.await(Duration.ofSeconds(30));
    // status.state == WorkflowState.COMPLETED; status.node("load").get().status
}
```

A failed step (after its retries) fails the run and skips downstream nodes;
`run.cancel()` skips pending nodes.

Payloads can also be supplied **at submit** instead of baked into each step —
declare structural steps with `stepAfter(name, task, deps...)` and pass a map:

```java
Workflow etl = Workflow.named("etl")
        .stepAfter("extract", extract)
        .stepAfter("transform", transform, "extract")
        .stepAfter("load", load, "transform");

flexiq.submitWorkflow(etl, Map.of("extract", 5, "transform", 6, "load", 7));
```

A step's effective payload is `map.get(name)` when present, else the one baked
into the step.

**Fan-out / fan-in** map a step over a producer's result list and gather the
results:

```java
Workflow wf = Workflow.named("pipeline")
        .step("seed", seed, 4)                          // returns List.of(1,2,3,4)
        .fanOut("square", square, "each", "seed")       // runs square(x) per item
        .fanIn("sum", sum, "all", "square");            // sum receives [1,4,9,16]
```

`trackWorkflows()` advances run state from worker outcomes, so **every worker
that processes workflow jobs must enable it** — in a multi-worker deployment, a
run stalls on any node finished by a worker that did not opt in.

**Approval gates** park a step until it is resolved (or its timeout elapses);
register the workflow so the worker holds downstream payloads:

```java
Workflow wf = Workflow.named("deploy")
        .step("build", build, 1)
        .gate("approve", GateConfig.timeout(Duration.ofMinutes(30), GateAction.REJECT), "build")
        .step("ship", ship, 2, "approve");
try (Worker w = flexiq.worker().handle(build, ...).handle(ship, ...).trackWorkflows(wf).start()) {
    w.approveGate(run.runId(), "approve");   // or w.rejectGate(runId, "approve", reason)
}
```

**Conditions** gate a step on its predecessors' outcomes —
`Step.of(...).onFailure()` / `.onSuccess()` / `.always()`, or a callable
`condition(ctx -> ...)`. **Sub-workflows** run a child workflow as a step
(`Workflow.subWorkflow(name, child, after...)`). **Sagas** roll a failed run
back: `Step.of(...).compensate(undoTask)` compensates completed steps in
reverse order, ending the run `COMPENSATED`.

`WorkflowAnalysis` (topological order, levels, ancestors/descendants),
`WorkflowVisualization` (Mermaid / DOT), and `Canvas` (`chain`/`group`/`chord`)
round out the engine.

### Middleware

```java
flexiq.use(new Middleware() {
    @Override public void onEnqueue(EnqueueContext ctx) { /* validate / rewrite */ }
    @Override public void before(TaskContext ctx) { /* trace */ }
    @Override public void onDeadLetter(OutcomeEvent e) { /* alert */ }
});
```

### Dashboard

```java
try (DashboardServer dashboard = DashboardServer.start(queue, 8080, token, staticDir)) {
    // GET /api/stats, /api/jobs, /api/workers, ... ; POST /api/jobs/{id}/cancel, ...
    dashboard.port();
}
```

### Serializers

```java
byte[] key = ...; // 16/24/32 bytes for AES
FlexiQ secure = FlexiQ.builder()
        .backend("sqlite").url("flexiq.db")
        .serializer(new EncryptedSerializer(new JsonSerializer(), key))
        .open();
```

### Resources (worker dependency injection)

The primary way to use a non-serializable dependency (pool, client, logger) in a
handler: register it once and resolve it inside the worker. Scopes: `WORKER`
(built once, shared) and `TASK` (built + disposed per invocation).

```java
flexiq.resource("db", ctx -> openPool());                       // WORKER
flexiq.resource("tx", ResourceScope.TASK, ctx -> ctx.<Pool>use("db").begin(), Tx::close);
flexiq.worker().handle(save, p -> Resources.<Tx>use("tx").save(p)).start();
```

When a handler takes `@Resource` parameters, the `@TaskHandler` processor wires
them from the runtime for you — no `Resources.use` call needed.

### Cross-process references (proxies)

Secondary to resources: when a *specific* resource identity must travel inside a
payload to another process, carry a signed `ProxyRef` and rebuild it on the
worker. Bind an optional TTL and purpose — both are folded into the HMAC.

```java
Proxies proxies = new Proxies(hmacKey).register(new FileProxyHandler());
ProxyRef ref = proxies.deconstruct(file, Duration.ofMinutes(5), "report"); // producer
File same = proxies.resolve(ref, "report");                                // worker (expiry + purpose checked)
```

### Enqueue gates

```java
flexiq.predicate("send_email", ctx -> payloadValid(ctx));       // boolean: false → PredicateRejectedException
flexiq.gate("send_email", Recipes.businessHours(zone));         // allow / skip / defer / reject
Optional<String> id = flexiq.tryEnqueue(emailTask, msg);        // empty when a gate skips
// Recipes: businessHours / timeWindow / dayOfWeek / payloadMatches / featureFlag.
```

### Producer batching

```java
try (Batcher<Event> batcher = Batcher.of(flexiq, ingest, 500, Duration.ofMillis(200))) {
    events.forEach(batcher::add);   // flushed in one enqueueMany when full or after the delay
}
```

### Autoscaling + scaler endpoint

```java
flexiq.worker().autoscale(AutoscaleOptions.of(2, 32)).handle(task, ...).start();  // resize by depth
try (Scaler scaler = Scaler.start(flexiq, ScalerOptions.onPort(9090))) { /* GET /api/scaler for KEDA */ }
```

### Observability + MessagePack (optional deps)

```java
flexiq.use(new FlexiQObservation(observationRegistry));   // Micrometer (metrics + tracing)
flexiq.use(new SentryMiddleware());                        // report failures to Sentry
FlexiQ.builder().sqlite("t.db").serializer(new MsgpackSerializer()).open();
```

`io.micrometer:micrometer-observation`, `io.sentry:sentry`, and
`org.msgpack:jackson-dataformat-msgpack` are `compileOnly` — add the one you use.

### Spring Boot 3 starter

Add `org.byteveda:flexiq-spring`; it auto-configures a `FlexiQ` bean from
`flexiq.url` / `flexiq.pool-size` / `flexiq.namespace`. Define your own
`FlexiQ` bean to override it.

## Structure

Packages are organized by feature; the root holds only the front door.

```text
org.byteveda.flexiq
├── FlexiQ            client interface + entry — FlexiQ.builder()...open()
├── Queue              named-queue handle (pause/resume) — FlexiQ.queue(name)
├── DefaultFlexiQ     package-private client impl (not exported)
├── NamedQueue         package-private Queue impl (not exported)
├── FlexiQException   unchecked error base type
├── errors/            typed exceptions (Serialization/Crypto, Workflow, Lock,
│                      Configuration, Webhook, Resource, PredicateRejected)
├── task/              Task, TaskFunction, EnqueueOptions
├── model/             Job, JobStatus, QueueStats, DeadJob, JobError,
│                      TaskMetric, WorkerInfo, TaskLog, JobFilter  (read-only views)
├── worker/            Worker runtime (concurrency, autoscale)
├── resources/         worker DI — ResourceRuntime, Resources.use(name), scopes
├── proxies/           signed cross-process references — Proxies, ProxyRef, handlers
├── interception/      enqueue-time arg interception — Interceptor, Interception
├── predicates/        enqueue gates — Predicate, EnqueueGate, EnqueueDecision, Recipes
├── batch/             Batcher — producer-side batching
├── autoscale/         Autoscaler, AutoscaleOptions
├── scaler/            Scaler — KEDA HTTP endpoint
├── locks/             Lock, LockInfo
├── scheduling/        PeriodicTask
├── workflows/         DAG builder, run, status, tracker; gates, conditions,
│                      sub-workflows, sagas, Canvas, analysis + visualization
├── serialization/     Serializer SPI + JsonSerializer default; Signed/Encrypted/Msgpack
├── annotation/        @TaskHandler (source-retention; see :processor)
├── middleware/        Middleware hooks
├── contrib/           observability — FlexiQObservation (Micrometer), SentryMiddleware
├── events/            worker outcome events
├── dashboard/ webhooks/ cli/
├── spi/               QueueBackend — seam between API and the native layer
└── internal/          JNI bindings (NativeQueue, NativeWorkflows, NativeLoader, ...)
```

Subprojects: `:processor` (compile-time `@TaskHandler`), `:test-support`
(`flexiq-test` in-memory backend), `:spring` (`flexiq-spring` Boot 3 starter),
`:graalvm-smoke` (native-image CI check).

The `:processor` subproject is a standalone compile-time annotation processor
(`TaskHandlerProcessor`) — it depends on nothing, reading `@TaskHandler`
structurally and emitting plain task companions.

The `spi.QueueBackend` seam keeps the public API independent of JNI: it can be
backed by the native library (default) or an in-memory fake in tests, and leaves
room for a future FFM/Panama backend without touching the API.

## Build & checks

```bash
./gradlew build            # cargoBuild → stage native → compile → test → jar
./gradlew test             # JUnit 5
./gradlew spotlessApply    # format (palantir-java-format)
./gradlew spotlessCheck    # verify formatting
./gradlew checkstyleMain   # static analysis
./gradlew check            # test + spotlessCheck + checkstyle
```

The Rust shell is built and checked with the workspace tooling:

```bash
cargo build -p flexiq-java --release --features postgres,redis
cargo fmt -p flexiq-java -- --check
cargo clippy -p flexiq-java -- -D warnings
```

### Native library resolution

At runtime the platform binary is extracted from the JAR and loaded. For local
development against a freshly built library, point the loader at it directly:

```bash
-Dflexiq.native.lib=/abs/path/to/target/release/libflexiq_java.so
```

On JDK 22+ the hot byte ops use a Project Panama (FFM) fast path; older JDKs (or
a jar built without the overlay) transparently fall back to JNI. FFM calls a
restricted native method, so a future JDK will deny it by default. The jar's
`Enable-Native-Access: ALL-UNNAMED` manifest attribute only covers `java -jar`;
apps that use the SDK as a classpath dependency should launch with
`--enable-native-access=ALL-UNNAMED` to grant access and silence the warning.
