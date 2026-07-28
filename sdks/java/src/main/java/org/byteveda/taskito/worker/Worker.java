package org.byteveda.taskito.worker;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.byteveda.taskito.autoscale.AutoscaleOptions;
import org.byteveda.taskito.autoscale.Autoscaler;
import org.byteveda.taskito.errors.SerializationException;
import org.byteveda.taskito.errors.WorkflowException;
import org.byteveda.taskito.events.Emitter;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.events.OutcomeEvent;
import org.byteveda.taskito.events.TaskitoEvent;
import org.byteveda.taskito.events.WorkerEvent;
import org.byteveda.taskito.logging.TaskitoLogger;
import org.byteveda.taskito.middleware.Middleware;
import org.byteveda.taskito.pubsub.LogConsumerConfig;
import org.byteveda.taskito.pubsub.SubscriptionConfig;
import org.byteveda.taskito.resources.ResourceRuntime;
import org.byteveda.taskito.serialization.PayloadCodec;
import org.byteveda.taskito.serialization.Serializer;
import org.byteveda.taskito.spi.QueueBackend;
import org.byteveda.taskito.spi.WorkerControl;
import org.byteveda.taskito.task.CircuitBreakerConfig;
import org.byteveda.taskito.task.RetryPolicy;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.task.TaskFunction;
import org.byteveda.taskito.workflows.Workflow;
import org.byteveda.taskito.workflows.WorkflowTracker;
import org.jspecify.annotations.Nullable;

/**
 * A running worker. Build one with {@link org.byteveda.taskito.Taskito#worker()}, register handlers,
 * then {@link Builder#start()}. {@link #close()} stops it and drains in-flight
 * jobs.
 */
public final class Worker implements AutoCloseable {
    private static final TaskitoLogger LOG = TaskitoLogger.create("worker");
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;
    private static final long LOG_CONSUMER_JOIN_TIMEOUT_SECONDS = 10;
    private static final ObjectMapper MESH_JSON = new ObjectMapper();

    private final WorkerControl control;
    private final ExecutorService executor;
    private final @Nullable WorkflowTracker tracker;
    private final ResourceRuntime resources;
    private final @Nullable Autoscaler autoscaler;
    private final List<LogConsumerThread> logConsumers;
    private final Emitter emitter;
    private final List<String> servedQueues;
    /** The owning client's live-worker tracker, or null for a manually built worker. */
    private final @Nullable WorkerLifecycle lifecycle;
    /** Guards the one-shot {@code worker.stopped} emission shared by {@link #stop()} and {@link #close()}. */
    private final AtomicBoolean stoppedEmitted = new AtomicBoolean();

    private final CountDownLatch shutdown = new CountDownLatch(1);
    private boolean closed;

    private Worker(
            WorkerControl control,
            ExecutorService executor,
            @Nullable WorkflowTracker tracker,
            ResourceRuntime resources,
            @Nullable Autoscaler autoscaler,
            List<LogConsumerThread> logConsumers,
            Emitter emitter,
            List<String> servedQueues,
            @Nullable WorkerLifecycle lifecycle) {
        this.control = control;
        this.executor = executor;
        this.tracker = tracker;
        this.resources = resources;
        this.autoscaler = autoscaler;
        this.logConsumers = logConsumers;
        this.emitter = emitter;
        this.servedQueues = servedQueues;
        this.lifecycle = lifecycle;
    }

    public static Builder builder(QueueBackend backend, Serializer serializer, List<Middleware> middleware) {
        return new Builder(backend, serializer, middleware, new ResourceRuntime(), Map.of());
    }

    public static Builder builder(
            QueueBackend backend,
            Serializer serializer,
            List<Middleware> middleware,
            ResourceRuntime resources,
            Map<String, PayloadCodec> codecs) {
        return new Builder(backend, serializer, middleware, resources, codecs);
    }

    /** Stop dispatching; in-flight jobs continue to drain. */
    public void stop() {
        control.stop();
        emitStopped();
    }

    /** Emit {@code worker.stopped} exactly once across {@link #stop()} and {@link #close()}. */
    private void emitStopped() {
        if (stoppedEmitted.compareAndSet(false, true)) {
            emitter.emit(new WorkerEvent(EventName.WORKER_STOPPED, servedQueues));
        }
    }

    /**
     * A snapshot of this worker's mesh cluster view (peer count, capacity, load),
     * or empty when the worker was not started with {@link Builder#mesh}.
     */
    public Optional<MeshClusterInfo> meshClusterInfo() {
        return control.meshClusterInfoJson().map(json -> {
            try {
                return MESH_JSON.readValue(json, MeshClusterInfo.class);
            } catch (Exception e) {
                throw new SerializationException("failed to decode mesh cluster info", e);
            }
        });
    }

    /**
     * Approve a parked workflow gate so its successors run. Requires this worker
     * to be tracking workflows ({@code trackWorkflows()} on the builder).
     */
    public void approveGate(String runId, String nodeName) {
        requireTracker().resolveGate(runId, nodeName, true, null);
    }

    /** Reject a parked workflow gate; the gate fails and its successors are skipped. */
    public void rejectGate(String runId, String nodeName, @Nullable String reason) {
        requireTracker().resolveGate(runId, nodeName, false, reason == null ? "gate rejected" : reason);
    }

    private WorkflowTracker requireTracker() {
        if (tracker == null) {
            throw new WorkflowException("worker is not tracking workflows; call trackWorkflows() on the builder");
        }
        return tracker;
    }

    /** Block until {@link #close()} is called. */
    public void awaitShutdown() throws InterruptedException {
        shutdown.await();
    }

    /**
     * Stop dispatching, drain in-flight handler tasks, then free the native
     * worker. Draining BEFORE {@code control.close()} is essential: a running
     * handler may still call back into the native worker
     * ({@code completeJob}/{@code failJob}), so the handle must outlive every
     * handler task. Idempotent.
     */
    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        // Drop out of the owning client's live set first: a concurrent
        // Taskito.shutdown() should not queue behind this teardown to learn it.
        if (lifecycle != null) {
            lifecycle.closed(this);
        }
        if (autoscaler != null) {
            autoscaler.close(); // stop resizing before we tear the pool down
        }
        control.stop(); // stop scheduling new work
        emitStopped();
        stopLogConsumers(); // drain managed consumers before the backend tears down
        executor.shutdown(); // stop accepting; let running handlers finish
        try {
            if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                // shutdownNow() only *requests* interruption; give stuck handlers
                // one more window before closing the handle out from under them.
                executor.shutdownNow();
                if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    LOG.warn("handler threads still running after " + (2 * SHUTDOWN_TIMEOUT_SECONDS)
                            + "s; closing the native worker — their late completions will be rejected");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        // Safe even if a straggler survives: JniWorkerControl serializes close()
        // against in-flight native calls and rejects any call made afterwards.
        control.close();
        emitter.emit(new WorkerEvent(EventName.WORKER_OFFLINE, servedQueues));
        if (tracker != null) {
            tracker.close(); // stop the gate-timeout scheduler
        }
        resources.teardownWorker(); // dispose worker-scoped resources when the last lease drops
        shutdown.countDown();
    }

    /**
     * Signal each managed log consumer to stop, then join it within a bounded window
     * so a wedged handler can't block shutdown (the threads are daemons either way).
     * Signal all first so they drain concurrently rather than one join at a time.
     */
    private void stopLogConsumers() {
        for (LogConsumerThread consumer : logConsumers) {
            consumer.shutdown();
        }
        for (LogConsumerThread consumer : logConsumers) {
            try {
                consumer.join(TimeUnit.SECONDS.toMillis(LOG_CONSUMER_JOIN_TIMEOUT_SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Registers handlers and worker options, then starts the worker. */
    public static final class Builder {
        private static final ObjectMapper JSON = new ObjectMapper();

        private final QueueBackend backend;
        private final Serializer serializer;
        private final List<Middleware> middleware;
        private final ResourceRuntime resources;
        private final Map<String, PayloadCodec> codecs;
        private final Map<String, RegisteredTask> handlers = new HashMap<>();
        private List<LogConsumerConfig> logConsumers = List.of();
        private @Nullable LogTopicReader logTopicReader;
        /**
         * Tasks carrying policy the worker must register, keyed by name. Holding the
         * task itself rather than a map per knob keeps {@link #encodeTaskConfigs}
         * reading from one source: a new knob is then encoded in one place instead of
         * needing its own map, its own capture, and its own arm of a name union.
         */
        private final Map<String, Task<?>> taskPolicies = new LinkedHashMap<>();

        private final Map<EventName, List<Consumer<TaskitoEvent>>> listeners = new EnumMap<>(EventName.class);
        private List<SubscriptionConfig> subscriptions = List.of();
        private Supplier<List<Map<String, Object>>> queueConfigs = List::of;
        private @Nullable List<String> queues;
        private int concurrency;
        private @Nullable Integer channelCapacity;
        private @Nullable Integer batchSize;
        private @Nullable WorkflowTracker tracker;
        private @Nullable AutoscaleOptions autoscale;
        private @Nullable MeshOptions mesh;
        private @Nullable Retention retention;
        private boolean pushDispatch;
        private @Nullable Emitter hub;
        private @Nullable WorkerLifecycle lifecycle;

        Builder(
                QueueBackend backend,
                Serializer serializer,
                List<Middleware> middleware,
                ResourceRuntime resources,
                Map<String, PayloadCodec> codecs) {
            this.backend = backend;
            this.serializer = serializer;
            this.middleware = middleware;
            this.resources = resources;
            this.codecs = codecs;
        }

        public <T, R> Builder handle(String taskName, Class<T> payloadType, TaskFunction<T, R> handler) {
            handlers.put(taskName, new RegisteredTask(payloadType, cast(handler), List.of(), null));
            return this;
        }

        public <T, R> Builder handle(Task<T> task, TaskFunction<T, R> handler) {
            handlers.put(
                    task.name(),
                    new RegisteredTask(task.payloadType(), cast(handler), task.codecNames(), task.retryOn()));
            capturePolicy(task);
            return this;
        }

        /** Apply a customizer to this builder (e.g. a generated {@code XxxTasks.bind}). */
        public Builder apply(Consumer<Builder> customizer) {
            customizer.accept(this);
            return this;
        }

        /** Register a single {@link Handler} (a task + its function). */
        public Builder register(Handler<?, ?> handler) {
            handlers.put(
                    handler.task().name(),
                    new RegisteredTask(
                            handler.task().payloadType(),
                            cast(handler.function()),
                            handler.task().codecNames(),
                            handler.task().retryOn()));
            capturePolicy(handler.task());
            return this;
        }

        /** Remember a task's policy — retries, breaker, throttling, caps — so {@code start()} registers it. */
        private void capturePolicy(Task<?> task) {
            if (hasPolicy(task)) {
                taskPolicies.put(task.name(), task);
            }
        }

        /** Whether a task sets anything the scheduler needs registering. */
        private static boolean hasPolicy(Task<?> task) {
            return task.retryPolicy() != null
                    || task.circuitBreaker() != null
                    || task.rateLimit() != null
                    || task.retryBudget() != null
                    || task.maxConcurrent() != null
                    || task.maxInFlightPerTask() != null;
        }

        /** Register every handler in a {@link HandlerRegistry} (e.g. a generated {@code XxxTasks.handlers}). */
        public Builder register(HandlerRegistry registry) {
            registry.handlers().forEach(this::register);
            return this;
        }

        /**
         * Topic subscriptions to register at {@code start()} (wired by
         * {@code Taskito.worker()}). Ephemeral entries bind to the started
         * worker's id and are reaped once it stops heartbeating.
         */
        public Builder subscriptions(List<SubscriptionConfig> subscriptions) {
            this.subscriptions = subscriptions;
            return this;
        }

        /**
         * Managed log-topic consumers to drive with a poll loop for the worker's life
         * (wired by {@code Taskito.worker()}). The {@code reader} pulls and acks each
         * topic's cursor; a manually built worker declares none.
         */
        public Builder logConsumers(List<LogConsumerConfig> logConsumers, LogTopicReader reader) {
            this.logConsumers = logConsumers;
            this.logTopicReader = reader;
            return this;
        }

        /**
         * Late-bound per-queue scheduler config in wire shape (CoDel + dispatch
         * order). Supplied by the owning {@code Taskito} via {@code Taskito.worker()}
         * and resolved at {@code start()}, so config set *after* the builder was
         * obtained (e.g. {@code Taskito.codel(...)}) is still picked up — matching
         * that method's documented timing. A manually built worker leaves it empty.
         */
        public Builder queueConfigs(Supplier<List<Map<String, Object>>> queueConfigs) {
            this.queueConfigs = queueConfigs;
            return this;
        }

        public Builder queues(String... queues) {
            this.queues = Arrays.asList(queues);
            return this;
        }

        /** Fixed handler-thread count; 0 (default) uses a cached pool. */
        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder channelCapacity(int channelCapacity) {
            this.channelCapacity = channelCapacity;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        /** Per-table retention windows for auto-cleanup. */
        public Builder retention(Retention retention) {
            this.retention = retention;
            return this;
        }

        /**
         * Opt into event-driven dispatch: an enqueue wakes the scheduler
         * immediately instead of it waiting for the next poll, removing the
         * dispatch latency floor and the idle database load of polling.
         * Requires the native library to be built with the {@code push-dispatch}
         * cargo feature; otherwise accepted and ignored (polling is kept).
         */
        public Builder pushDispatch(boolean pushDispatch) {
            this.pushDispatch = pushDispatch;
            return this;
        }

        /**
         * Autoscale the handler pool between {@code min} and {@code max} threads
         * based on queue depth. Replaces the fixed/cached pool with a resizable
         * one driven by an {@link Autoscaler}.
         */
        public Builder autoscale(AutoscaleOptions options) {
            this.autoscale = options;
            return this;
        }

        /**
         * Join a scheduling mesh: discover peers via gossip and steal work from
         * busy ones. The DB stays the source of truth — mesh only changes how
         * ready jobs are buffered and balanced, so it is safe to add to any worker.
         */
        public Builder mesh(MeshOptions options) {
            this.mesh = options;
            return this;
        }

        /**
         * Listen to a job outcome's {@link OutcomeEvent}s. Only valid for names
         * where {@link EventName#isJobOutcome()} holds; subscribe to other
         * events via {@link #onEvent}.
         */
        public Builder on(EventName name, Consumer<OutcomeEvent> listener) {
            name.requireJobOutcome();
            return onEvent(name, event -> listener.accept((OutcomeEvent) event));
        }

        /** Listen to any of this worker's events by name; the listener narrows to the concrete type. */
        public Builder onEvent(EventName name, Consumer<TaskitoEvent> listener) {
            listeners.computeIfAbsent(name, key -> new ArrayList<>()).add(listener);
            return this;
        }

        /**
         * Forward this worker's events to a queue-level {@link Emitter} (wired by
         * {@code Taskito.worker()}), so listeners registered via the queue's
         * {@code onEvent} also see them.
         */
        public Builder eventHub(Emitter hub) {
            this.hub = hub;
            return this;
        }

        /**
         * Report this worker's start and close to its owning client (wired by
         * {@code Taskito.worker()}), so {@code Taskito.shutdown()} can close every
         * worker started from that client. A manually built worker declares none.
         */
        public Builder lifecycle(WorkerLifecycle lifecycle) {
            this.lifecycle = lifecycle;
            return this;
        }

        /** Drive workflow node and run state from this worker's job outcomes. */
        public Builder trackWorkflows() {
            ensureTracker();
            return this;
        }

        /**
         * Track workflows and register {@code workflows} so the tracker can supply
         * the payloads of their deferred nodes (gates' downstream steps, etc.).
         */
        public Builder trackWorkflows(Workflow... workflows) {
            WorkflowTracker active = ensureTracker();
            for (Workflow workflow : workflows) {
                active.register(workflow);
            }
            return this;
        }

        /** Create the tracker and wire its outcome listeners once. */
        private WorkflowTracker ensureTracker() {
            if (tracker == null) {
                tracker = new WorkflowTracker(backend, serializer);
                on(EventName.SUCCESS, tracker::onSuccess);
                on(EventName.DEAD, tracker::onDead);
            }
            return tracker;
        }

        public Worker start() {
            @Nullable Autoscaler scaler = null;
            ExecutorService executor;
            if (autoscale != null) {
                ThreadPoolExecutor pool = new ThreadPoolExecutor(
                        autoscale.minWorkers(),
                        autoscale.maxWorkers(),
                        60L,
                        TimeUnit.SECONDS,
                        new LinkedBlockingQueue<>());
                scaler = new Autoscaler(pool, this::currentDepth, autoscale);
                executor = pool;
            } else {
                executor =
                        concurrency > 0 ? Executors.newFixedThreadPool(concurrency) : Executors.newCachedThreadPool();
            }
            Emitter emitter = new Emitter(hub);
            listeners.forEach((name, bound) -> bound.forEach(listener -> emitter.onEvent(name, listener)));
            if (tracker != null) {
                tracker.bindEmitter(emitter);
            }
            WorkerDispatchBridge bridge = new WorkerDispatchBridge(
                    backend, handlers, serializer, executor, emitter, middleware, resources, codecs);
            List<String> servedQueues = queues == null ? List.of() : List.copyOf(queues);
            WorkerControl control = backend.startWorker(bridge, encodeOptions());
            bridge.bind(control);
            // Emit only after the bridge is bound: a synchronous listener may
            // enqueue and join a job, which needs a live control. STARTED never
            // fires for a worker whose native start threw.
            emitter.emit(new WorkerEvent(EventName.WORKER_STARTED, servedQueues));
            emitter.emit(new WorkerEvent(EventName.WORKER_ONLINE, servedQueues));
            // Lease worker resources only after the native worker started cleanly.
            resources.acquireWorker();
            if (scaler != null) {
                scaler.start();
            }
            // Managed log-topic consumers: one poll loop each, beside the worker.
            List<LogConsumerThread> consumerThreads = startLogConsumers();
            Worker worker = new Worker(
                    control, executor, tracker, resources, scaler, consumerThreads, emitter, servedQueues, lifecycle);
            if (lifecycle != null) {
                lifecycle.started(worker);
            }
            return worker;
        }

        /** Spawn one daemon poll loop per declared log consumer; empty when none. */
        private List<LogConsumerThread> startLogConsumers() {
            if (logConsumers.isEmpty()) {
                return List.of();
            }
            // Non-empty consumers imply a reader: logConsumers(..) sets both together.
            LogTopicReader reader = Objects.requireNonNull(logTopicReader, "log consumers need a reader");
            List<LogConsumerThread> threads = new ArrayList<>(logConsumers.size());
            for (LogConsumerConfig config : logConsumers) {
                LogConsumerThread thread = new LogConsumerThread(reader, serializer, config);
                thread.start();
                threads.add(thread);
            }
            return threads;
        }

        /**
         * Outstanding work (pending + running) this worker can actually consume.
         * Scoped to the configured {@code queues} so backlog on queues this worker
         * doesn't serve can't drive it to {@code maxWorkers}; unscoped otherwise.
         */
        private long currentDepth() {
            if (queues == null || queues.isEmpty()) {
                return depthFrom(backend.statsJson());
            }
            long total = 0;
            for (String queue : queues) {
                total += depthFrom(backend.statsByQueueJson(queue));
            }
            return total;
        }

        /**
         * Depth from one stats blob. Propagates on failure rather than reporting
         * {@code 0}: the autoscaler swallows a tick error and retries, whereas a
         * false-empty reading would shrink the pool while backlog still exists.
         */
        private static long depthFrom(String statsJson) {
            JsonNode stats;
            try {
                stats = JSON.readTree(statsJson);
            } catch (JacksonException e) {
                throw new IllegalStateException("failed to parse worker queue stats", e);
            }
            return numericField(stats, "pending") + numericField(stats, "running");
        }

        /**
         * Read an integral stats field, rejecting a missing or non-numeric value
         * instead of coercing it to {@code 0} (which would read as an empty queue
         * and shrink the pool). The autoscaler swallows the resulting error and
         * retries on the next tick.
         */
        private static long numericField(JsonNode stats, String name) {
            JsonNode node = stats.get(name);
            if (node == null || !node.canConvertToLong()) {
                throw new IllegalStateException("worker queue stats field '" + name + "' is missing or non-numeric");
            }
            return node.asLong();
        }

        private String encodeOptions() {
            Map<String, Object> options = new LinkedHashMap<>();
            if (queues != null) {
                options.put("queues", queues);
            }
            if (channelCapacity != null) {
                options.put("channelCapacity", channelCapacity);
            }
            if (batchSize != null) {
                options.put("batchSize", batchSize);
            }
            // Cap in-flight dispatch by the pool's execution ceiling: the
            // autoscaler's max when autoscaling, otherwise a fixed pool's size.
            // A plain cached pool (concurrency == 0, no autoscale) stays unbounded.
            if (autoscale != null) {
                options.put("concurrency", autoscale.maxWorkers());
            } else if (concurrency > 0) {
                options.put("concurrency", concurrency);
            }
            if (!taskPolicies.isEmpty()) {
                options.put("taskConfigs", encodeTaskConfigs());
            }
            if (mesh != null) {
                options.put("meshConfig", mesh.toConfigJson());
            }
            if (!subscriptions.isEmpty()) {
                options.put("subscriptions", encodeSubscriptions());
            }
            // Resolve at start() so config set after the builder was obtained is seen.
            List<Map<String, Object>> resolvedQueueConfigs = queueConfigs.get();
            if (!resolvedQueueConfigs.isEmpty()) {
                options.put("queueConfigs", resolvedQueueConfigs);
            }
            // Presence, not emptiness: an empty Retention encodes as `{}` and
            // disables retention, which the core distinguishes from an omitted
            // option (recommended defaults). Do NOT change to a non-empty check.
            if (retention != null) {
                options.put("retention", retention.toMap());
            }
            // Only when asked: the binding's default is polling, so omitting the
            // key keeps the wire shape unchanged for every worker that never
            // touches this knob.
            if (pushDispatch) {
                options.put("pushDispatch", true);
            }
            try {
                return JSON.writeValueAsString(options);
            } catch (Exception e) {
                throw new SerializationException("failed to encode worker options", e);
            }
        }

        /**
         * Serialize the declared topic subscriptions into the wire shape the binding reads.
         * The subscriber task's delivery settings are emitted only when set, so an unset
         * one is persisted as "take the queue default" rather than a spurious zero.
         */
        private List<Map<String, Object>> encodeSubscriptions() {
            List<Map<String, Object>> specs = new ArrayList<>(subscriptions.size());
            for (SubscriptionConfig config : subscriptions) {
                Map<String, Object> spec = new LinkedHashMap<>();
                spec.put("topic", config.topic());
                spec.put("subscriptionName", config.name());
                spec.put("taskName", config.taskName());
                spec.put("queue", config.queue());
                spec.put("durable", config.durable());
                if (config.taskPriority() != null) {
                    spec.put("priority", config.taskPriority());
                }
                if (config.taskMaxRetries() != null) {
                    spec.put("maxRetries", config.taskMaxRetries());
                }
                if (config.taskTimeoutMs() != null) {
                    spec.put("timeoutMs", config.taskTimeoutMs());
                }
                specs.add(spec);
            }
            return specs;
        }

        /**
         * Serialize each task's per-task config (retry curve and/or circuit breaker) into the
         * wire shape the binding reads — one entry per task, merging both sources by name.
         */
        private List<Map<String, Object>> encodeTaskConfigs() {
            List<Map<String, Object>> configs = new ArrayList<>(taskPolicies.size());
            for (Task<?> task : taskPolicies.values()) {
                Map<String, Object> config = new LinkedHashMap<>();
                config.put("name", task.name());
                encodeRetryPolicy(task.retryPolicy(), config);
                encodeCircuitBreaker(task.circuitBreaker(), config);
                if (task.rateLimit() != null) {
                    config.put("rateLimit", task.rateLimit());
                }
                if (task.retryBudget() != null) {
                    config.put("retryBudget", task.retryBudget());
                }
                if (task.maxConcurrent() != null) {
                    config.put("maxConcurrent", task.maxConcurrent());
                }
                if (task.maxInFlightPerTask() != null) {
                    config.put("maxInFlightPerTask", task.maxInFlightPerTask());
                }
                configs.add(config);
            }
            return configs;
        }

        private static void encodeRetryPolicy(@Nullable RetryPolicy policy, Map<String, Object> config) {
            if (policy == null) {
                return;
            }
            if (policy.baseDelay() != null) {
                config.put("baseDelayMs", policy.baseDelay().toMillis());
            }
            if (policy.maxDelay() != null) {
                config.put("maxDelayMs", policy.maxDelay().toMillis());
            }
            if (!policy.customDelays().isEmpty()) {
                List<Long> delaysMs = new ArrayList<>(policy.customDelays().size());
                policy.customDelays().forEach(delay -> delaysMs.add(delay.toMillis()));
                config.put("customDelaysMs", delaysMs);
            }
        }

        private static void encodeCircuitBreaker(@Nullable CircuitBreakerConfig breaker, Map<String, Object> config) {
            if (breaker == null) {
                return;
            }
            config.put("circuitBreakerThreshold", breaker.threshold());
            config.put("circuitBreakerWindowMs", breaker.window().toMillis());
            config.put("circuitBreakerCooldownMs", breaker.cooldown().toMillis());
            config.put("circuitBreakerHalfOpenProbes", breaker.halfOpenProbes());
            config.put("circuitBreakerHalfOpenSuccessRate", breaker.halfOpenSuccessRate());
        }

        @SuppressWarnings("unchecked")
        private static <T, R> TaskFunction<Object, Object> cast(TaskFunction<T, R> handler) {
            return (TaskFunction<Object, Object>) handler;
        }
    }
}
