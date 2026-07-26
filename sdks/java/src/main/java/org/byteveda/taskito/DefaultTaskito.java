package org.byteveda.taskito;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import org.byteveda.taskito.core.CoreFacade;
import org.byteveda.taskito.errors.EnqueueSkippedException;
import org.byteveda.taskito.errors.InterceptionException;
import org.byteveda.taskito.errors.PredicateRejectedException;
import org.byteveda.taskito.errors.QueueFullException;
import org.byteveda.taskito.errors.SerializationException;
import org.byteveda.taskito.errors.WorkflowException;
import org.byteveda.taskito.events.Emitter;
import org.byteveda.taskito.events.EnqueuedEvent;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.events.PredicateEvent;
import org.byteveda.taskito.events.QueueEvent;
import org.byteveda.taskito.events.TaskitoEvent;
import org.byteveda.taskito.events.WorkflowEvent;
import org.byteveda.taskito.interception.Interception;
import org.byteveda.taskito.interception.InterceptionAnalysis;
import org.byteveda.taskito.interception.Interceptor;
import org.byteveda.taskito.internal.IdempotencyKeys;
import org.byteveda.taskito.internal.MiddlewareDisables;
import org.byteveda.taskito.locks.Lock;
import org.byteveda.taskito.locks.LockInfo;
import org.byteveda.taskito.middleware.EnqueueContext;
import org.byteveda.taskito.middleware.Middleware;
import org.byteveda.taskito.model.CircuitBreakerState;
import org.byteveda.taskito.model.DeadJob;
import org.byteveda.taskito.model.DispatchOrder;
import org.byteveda.taskito.model.EffectiveRetention;
import org.byteveda.taskito.model.Job;
import org.byteveda.taskito.model.JobDag;
import org.byteveda.taskito.model.JobError;
import org.byteveda.taskito.model.JobFilter;
import org.byteveda.taskito.model.Page;
import org.byteveda.taskito.model.PeriodicInfo;
import org.byteveda.taskito.model.QueueStats;
import org.byteveda.taskito.model.ReplayEntry;
import org.byteveda.taskito.model.RetentionPreview;
import org.byteveda.taskito.model.Subscription;
import org.byteveda.taskito.model.TaskLog;
import org.byteveda.taskito.model.TaskLogLevel;
import org.byteveda.taskito.model.TaskMetric;
import org.byteveda.taskito.model.Topic;
import org.byteveda.taskito.model.TopicLogStat;
import org.byteveda.taskito.model.TopicMessage;
import org.byteveda.taskito.model.TopicStat;
import org.byteveda.taskito.model.WorkerInfo;
import org.byteveda.taskito.model.WorkflowRunInfo;
import org.byteveda.taskito.predicates.EnqueueDecision;
import org.byteveda.taskito.predicates.EnqueueGate;
import org.byteveda.taskito.predicates.Predicate;
import org.byteveda.taskito.predicates.PredicateContext;
import org.byteveda.taskito.predicates.PredicateMetrics;
import org.byteveda.taskito.predicates.PredicateStats;
import org.byteveda.taskito.pubsub.LogConsumerConfig;
import org.byteveda.taskito.pubsub.LogConsumerOptions;
import org.byteveda.taskito.pubsub.PublishOptions;
import org.byteveda.taskito.pubsub.SubscriptionConfig;
import org.byteveda.taskito.pubsub.SubscriptionOptions;
import org.byteveda.taskito.resources.PoolConfig;
import org.byteveda.taskito.resources.ResourceContext;
import org.byteveda.taskito.resources.ResourceDefinition;
import org.byteveda.taskito.resources.ResourceRuntime;
import org.byteveda.taskito.resources.ResourceScope;
import org.byteveda.taskito.resources.ResourceStat;
import org.byteveda.taskito.scheduling.PeriodicTask;
import org.byteveda.taskito.serialization.PayloadCodec;
import org.byteveda.taskito.serialization.Serializer;
import org.byteveda.taskito.spi.QueueBackend;
import org.byteveda.taskito.task.EnqueueOptions;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.LogTopicReader;
import org.byteveda.taskito.worker.Retention;
import org.byteveda.taskito.worker.Worker;
import org.byteveda.taskito.worker.WorkerLifecycle;
import org.byteveda.taskito.workflows.GateConfig;
import org.byteveda.taskito.workflows.Step;
import org.byteveda.taskito.workflows.Workflow;
import org.byteveda.taskito.workflows.WorkflowRun;
import org.byteveda.taskito.workflows.WorkflowStatus;

/**
 * Default {@link Taskito}: maps the typed public API onto a {@link QueueBackend},
 * serializing payloads with the configured {@link Serializer} and decoding
 * native JSON views with a private mapper.
 */
final class DefaultTaskito implements Taskito, LogTopicReader {
    private static final ObjectMapper VIEWS = new ObjectMapper();

    private static final long DEFAULT_LOCK_TTL_MS = 30_000;

    private final QueueBackend backend;
    private final CoreFacade facade;
    private final Serializer serializer;
    private final Map<String, PayloadCodec> codecs;
    private final List<Middleware> middleware = new CopyOnWriteArrayList<>();
    private final ResourceRuntime resources = new ResourceRuntime();
    private final Map<String, List<EnqueueGate>> gates = new ConcurrentHashMap<>();
    private final PredicateMetrics predicateMetrics = new PredicateMetrics();
    private final List<Interceptor> interceptors = new CopyOnWriteArrayList<>();
    private final List<SubscriptionConfig> subscriptions = new CopyOnWriteArrayList<>();
    // Managed log consumers registered before start; worker spawns one poll loop each.
    private final List<LogConsumerConfig> logConsumers = new CopyOnWriteArrayList<>();
    // Opt-in per-queue admission caps (queue -> max pending). Absent = uncapped.
    private final Map<String, Integer> maxPending = new ConcurrentHashMap<>();
    // Opt-in per-queue CoDel config (queue -> [targetMs, intervalMs]).
    private final Map<String, long[]> codelConfigs = new ConcurrentHashMap<>();
    // Opt-in per-queue dispatch order (queue -> "lifo"). Absent = "fifo".
    private final Map<String, String> dispatchOrders = new ConcurrentHashMap<>();
    // Queue-level event hub: local emissions land here, and every worker built
    // via worker() forwards its events here through its emitter's parent link.
    private final Emitter events = new Emitter();
    /** Workers started from this client and not yet closed — the shutdown set. */
    private final Set<Worker> liveWorkers = ConcurrentHashMap.newKeySet();

    DefaultTaskito(QueueBackend backend, Serializer serializer, Map<String, PayloadCodec> codecs) {
        this.backend = backend;
        this.facade = new CoreFacade(backend);
        this.serializer = serializer;
        this.codecs = codecs;
    }

    @Override
    public Queue queue(String name) {
        return new NamedQueue(this, name);
    }

    @Override
    public Taskito use(Middleware middleware) {
        this.middleware.add(middleware);
        return this;
    }

    @Override
    public Taskito onEvent(EventName name, Consumer<TaskitoEvent> listener) {
        events.onEvent(name, listener);
        return this;
    }

    // ── Resources ────────────────────────────────────────────────────

    @Override
    public <T> Taskito resource(String name, Function<ResourceContext, T> factory) {
        return resource(name, ResourceScope.WORKER, factory);
    }

    @Override
    public <T> Taskito resource(String name, ResourceScope scope, Function<ResourceContext, T> factory) {
        resources.register(name, new ResourceDefinition(factory::apply, scope, null));
        return this;
    }

    @Override
    public <T> Taskito resource(
            String name, ResourceScope scope, Function<ResourceContext, T> factory, Consumer<T> dispose) {
        resources.register(name, new ResourceDefinition(factory::apply, scope, value -> dispose.accept(cast(value))));
        return this;
    }

    @Override
    public <T> Taskito resource(
            String name, PoolConfig pool, Function<ResourceContext, T> factory, Consumer<T> dispose) {
        resources.register(
                name,
                new ResourceDefinition(
                        factory::apply, ResourceScope.POOLED, value -> dispose.accept(cast(value)), pool));
        return this;
    }

    @Override
    public Taskito resource(String name, ResourceDefinition definition) {
        resources.register(name, definition);
        return this;
    }

    @Override
    public Map<String, ResourceStat> resourceMetrics() {
        return resources.metrics();
    }

    @Override
    public Map<String, Boolean> reloadResources() {
        return resources.reload(null);
    }

    @Override
    public Map<String, Boolean> reloadResources(Collection<String> names) {
        return resources.reload(names);
    }

    @Override
    public Taskito maxPending(String queue, int cap) {
        if (cap < 0) {
            // A negative cap makes `pending + incoming > cap` always true and
            // would permanently reject every enqueue for the queue.
            throw new IllegalArgumentException("cap must be non-negative");
        }
        maxPending.put(queue, cap);
        return this;
    }

    /**
     * Enforce the opt-in {@code maxPending} admission cap for a queue. Throws
     * {@link QueueFullException} when admitting {@code incoming} jobs would push
     * the queue's pending backlog past its cap; {@code incoming} is the batch
     * size, so a batch is rejected as a whole rather than overshooting the cap by
     * its full size. A no-op (and no query) for uncapped queues. Non-atomic
     * count-then-insert, like the rate limiter.
     */
    private void rejectIfQueueFull(String queueOrNull, int incoming) {
        String queue = queueOrDefault(queueOrNull);
        Integer cap = maxPending.get(queue);
        if (cap == null) {
            return;
        }
        long pending = backend.countPendingByQueue(queue);
        if (pending + incoming > cap) {
            throw new QueueFullException(queue, pending, cap);
        }
    }

    @Override
    public Taskito predicate(String taskName, Predicate predicate) {
        return gate(
                taskName,
                context -> predicate.test(context)
                        ? EnqueueDecision.allow()
                        : EnqueueDecision.reject("enqueue of task '" + taskName + "' rejected by a predicate"));
    }

    @Override
    public Taskito gate(String taskName, EnqueueGate gate) {
        gates.computeIfAbsent(taskName, key -> new CopyOnWriteArrayList<>()).add(gate);
        return this;
    }

    @Override
    public Taskito codel(String queue, long targetMs, long intervalMs) {
        if (targetMs <= 0 || intervalMs <= 0) {
            throw new IllegalArgumentException("targetMs and intervalMs must be positive");
        }
        codelConfigs.put(queue, new long[] {targetMs, intervalMs});
        return this;
    }

    @Override
    public Taskito dispatchOrder(String queue, DispatchOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("order must not be null");
        }
        dispatchOrders.put(queue, order.wire());
        return this;
    }

    @Override
    @Deprecated
    public Taskito dispatchOrder(String queue, String order) {
        // Keeps throwing IllegalArgumentException rather than the model enum's
        // SerializationException: callers of this overload predate the enum.
        if (!"fifo".equals(order) && !"lifo".equals(order)) {
            throw new IllegalArgumentException("order must be 'fifo' or 'lifo'");
        }
        return dispatchOrder(queue, DispatchOrder.fromWire(order));
    }

    /**
     * Serialize the per-queue scheduler config (CoDel + dispatch order) into the
     * worker-options wire shape, one spec per queue that has either set.
     */
    private List<Map<String, Object>> encodeQueueConfigs() {
        Map<String, Map<String, Object>> byQueue = new LinkedHashMap<>();
        codelConfigs.forEach((queue, codel) -> {
            Map<String, Object> spec = byQueue.computeIfAbsent(queue, DefaultTaskito::queueSpec);
            spec.put("codelTargetMs", codel[0]);
            spec.put("codelIntervalMs", codel[1]);
        });
        dispatchOrders.forEach((queue, order) -> {
            Map<String, Object> spec = byQueue.computeIfAbsent(queue, DefaultTaskito::queueSpec);
            spec.put("dispatchOrder", order);
        });
        return new ArrayList<>(byQueue.values());
    }

    /** A fresh wire spec carrying just the queue name; callers add their keys. */
    private static Map<String, Object> queueSpec(String queue) {
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("name", queue);
        return spec;
    }

    @Override
    public Taskito intercept(Interceptor interceptor) {
        interceptors.add(interceptor);
        return this;
    }

    @SuppressWarnings("unchecked")
    private static <T> T cast(Object value) {
        return (T) value;
    }

    // ── Producer ────────────────────────────────────────────────────

    @Override
    public <T> String enqueue(Task<T> task, T payload) {
        return enqueue(task, payload, task.options());
    }

    @Override
    public <T> String enqueue(Task<T> task, T payload, EnqueueOptions options) {
        return required(
                dispatchEnqueue(task.name(), payload, options, task.codecNames(), task.idempotent()), task.name());
    }

    @Override
    public String enqueue(String taskName, Object payload) {
        return required(dispatchEnqueue(taskName, payload, EnqueueOptions.none(), List.of(), false), taskName);
    }

    @Override
    public <T> Optional<String> tryEnqueue(Task<T> task, T payload) {
        return tryEnqueue(task, payload, task.options());
    }

    @Override
    public <T> Optional<String> tryEnqueue(Task<T> task, T payload, EnqueueOptions options) {
        return dispatchEnqueue(task.name(), payload, options, task.codecNames(), task.idempotent());
    }

    @Override
    public Optional<String> tryEnqueue(String taskName, Object payload) {
        return dispatchEnqueue(taskName, payload, EnqueueOptions.none(), List.of(), false);
    }

    /** Unwrap a dispatch result, turning a gate {@code Skip} into an {@link EnqueueSkippedException}. */
    private static String required(Optional<String> jobId, String taskName) {
        return jobId.orElseThrow(() ->
                new EnqueueSkippedException("enqueue of '" + taskName + "' was skipped by a gate; use tryEnqueue"));
    }

    /**
     * Interceptors → onEnqueue middleware → enqueue gates, then serialize,
     * codec-encode, and submit. Returns the job id, or empty when a gate skips
     * the enqueue; throws when a gate rejects it.
     */
    private Optional<String> dispatchEnqueue(
            String taskName,
            Object payload,
            EnqueueOptions options,
            List<String> codecNames,
            boolean taskIdempotentDefault) {
        String originalTaskName = taskName;
        Intercepted intercepted = applyInterceptors(taskName, payload, null);
        taskName = intercepted.taskName();
        payload = intercepted.payload();
        // A redirect to a different task invalidates the source task's codec chain:
        // codecNames is the ORIGINAL task's, and the target's codecs can't be resolved
        // from a bare name — encoding with the wrong chain would corrupt the payload or
        // break the worker's decode. Reject (batch enqueue rejects Redirect wholesale).
        if (!codecNames.isEmpty() && !taskName.equals(originalTaskName)) {
            throw new InterceptionException("interceptor Redirect is not supported for a task with payload codecs ('"
                    + originalTaskName + "')");
        }
        EnqueueContext context = new EnqueueContext(taskName, payload, options);
        for (Middleware m : middleware) {
            m.onEnqueue(context);
        }
        // Gate the payload that will actually be enqueued (after interceptors and
        // middleware may have rewritten it); the first non-Allow decision wins.
        EnqueueDecision decision = evaluate(taskName, context.payload());
        if (decision instanceof EnqueueDecision.Reject reject) {
            events.emit(new PredicateEvent(taskName, reject.reason()));
            throw new PredicateRejectedException("enqueue of '" + taskName + "' rejected: " + reject.reason());
        }
        if (decision instanceof EnqueueDecision.Skip) {
            return Optional.empty();
        }
        EnqueueOptions finalOptions = context.options();
        if (decision instanceof EnqueueDecision.Defer defer) {
            finalOptions = withDelay(finalOptions, defer.delay());
        }
        if (!context.metadata().isEmpty()) {
            finalOptions = finalOptions.toBuilder()
                    .metadata(encode(context.metadata()))
                    .build();
        }
        // Admission cap: reject before serializing/inserting if the target queue is full.
        rejectIfQueueFull(finalOptions.queue(), 1);
        // Serialize before codec-encoding so the idempotency key hashes the deterministic
        // pre-codec payload — a non-deterministic codec (e.g. an AES-GCM nonce) must not
        // change the dedup key.
        byte[] payloadBytes = serializer.serializeCall(context.payload());
        String uniqueKey = resolveUniqueKey(taskName, payloadBytes, finalOptions, taskIdempotentDefault);
        if (uniqueKey != null && !uniqueKey.equals(finalOptions.uniqueKey())) {
            finalOptions = finalOptions.toBuilder().uniqueKey(uniqueKey).build();
        }
        byte[] data = encodeCodecs(payloadBytes, codecNames);
        String jobId = backend.enqueue(taskName, data, encode(finalOptions));
        events.emit(new EnqueuedEvent(jobId, taskName, queueOrDefault(finalOptions.queue())));
        return Optional.of(jobId);
    }

    /**
     * Resolve the effective dedup key: an explicit {@code uniqueKey} wins, then an explicit
     * {@code idempotencyKey}; a per-enqueue {@code idempotent(false)} opts out; otherwise a
     * per-enqueue {@code idempotent(true)} or the task-level default auto-derives one from the
     * payload. Returns {@code null} when the enqueue should not be deduped.
     */
    private static String resolveUniqueKey(
            String taskName, byte[] preCodecPayload, EnqueueOptions options, boolean taskIdempotentDefault) {
        if (options.uniqueKey() != null) {
            return options.uniqueKey();
        }
        if (options.idempotencyKey() != null) {
            return options.idempotencyKey();
        }
        Boolean idempotent = options.idempotent();
        if (Boolean.FALSE.equals(idempotent)) {
            return null;
        }
        boolean enabled = Boolean.TRUE.equals(idempotent) || taskIdempotentDefault;
        return enabled ? IdempotencyKeys.autoKey(taskName, preCodecPayload) : null;
    }

    /**
     * Apply a defer delay to the options, overriding any delay already set —
     * {@code Defer(Duration.ZERO)} (e.g. {@code deferUntil} of a past instant)
     * means "enqueue now", not "keep the task's baked-in delay".
     */
    private static EnqueueOptions withDelay(EnqueueOptions options, Duration delay) {
        long delayMs = Math.max(delay.toMillis(), 0);
        return options.toBuilder().delayMs(delayMs).build();
    }

    /** Apply a task's payload codecs in order; throws if a name is not registered. */
    private byte[] encodeCodecs(byte[] data, List<String> codecNames) {
        byte[] bytes = data;
        for (String name : codecNames) {
            PayloadCodec codec = codecs.get(name);
            if (codec == null) {
                throw new IllegalStateException("no codec registered named '" + name + "'");
            }
            bytes = codec.encode(bytes);
        }
        return bytes;
    }

    /** Task and payload as the interceptor chain left them. */
    private record Intercepted(String taskName, Object payload) {}

    /**
     * Run the interceptor chain in registration order, applying each outcome to the
     * task/payload carried forward. {@code outcomes}, when non-null, collects what
     * every interceptor returned so a dry run can report the chain without repeating
     * it — {@link #analyzeArguments} is then the same code path as a real enqueue.
     */
    private Intercepted applyInterceptors(String taskName, Object payload, List<Interception> outcomes) {
        for (Interceptor interceptor : interceptors) {
            Interception outcome = interceptor.intercept(taskName, payload);
            if (outcome == null) {
                throw new InterceptionException("interceptor returned null for task '" + taskName + "'");
            }
            if (outcomes != null) {
                outcomes.add(outcome);
            }
            if (outcome instanceof Interception.Reject reject) {
                throw new InterceptionException("enqueue of '" + taskName + "' rejected: " + reject.reason());
            } else if (outcome instanceof Interception.Redirect redirect) {
                taskName = redirect.taskName();
                payload = redirect.payload();
            } else if (outcome instanceof Interception.Convert convert) {
                payload = convert.payload();
            }
            // Pass: leave taskName/payload unchanged.
        }
        return new Intercepted(taskName, payload);
    }

    @Override
    public InterceptionAnalysis analyzeArguments(String taskName, Object payload) {
        List<Interception> outcomes = new ArrayList<>();
        try {
            Intercepted applied = applyInterceptors(taskName, payload, outcomes);
            return new InterceptionAnalysis(applied.taskName(), applied.payload(), outcomes, false, null);
        } catch (InterceptionException e) {
            // The chain stopped part-way, so report the original input alongside
            // the outcomes it did produce rather than a half-rewritten payload.
            return new InterceptionAnalysis(taskName, payload, outcomes, true, e.getMessage());
        }
    }

    @Override
    public PredicateStats predicateStats() {
        return predicateMetrics.snapshot();
    }

    /**
     * Evaluate registered gates in order; the first non-{@code Allow} decision wins.
     * Only gated tasks touch the counters — an ungated enqueue is not a decision.
     */
    private EnqueueDecision evaluate(String taskName, Object payload) {
        List<EnqueueGate> taskGates = gates.get(taskName);
        if (taskGates == null || taskGates.isEmpty()) {
            return EnqueueDecision.allow();
        }
        PredicateContext context = new PredicateContext(taskName, payload);
        EnqueueDecision outcome = EnqueueDecision.allow();
        try {
            for (EnqueueGate gate : taskGates) {
                EnqueueDecision decision = gate.decide(context);
                if (!(decision instanceof EnqueueDecision.Allow)) {
                    outcome = decision;
                    break;
                }
            }
        } catch (RuntimeException e) {
            // A throwing gate is its own outcome: count it, then let the enqueue fail.
            predicateMetrics.recordError();
            throw e;
        }
        predicateMetrics.record(outcome);
        return outcome;
    }

    @Override
    public <T> List<String> enqueueMany(Task<T> task, List<T> payloads) {
        return enqueueMany(task, payloads, task.options());
    }

    @Override
    public <T> List<String> enqueueMany(Task<T> task, List<T> payloads, EnqueueOptions options) {
        // Run each payload through interceptors then gates (a single rejection fails
        // the whole batch), so batch enqueue can't bypass the interception contract.
        // Nothing is submitted until the single enqueueMany call, so it stays all-or-nothing.
        byte[][] bytes = new byte[payloads.size()][];
        List<EnqueueOptions> perJob = new ArrayList<>(payloads.size());
        for (int i = 0; i < payloads.size(); i++) {
            Object payload = interceptBatchPayload(task.name(), payloads.get(i));
            EnqueueDecision decision = evaluate(task.name(), payload);
            if (decision instanceof EnqueueDecision.Reject reject) {
                events.emit(new PredicateEvent(task.name(), reject.reason()));
                throw new PredicateRejectedException("enqueue of '" + task.name() + "' rejected: " + reject.reason());
            }
            if (!(decision instanceof EnqueueDecision.Allow)) {
                // Skip/Defer can't be expressed per-item in a single all-or-nothing batch.
                throw new EnqueueSkippedException("enqueue of '" + task.name()
                        + "' was skipped or deferred by a gate; batch enqueue needs Allow");
            }
            // Resolve the dedup key per payload so bulk enqueue honors idempotent
            // auto-derive, mirroring the single-enqueue path (explicit
            // uniqueKey/idempotencyKey precedence preserved). Key hashes the
            // deterministic pre-codec bytes.
            byte[] payloadBytes = serializer.serializeCall(payload);
            EnqueueOptions jobOptions = options;
            String uniqueKey = resolveUniqueKey(task.name(), payloadBytes, jobOptions, task.idempotent());
            if (uniqueKey != null && !uniqueKey.equals(jobOptions.uniqueKey())) {
                jobOptions = jobOptions.toBuilder().uniqueKey(uniqueKey).build();
            }
            bytes[i] = encodeCodecs(payloadBytes, task.codecNames());
            perJob.add(jobOptions);
        }
        // The whole batch targets one queue (a single `options`); reject before
        // inserting if admitting all of it would push that queue past its cap.
        rejectIfQueueFull(options.queue(), payloads.size());
        List<String> jobIds = Arrays.asList(backend.enqueueMany(task.name(), bytes, encode(perJob)));
        String queue = queueOrDefault(options.queue());
        for (String jobId : jobIds) {
            events.emit(new EnqueuedEvent(jobId, task.name(), queue));
        }
        return jobIds;
    }

    /**
     * Apply interceptors to one batched payload: {@code Convert} rewrites it,
     * {@code Reject} fails the batch. {@code Redirect} is unsupported here — it would
     * move an item to a different task, breaking the single-task batch — so it throws.
     */
    private Object interceptBatchPayload(String taskName, Object payload) {
        for (Interceptor interceptor : interceptors) {
            Interception outcome = interceptor.intercept(taskName, payload);
            if (outcome == null) {
                throw new InterceptionException("interceptor returned null for task '" + taskName + "'");
            }
            if (outcome instanceof Interception.Reject reject) {
                throw new InterceptionException("enqueue of '" + taskName + "' rejected: " + reject.reason());
            } else if (outcome instanceof Interception.Convert convert) {
                payload = convert.payload();
            } else if (outcome instanceof Interception.Redirect) {
                throw new InterceptionException(
                        "interceptor Redirect is not supported for batch enqueue of task '" + taskName + "'");
            }
            // Pass: leave payload unchanged.
        }
        return payload;
    }

    @Override
    public <T> List<String> enqueueAll(Task<T> task, List<T> payloads) {
        return enqueueMany(task, payloads);
    }

    @Override
    public Optional<Job> getJob(String jobId) {
        return backend.getJobJson(jobId).map(json -> decode(json, Job.class));
    }

    @Override
    public Optional<Job> awaitJob(String jobId, Duration timeout) {
        return facade.awaitJobJson(jobId, timeout, Duration.ofMillis(100)).map(json -> decode(json, Job.class));
    }

    @Override
    public Optional<byte[]> getResult(String jobId) {
        return backend.getResult(jobId);
    }

    @Override
    public <R> Optional<R> getResult(String jobId, Class<R> type) {
        return backend.getResult(jobId).map(bytes -> serializer.deserialize(bytes, type));
    }

    @Override
    public boolean cancel(String jobId) {
        return backend.cancel(jobId);
    }

    @Override
    public boolean requestCancel(String jobId) {
        return backend.requestCancel(jobId);
    }

    @Override
    public boolean isCancelRequested(String jobId) {
        return backend.isCancelRequested(jobId);
    }

    @Override
    public void setProgress(String jobId, int progress) {
        backend.setProgress(jobId, progress);
    }

    // ── Inspection ──────────────────────────────────────────────────

    @Override
    public QueueStats stats() {
        return decode(backend.statsJson(), QueueStats.class);
    }

    @Override
    public QueueStats statsByQueue(String queue) {
        return decode(backend.statsByQueueJson(queue), QueueStats.class);
    }

    @Override
    public long countPendingByQueue(String queue) {
        return backend.countPendingByQueue(queue);
    }

    @Override
    public Map<String, QueueStats> statsAllQueues() {
        return decodeMap(backend.statsAllQueuesJson(), QueueStats.class);
    }

    @Override
    public List<Job> listJobs(JobFilter filter) {
        return decodeList(backend.listJobsJson(encode(filter)), Job.class);
    }

    @Override
    public Page<Job> listJobsAfter(JobFilter filter, String after) {
        return decodePage(backend.listJobsAfterJson(encode(filter), after), Job.class);
    }

    @Override
    public Page<Job> listArchivedAfter(long limit, String after) {
        return decodePage(backend.listArchivedAfterJson(limit, after), Job.class);
    }

    @Override
    public List<JobError> jobErrors(String jobId) {
        return decodeList(backend.jobErrorsJson(jobId), JobError.class);
    }

    @Override
    public List<TaskMetric> metrics(String taskName, long sinceMs) {
        return decodeList(backend.metricsJson(taskName, sinceMs), TaskMetric.class);
    }

    @Override
    public List<WorkerInfo> listWorkers() {
        return decodeList(backend.listWorkersJson(), WorkerInfo.class);
    }

    @Override
    public List<CircuitBreakerState> listCircuitBreakers() {
        return decodeList(backend.listCircuitBreakersJson(), CircuitBreakerState.class);
    }

    // ── Admin ───────────────────────────────────────────────────────

    @Override
    public List<DeadJob> listDead(long limit, long offset) {
        return decodeList(backend.listDeadJson(limit, offset), DeadJob.class);
    }

    @Override
    public List<DeadJob> listDeadByTask(String taskName, long limit, long offset) {
        return decodeList(backend.listDeadByTaskJson(taskName, limit, offset), DeadJob.class);
    }

    @Override
    public long purgeDeadByTask(String taskName) {
        return backend.purgeDeadByTask(taskName);
    }

    @Override
    public String retryDead(String deadId) {
        return backend.retryDead(deadId);
    }

    @Override
    public String retry(String deadId) {
        return backend.retryDead(deadId);
    }

    @Override
    public String replayJob(String jobId) {
        return backend.replayJob(jobId);
    }

    @Override
    public List<ReplayEntry> getReplayHistory(String jobId) {
        return decodeList(backend.getReplayHistoryJson(jobId), ReplayEntry.class);
    }

    @Override
    public JobDag jobDag(String jobId) {
        return decode(backend.jobDagJson(jobId), JobDag.class);
    }

    @Override
    public boolean deleteDead(String deadId) {
        return backend.deleteDead(deadId);
    }

    @Override
    public boolean requeueJob(String jobId) {
        return backend.requeueJob(jobId);
    }

    @Override
    public long purgeDead(long olderThanMs) {
        return backend.purgeDead(olderThanMs);
    }

    @Override
    public long purgeCompleted(long olderThanMs) {
        return backend.purgeCompleted(olderThanMs);
    }

    /** Pause one named queue; backs {@link NamedQueue#pause()}. */
    void pauseLane(String queue) {
        backend.pauseQueue(queue);
        events.emit(new QueueEvent(EventName.QUEUE_PAUSED, queue));
    }

    /** Resume one named queue; backs {@link NamedQueue#resume()}. */
    void resumeLane(String queue) {
        backend.resumeQueue(queue);
        events.emit(new QueueEvent(EventName.QUEUE_RESUMED, queue));
    }

    /** The effective queue name: an unset option targets the {@code default} queue. */
    private static String queueOrDefault(String queueOrNull) {
        return queueOrNull == null ? "default" : queueOrNull;
    }

    @Override
    public List<String> listPausedQueues() {
        return decodeList(backend.listPausedQueuesJson(), String.class);
    }

    @Override
    public Optional<String> getSetting(String key) {
        return backend.getSetting(key);
    }

    @Override
    public Optional<EffectiveRetention> effectiveRetention() {
        return backend.effectiveRetentionJson().map(json -> decode(json, EffectiveRetention.class));
    }

    @Override
    public RetentionPreview dryRunRetention() {
        return decode(backend.dryRunRetentionJson(null), RetentionPreview.class);
    }

    @Override
    public RetentionPreview dryRunRetention(Retention retention) {
        String spec = retention == null ? null : encode(retention.toMap());
        return decode(backend.dryRunRetentionJson(spec), RetentionPreview.class);
    }

    @Override
    public void setSetting(String key, String value) {
        backend.setSetting(key, value);
    }

    @Override
    public boolean deleteSetting(String key) {
        return backend.deleteSetting(key);
    }

    @Override
    public Map<String, String> listSettings() {
        return decodeMap(backend.listSettingsJson(), String.class);
    }

    // ── Logs ────────────────────────────────────────────────────────

    @Override
    public void writeTaskLog(String jobId, String taskName, TaskLogLevel level, String message) {
        writeTaskLog(jobId, taskName, level, message, null);
    }

    @Override
    public void writeTaskLog(String jobId, String taskName, TaskLogLevel level, String message, String extra) {
        if (level == null) {
            throw new IllegalArgumentException("level must not be null");
        }
        backend.writeTaskLog(jobId, taskName, level.wire(), message, extra);
    }

    @Override
    @Deprecated
    public void writeTaskLog(String jobId, String taskName, String level, String message) {
        writeTaskLog(jobId, taskName, TaskLogLevel.fromWire(level), message);
    }

    @Override
    @Deprecated
    public void writeTaskLog(String jobId, String taskName, String level, String message, String extra) {
        writeTaskLog(jobId, taskName, TaskLogLevel.fromWire(level), message, extra);
    }

    @Override
    public List<TaskLog> getTaskLogs(String jobId) {
        return decodeList(backend.getTaskLogsJson(jobId), TaskLog.class);
    }

    @Override
    public List<TaskLog> getTaskLogsAfter(String jobId, String afterId) {
        return decodeList(backend.getTaskLogsAfterJson(jobId, afterId), TaskLog.class);
    }

    @Override
    public List<TaskLog> queryTaskLogs(String taskName, String level, long sinceMs, long limit) {
        return decodeList(backend.queryTaskLogsJson(taskName, level, sinceMs, limit), TaskLog.class);
    }

    @Override
    public Lock lock(String name, long ttlMs) {
        return new Lock(backend, name, ttlMs);
    }

    @Override
    public Lock lock(String name) {
        return new Lock(backend, name, DEFAULT_LOCK_TTL_MS);
    }

    @Override
    public boolean withLock(String name, long ttlMs, Runnable body) {
        try (Lock lock = new Lock(backend, name, ttlMs)) {
            if (!lock.acquire()) {
                return false;
            }
            body.run();
            return true;
        }
    }

    @Override
    public Optional<LockInfo> lockInfo(String name) {
        return backend.lockInfoJson(name).map(json -> decode(json, LockInfo.class));
    }

    @Override
    public Optional<LockInfo> getLockInfo(String name) {
        return lockInfo(name);
    }

    @Override
    public long registerPeriodic(PeriodicTask task) {
        byte[] payload = task.payload == null ? null : serializer.serializeCall(task.payload);
        return backend.registerPeriodic(
                task.name, task.taskName, task.cron, payload, task.queue, task.timezone, task.enabled);
    }

    @Override
    public List<PeriodicInfo> listPeriodic() {
        return decodeList(backend.listPeriodicJson(), PeriodicInfo.class);
    }

    @Override
    public boolean deletePeriodic(String name) {
        return backend.deletePeriodic(name);
    }

    @Override
    public boolean pausePeriodic(String name) {
        return backend.setPeriodicEnabled(name, false);
    }

    @Override
    public boolean resumePeriodic(String name) {
        return backend.setPeriodicEnabled(name, true);
    }

    // ── Pub/Sub ─────────────────────────────────────────────────────

    @Override
    public <T> Taskito subscribe(String topic, Task<T> task) {
        return subscribe(topic, task, SubscriptionOptions.none());
    }

    @Override
    public <T> Taskito subscribe(String topic, Task<T> task, SubscriptionOptions options) {
        String name = options.name() != null ? options.name() : task.name();
        EnqueueOptions taskDefaults = task.options();
        // A durable subscription registers now so producer-only processes see it;
        // an ephemeral one waits for a worker start to bind to that worker's id.
        // Register before recording locally, so a failed registration leaves no
        // orphaned local declaration behind.
        if (options.durable()) {
            backend.registerSubscription(
                    topic,
                    name,
                    task.name(),
                    options.queue(),
                    true,
                    null,
                    taskDefaults.priority(),
                    taskDefaults.maxRetries(),
                    taskDefaults.timeoutMs());
        }
        // Re-declaring (topic, name) replaces the previous local entry, mirroring
        // the backend's upsert instead of accumulating duplicates.
        subscriptions.removeIf(
                existing -> existing.topic().equals(topic) && existing.name().equals(name));
        subscriptions.add(new SubscriptionConfig(
                topic,
                name,
                task.name(),
                options.queue(),
                options.durable(),
                taskDefaults.priority(),
                taskDefaults.maxRetries(),
                taskDefaults.timeoutMs()));
        return this;
    }

    @Override
    public List<Job> publish(String topic, Object payload) {
        return publish(topic, payload, PublishOptions.none());
    }

    @Override
    public List<Job> publish(String topic, Object payload, PublishOptions options) {
        // Deliveries use the queue-level serializer: there is one payload for all
        // subscribers, so per-task codec chains don't apply to topic tasks.
        byte[] payloadBytes = serializer.serializeCall(payload);
        return decodeList(backend.publishJson(topic, payloadBytes, encode(publishRequest(options))), Job.class);
    }

    /**
     * The wire shape of a publish: just the caller's own options. Per-subscriber
     * delivery defaults are persisted on each subscription row (see
     * {@link #subscribe}), so the core resolves them without the producer's help.
     */
    private static Map<String, Object> publishRequest(PublishOptions options) {
        Map<String, Object> request = new LinkedHashMap<>();
        putIfSet(request, "idempotencyKey", options.idempotencyKey());
        putIfSet(request, "metadata", options.metadata());
        putIfSet(request, "notes", options.notes());
        putIfSet(request, "priority", options.priority());
        putIfSet(request, "delayMs", options.delayMs());
        putIfSet(request, "maxRetries", options.maxRetries());
        putIfSet(request, "timeoutMs", options.timeoutMs());
        putIfSet(request, "expiresMs", options.expiresMs());
        putIfSet(request, "resultTtlMs", options.resultTtlMs());
        return request;
    }

    private static void putIfSet(Map<String, Object> request, String key, Object value) {
        if (value != null) {
            request.put(key, value);
        }
    }

    @Override
    public boolean unsubscribe(String topic, String name) {
        // Drop the local declarations too, or a later worker start would
        // re-register the subscription (or re-spawn a managed consumer polling a
        // deleted cursor) from the shared worker state.
        subscriptions.removeIf(
                config -> config.topic().equals(topic) && config.name().equals(name));
        logConsumers.removeIf(
                config -> config.topic().equals(topic) && config.name().equals(name));
        return backend.unsubscribe(topic, name);
    }

    @Override
    public boolean pauseSubscription(String topic, String name) {
        return backend.setSubscriptionActive(topic, name, false);
    }

    @Override
    public boolean resumeSubscription(String topic, String name) {
        return backend.setSubscriptionActive(topic, name, true);
    }

    @Override
    public List<Subscription> listSubscriptions() {
        return decodeList(backend.listSubscriptionsJson(null), Subscription.class);
    }

    @Override
    public List<Subscription> listSubscriptions(String topic) {
        return decodeList(backend.listSubscriptionsJson(topic), Subscription.class);
    }

    @Override
    public List<TopicStat> topicStats() {
        return decodeList(backend.topicBacklogStatsJson(), TopicStat.class);
    }

    @Override
    public List<TopicStat> topicStats(String topic) {
        // A null topic is "no filter", matching listSubscriptions(null).
        if (topic == null) {
            return topicStats();
        }
        List<TopicStat> filtered = new ArrayList<>();
        for (TopicStat stat : topicStats()) {
            if (stat.topic.equals(topic)) {
                filtered.add(stat);
            }
        }
        return filtered;
    }

    @Override
    public List<String> listTopics() {
        Set<String> topics = new LinkedHashSet<>();
        for (Subscription subscription : listSubscriptions()) {
            topics.add(subscription.topic);
        }
        return new ArrayList<>(topics);
    }

    @Override
    public void subscribeLog(String topic, String name) {
        // A log subscription has no handler: durable, no owner, no delivery
        // settings — just a named cursor over the topic (mode = "log").
        backend.registerSubscription(topic, name, "", "default", true, null, null, null, null, "log");
    }

    @Override
    public List<TopicMessage> readTopic(String topic, String name) {
        return readTopic(topic, name, 100);
    }

    @Override
    public List<TopicMessage> readTopic(String topic, String name, int limit) {
        return decodeTopicMessages(backend.readTopicMessagesJson(topic, name, limit));
    }

    @Override
    public boolean ackTopic(String topic, String name, String cursor) {
        return backend.ackTopicCursor(topic, name, cursor);
    }

    @Override
    public List<TopicMessage> leaseTopic(String topic, String name) {
        return leaseTopic(topic, name, 100, Duration.ofSeconds(30));
    }

    @Override
    public List<TopicMessage> leaseTopic(String topic, String name, int limit, Duration visibility) {
        return decodeTopicMessages(backend.leaseTopicMessagesJson(topic, name, limit, visibility.toMillis()));
    }

    @Override
    public boolean ackMessage(String topic, String name, String messageId) {
        return backend.ackMessage(topic, name, messageId);
    }

    @Override
    public boolean nackMessage(String topic, String name, String messageId) {
        return backend.nackMessage(topic, name, messageId);
    }

    @Override
    public List<TopicLogStat> topicLogStats() {
        return decodeList(backend.topicLogStatsJson(), TopicLogStat.class);
    }

    @Override
    public Taskito declareTopic(String name) {
        return declareTopic(name, null);
    }

    @Override
    public Taskito declareTopic(String name, Duration retention) {
        Long retentionMs = retention == null ? null : retention.toMillis();
        backend.declareTopic(name, retentionMs);
        return this;
    }

    @Override
    public List<Topic> listDeclaredTopics() {
        return decodeList(backend.listDeclaredTopicsJson(), Topic.class);
    }

    @Override
    public <T> Taskito logConsumer(String topic, String name, Class<T> payloadType, Consumer<T> handler) {
        return logConsumer(topic, name, payloadType, handler, LogConsumerOptions.none());
    }

    @Override
    public <T> Taskito logConsumer(
            String topic, String name, Class<T> payloadType, Consumer<T> handler, LogConsumerOptions options) {
        // Register the durable cursor now (like subscribeLog) so publishes are
        // retained even before a worker runs; the poll loop starts with the worker.
        subscribeLog(topic, name);
        LogConsumerConfig config = new LogConsumerConfig(
                topic,
                name,
                payloadType,
                castConsumer(handler),
                options.pollIntervalMs(),
                options.batchSize(),
                options.onError());
        // Re-declaring (topic, name) replaces the previous entry, so a duplicate
        // registration doesn't spawn a second poll loop for the same subscription.
        logConsumers.removeIf(
                existing -> existing.topic().equals(topic) && existing.name().equals(name));
        logConsumers.add(config);
        return this;
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<Object> castConsumer(Consumer<T> handler) {
        return (Consumer<Object>) handler;
    }

    /** Decode a native JSON array of message views into {@link TopicMessage}s (shared by read/lease). */
    private List<TopicMessage> decodeTopicMessages(String json) {
        List<TopicMessageRow> rows = decodeList(json, TopicMessageRow.class);
        List<TopicMessage> messages = new ArrayList<>(rows.size());
        for (TopicMessageRow row : rows) {
            messages.add(new TopicMessage(
                    row.id(), row.payload(), parseJsonMap(row.metadata()), parseJsonMap(row.notes()), row.createdAt()));
        }
        return messages;
    }

    /**
     * Native wire view of a log message: the payload crosses as base64 (Jackson
     * decodes it into {@code byte[]}); metadata/notes are JSON strings this SDK
     * parses back into maps.
     */
    private record TopicMessageRow(
            @JsonProperty("id") String id,
            @JsonProperty("payload") byte[] payload,
            @JsonProperty("metadata") String metadata,
            @JsonProperty("notes") String notes,
            @JsonProperty("createdAt") long createdAt) {}

    private static Map<String, Object> parseJsonMap(String json) {
        return json == null ? null : decodeMap(json, Object.class);
    }

    // ── Workflows ───────────────────────────────────────────────────

    @Override
    public WorkflowRun submitWorkflow(Workflow workflow) {
        return submitWorkflow(workflow, java.util.Collections.emptyMap());
    }

    @Override
    public WorkflowRun submitWorkflow(Workflow workflow, Map<String, Object> suppliedPayloads) {
        List<Step> steps = workflow.steps();
        rejectCachedFanProducers(steps);
        Set<String> deferred = deferredNodes(steps);
        List<Map<String, Object>> specs = new ArrayList<>(steps.size());
        // Deferred nodes (fan-out/fan-in + their downstream) have no job — and so
        // no payload — at submit; the tracker enqueues them at runtime. A static
        // node's payload is the one supplied at submit, else the one baked in.
        List<String> payloadNames = new ArrayList<>();
        List<byte[]> payloads = new ArrayList<>();
        for (Step step : steps) {
            specs.add(stepSpec(step));
            if (!deferred.contains(step.name)) {
                Object payload = suppliedPayloads.getOrDefault(step.name, step.payload);
                // A payloadless structural step (stepAfter) must get its payload at
                // submit; fail fast rather than enqueue a null-payload job.
                if (payload == null && !suppliedPayloads.containsKey(step.name)) {
                    throw new WorkflowException("workflow step '" + step.name
                            + "' has no payload; supply one via submitWorkflow(wf, payloads)");
                }
                payloadNames.add(step.name);
                payloads.add(serializer.serializeCall(payload));
            }
        }
        String runId = backend.submitWorkflow(
                workflow.name(),
                workflow.version(),
                encode(specs),
                payloadNames.toArray(new String[0]),
                payloads.toArray(new byte[0][]),
                null,
                null,
                deferred.toArray(new String[0]),
                null,
                null);
        events.emit(new WorkflowEvent(EventName.WORKFLOW_SUBMITTED, runId, workflow.name(), null));
        return new WorkflowRun(backend, VIEWS, runId, workflow.name());
    }

    /**
     * Fan-out/fan-in, gate, and conditional nodes — plus everything transitively
     * downstream of them — are deferred: they carry a node row but no job at
     * submit, and the worker tracker creates/parks/evaluates them at runtime.
     */
    /**
     * A cache hit produces no forward result, so a fan-out/fan-in over a cached step
     * would have nothing to expand or collect (the run would hang). Reject it at submit.
     */
    private static void rejectCachedFanProducers(List<Step> steps) {
        Set<String> cacheable = new HashSet<>();
        for (Step step : steps) {
            if (step.cacheTtlMs != null) {
                cacheable.add(step.name);
            }
        }
        if (cacheable.isEmpty()) {
            return;
        }
        for (Step step : steps) {
            if (step.fanOut == null && step.fanIn == null) {
                continue;
            }
            for (String predecessor : step.after) {
                if (cacheable.contains(predecessor)) {
                    throw new WorkflowException("step '" + step.name + "' fans over cached step '" + predecessor
                            + "', but a cache hit yields no result to fan — don't cache a fan-out/fan-in producer");
                }
            }
        }
    }

    private static Set<String> deferredNodes(List<Step> steps) {
        Set<String> deferred = new HashSet<>();
        for (Step step : steps) {
            // "on_success" is the default/static path — only runtime-evaluated
            // conditions (on_failure / always / callable) force a deferred node.
            boolean runtimeCondition = step.condition != null && !"on_success".equals(step.condition);
            if (step.fanOut != null
                    || step.fanIn != null
                    || step.gate != null
                    || step.subWorkflow != null
                    || step.cacheTtlMs != null
                    || runtimeCondition) {
                deferred.add(step.name);
            }
        }
        boolean changed = true;
        while (changed) {
            changed = false;
            for (Step step : steps) {
                if (!deferred.contains(step.name) && step.after.stream().anyMatch(deferred::contains)) {
                    deferred.add(step.name);
                    changed = true;
                }
            }
        }
        return deferred;
    }

    @Override
    public Optional<WorkflowStatus> workflowStatus(String runId) {
        return backend.getWorkflowStatusJson(runId).map(json -> decode(json, WorkflowStatus.class));
    }

    @Override
    public void cancelWorkflow(String runId) {
        backend.cancelWorkflowRun(runId);
        events.emit(new WorkflowEvent(EventName.WORKFLOW_CANCELLED, runId, null, null));
    }

    @Override
    public List<WorkflowRunInfo> listWorkflowRuns(String definitionName, String state, long limit, long offset) {
        return decodeList(backend.listWorkflowRunsJson(definitionName, state, limit, offset), WorkflowRunInfo.class);
    }

    @Override
    public Optional<WorkflowRunInfo> getWorkflowRun(String runId) {
        return backend.getWorkflowRunJson(runId).map(json -> decode(json, WorkflowRunInfo.class));
    }

    @Override
    public List<WorkflowRunInfo> getWorkflowChildren(String runId) {
        return decodeList(backend.getWorkflowChildrenJson(runId), WorkflowRunInfo.class);
    }

    @Override
    public Optional<String> getWorkflowDag(String runId) {
        return backend.getWorkflowDagJson(runId);
    }

    /** Encode a step's structure + per-step overrides for the native submit call. */
    private Map<String, Object> stepSpec(Step step) {
        Map<String, Object> spec = new LinkedHashMap<>();
        spec.put("name", step.name);
        spec.put("taskName", step.taskName);
        spec.put("after", step.after);
        if (step.queue != null) {
            spec.put("queue", step.queue);
        }
        if (step.maxRetries != null) {
            spec.put("maxRetries", step.maxRetries);
        }
        if (step.timeoutMs != null) {
            spec.put("timeoutMs", step.timeoutMs);
        }
        if (step.priority != null) {
            spec.put("priority", step.priority);
        }
        if (step.fanOut != null) {
            spec.put("fanOut", step.fanOut);
        }
        if (step.fanIn != null) {
            spec.put("fanIn", step.fanIn);
        }
        if (step.gate != null) {
            spec.put("gate", encodeGate(step.gate));
        }
        if (step.condition != null) {
            spec.put("condition", step.condition);
        }
        if (step.subWorkflow != null) {
            spec.put("subWorkflow", encodeChild(step.subWorkflow));
        }
        if (step.compensate != null) {
            spec.put("compensate", step.compensate);
        }
        if (step.cacheTtlMs != null) {
            spec.put("cache", encode(Map.of("ttlMs", step.cacheTtlMs)));
        }
        return spec;
    }

    /**
     * Encode a child workflow as a JSON string the worker tracker submits when the
     * sub-workflow node is reached: its name/version, the child's step specs, its
     * deferred set, and every child step's payload (Base64). Recurses through
     * {@link #stepSpec}, so a child may itself contain gates/conditions/sub-workflows.
     */
    private String encodeChild(Workflow child) {
        List<Step> steps = child.steps();
        Set<String> deferred = deferredNodes(steps);
        List<Map<String, Object>> specs = new ArrayList<>(steps.size());
        Map<String, String> payloads = new LinkedHashMap<>();
        for (Step step : steps) {
            // A child run has no submit-time payload map and no callable registry, so
            // any step that depends on either would lose state — reject it at build time.
            if ("callable".equals(step.condition)) {
                throw new WorkflowException("child workflow step '" + step.name
                        + "' uses condition(Condition); a sub-workflow cannot carry callable predicates");
            }
            boolean controlNode =
                    step.fanOut != null || step.fanIn != null || step.gate != null || step.subWorkflow != null;
            if (!controlNode && step.payload == null) {
                throw new WorkflowException("child workflow step '" + step.name
                        + "' has no payload; a sub-workflow cannot supply child step payloads at submit");
            }
            // The saga only compensates top-level runs; a child run resolves its parent
            // node without rolling back, so a child compensator would never fire.
            if (step.compensate != null) {
                throw new WorkflowException("child workflow step '" + step.name
                        + "' declares a compensator; sub-workflow runs cannot compensate yet");
            }
            specs.add(stepSpec(step));
            if (step.payload != null) {
                payloads.put(step.name, Base64.getEncoder().encodeToString(serializer.serializeCall(step.payload)));
            }
        }
        Map<String, Object> blob = new LinkedHashMap<>();
        blob.put("name", child.name());
        blob.put("version", child.version());
        blob.put("stepsJson", encode(specs));
        blob.put("deferred", new ArrayList<>(deferred));
        blob.put("payloads", payloads);
        return encode(blob);
    }

    /**
     * Encode a gate as a JSON string {@code {timeoutMs, onTimeout, message}}. The
     * core stores it opaquely on the step ({@code StepMetadata.gate}) and the
     * worker tracker parses it back when the gate is reached.
     */
    private static String encodeGate(GateConfig gate) {
        Map<String, Object> blob = new LinkedHashMap<>();
        if (gate.timeout() != null) {
            blob.put("timeoutMs", gate.timeout().toMillis());
        }
        blob.put("onTimeout", gate.onTimeout().wire());
        if (gate.message() != null) {
            blob.put("message", gate.message());
        }
        return encode(blob);
    }

    @Override
    public Worker.Builder worker() {
        // Each worker gets its own runtime (own WORKER-scoped cache) over shared definitions.
        // The live subscription list is shared so subscribe() calls made before
        // start() are registered under the started worker's id.
        return Worker.builder(backend, serializer, middleware, resources.forWorker(), codecs)
                .subscriptions(subscriptions)
                .logConsumers(logConsumers, this)
                .queueConfigs(this::encodeQueueConfigs)
                .lifecycle(new WorkerLifecycle() {
                    @Override
                    public void started(Worker worker) {
                        liveWorkers.add(worker);
                    }

                    @Override
                    public void closed(Worker worker) {
                        liveWorkers.remove(worker);
                    }
                })
                .eventHub(events);
    }

    @Override
    public void shutdown() {
        // Weakly consistent iteration: each close() removes its own worker from the
        // set as it goes, and closing an already-closed worker is a no-op.
        for (Worker worker : liveWorkers) {
            worker.close();
        }
    }

    @Override
    public void close() {
        backend.close();
    }

    // ── JSON helpers ────────────────────────────────────────────────

    private static String encode(Object value) {
        try {
            return VIEWS.writeValueAsString(value);
        } catch (Exception e) {
            throw new SerializationException("failed to encode request", e);
        }
    }

    private static <R> R decode(String json, Class<R> type) {
        try {
            return VIEWS.readValue(json, type);
        } catch (Exception e) {
            throw new SerializationException("failed to decode native response", e);
        }
    }

    @Override
    public void disableMiddleware(String taskName, String middlewareName) {
        List<String> disabled = new ArrayList<>(listDisabledMiddleware(taskName));
        if (!disabled.contains(middlewareName)) {
            disabled.add(middlewareName);
            backend.setSetting(MiddlewareDisables.key(taskName), encode(disabled));
        }
    }

    @Override
    public void enableMiddleware(String taskName, String middlewareName) {
        List<String> disabled = new ArrayList<>(listDisabledMiddleware(taskName));
        if (disabled.remove(middlewareName)) {
            backend.setSetting(MiddlewareDisables.key(taskName), encode(disabled));
        }
    }

    @Override
    public List<String> listDisabledMiddleware(String taskName) {
        return new MiddlewareDisables(backend).disabledFor(taskName);
    }

    private static <R> Page<R> decodePage(String json, Class<R> element) {
        JavaType type = VIEWS.getTypeFactory().constructParametricType(Page.class, element);
        try {
            return VIEWS.readValue(json, type);
        } catch (Exception e) {
            throw new SerializationException("failed to decode native response", e);
        }
    }

    private static <R> List<R> decodeList(String json, Class<R> element) {
        JavaType type = VIEWS.getTypeFactory().constructCollectionType(List.class, element);
        try {
            return VIEWS.readValue(json, type);
        } catch (Exception e) {
            throw new SerializationException("failed to decode native response", e);
        }
    }

    private static <V> Map<String, V> decodeMap(String json, Class<V> value) {
        JavaType type = VIEWS.getTypeFactory().constructMapType(Map.class, String.class, value);
        try {
            return VIEWS.readValue(json, type);
        } catch (Exception e) {
            throw new SerializationException("failed to decode native response", e);
        }
    }
}
