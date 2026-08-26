package org.byteveda.flexiq.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import org.byteveda.flexiq.JobContext;
import org.byteveda.flexiq.errors.TaskErrors;
import org.byteveda.flexiq.events.Emitter;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.OutcomeEvent;
import org.byteveda.flexiq.events.SleepEvent;
import org.byteveda.flexiq.internal.MiddlewareDisables;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.byteveda.flexiq.middleware.JobInfo;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;
import org.byteveda.flexiq.resources.ResourceRuntime;
import org.byteveda.flexiq.resources.Resources;
import org.byteveda.flexiq.resources.TaskScope;
import org.byteveda.flexiq.serialization.PayloadCodec;
import org.byteveda.flexiq.serialization.Serializer;
import org.byteveda.flexiq.spi.QueueBackend;
import org.byteveda.flexiq.spi.WorkerBridge;
import org.byteveda.flexiq.spi.WorkerControl;
import org.byteveda.flexiq.steps.StepContext;
import org.byteveda.flexiq.steps.StepLatch;
import org.byteveda.flexiq.steps.StepSleepSignal;
import org.jspecify.annotations.Nullable;

/**
 * Bridges native job dispatch to registered handlers. {@code onJob} hands work to
 * an executor, runs middleware around the handler, and completes via the
 * {@link WorkerControl}; {@code onOutcome} fans finished jobs out to middleware
 * and event listeners.
 */
final class WorkerDispatchBridge implements WorkerBridge {
    private static final FlexiQLogger LOG = FlexiQLogger.create("worker");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<Map<String, Object>>() {};

    /**
     * Middleware already warned about for pairing {@code before} with only
     * {@code after}. Bounded by the number of distinct middleware classes, which
     * is small and fixed at startup.
     */
    private static final Set<String> WARNED_UNPAIRED = ConcurrentHashMap.newKeySet();

    /**
     * Absent for an attached executor, which reads no storage: job metadata is
     * then unavailable to middleware, and the toggle list is empty.
     */
    private final @Nullable QueueBackend backend;

    private final Map<String, RegisteredTask> handlers;
    private final Serializer serializer;
    private final ExecutorService executor;
    private final Emitter emitter;
    private final List<Middleware> middleware;
    private final ResourceRuntime resources;
    private final Map<String, PayloadCodec> codecs;
    private final MiddlewareDisables disables;
    // Resolved once startWorker returns; job tasks await it before completing.
    private final CompletableFuture<WorkerControl> control = new CompletableFuture<>();

    WorkerDispatchBridge(
            @Nullable QueueBackend backend,
            Map<String, RegisteredTask> handlers,
            Serializer serializer,
            ExecutorService executor,
            Emitter emitter,
            List<Middleware> middleware,
            ResourceRuntime resources,
            Map<String, PayloadCodec> codecs) {
        this.backend = backend;
        this.handlers = handlers;
        this.serializer = serializer;
        this.executor = executor;
        this.emitter = emitter;
        this.middleware = middleware;
        this.resources = resources;
        this.codecs = codecs;
        this.disables = new MiddlewareDisables(backend);
    }

    void bind(WorkerControl control) {
        this.control.complete(control);
    }

    @Override
    public void onJob(
            long token,
            String jobId,
            String taskName,
            byte[] payload,
            @Nullable String metadataJson,
            @Nullable String disabledMiddlewareJson) {
        onJob(token, jobId, taskName, payload, metadataJson, disabledMiddlewareJson, 0);
    }

    @Override
    public void onJob(
            long token,
            String jobId,
            String taskName,
            byte[] payload,
            @Nullable String metadataJson,
            @Nullable String disabledMiddlewareJson,
            int attempt) {
        executor.execute(() -> runJob(token, jobId, taskName, payload, metadataJson, disabledMiddlewareJson, attempt));
    }

    private void runJob(
            long token,
            String jobId,
            String taskName,
            byte[] payload,
            @Nullable String metadataJson,
            @Nullable String disabledMiddlewareJson,
            int attempt) {
        WorkerControl bound = control.join();
        RegisteredTask task = handlers.get(taskName);
        if (task == null) {
            // Retryable: another worker in the fleet may well have it registered.
            bound.failJob(token, "no handler registered for task '" + taskName + "'", true);
            return;
        }
        // Off the dispatch when it came with one — an executor cannot read the
        // row — else lazily off the backend, which only a worker has.
        JobInfo job = metadataJson == null
                ? new JobInfo(jobId, taskName, () -> loadMetadata(jobId))
                : new JobInfo(jobId, taskName, () -> parseMetadata(metadataJson));
        TaskContext context = new TaskContext(jobId, taskName, job);
        // Bind a per-task resource scope around the handler; skip all wiring when
        // no resources are registered (zero overhead for the common case).
        TaskScope scope = resources.isEmpty() ? null : resources.createTaskScope();
        if (scope != null) {
            Resources.enter(scope);
        }
        // One latch per invocation, shared by the step context and the swallow
        // check below. The steps commit through `bound`, whose default refuses:
        // an attached executor holds no storage and no channel to commit on, so
        // the refusal lives in the one place rather than being mirrored here.
        // Step results are encoded with the *queue* serializer, which already
        // carries the codec chain — that is how an encrypting codec reaches the
        // step store with no extra plumbing.
        StepLatch latch = new StepLatch();
        StepContext step = new StepContext(jobId, attempt, serializer, latch, bound::openStepSession);
        JobContext.enter(new JobContext(jobId, taskName, sinkFor(bound), step));
        // Empty until resolved, so a failure to read the disable list runs onError
        // on nothing — which is right, because no before() ran either.
        List<Middleware> chain = List.of();
        long startedAtNanos = System.nanoTime();
        try {
            // Resolved once and reused below: re-reading the disable list between
            // before and after would let a mid-job toggle run after on a middleware
            // whose before never ran. Inside the try because it may read the
            // backend, so a settings failure fails the job rather than leaving it
            // unresolved with its resource scope still bound.
            chain = disabledMiddlewareJson == null
                    ? disables.resolve(taskName, middleware)
                    : MiddlewareDisables.without(middleware, disabledMiddlewareJson);
            for (Middleware m : chain) {
                m.before(context);
            }
            Object argument = serializer.deserializeCall(decodePayload(payload, task.codecs), task.payloadType);
            Object result = task.handler.apply(argument);
            // Before the after hooks, which exist to see a result: a body that
            // caught a step control signal and returned did not produce one.
            latch.check();
            for (Middleware m : chain) {
                m.after(context, result);
            }
            bound.completeJob(token, serializer.serialize(result));
        } catch (StepSleepSignal sleeping) {
            // A slept attempt is neither a result nor a failure: the sleep row is
            // committed, the claim released and the job already Pending at its
            // deadline. It pairs before() with onSleep() rather than after(),
            // runs no onError, and emits job.sleeping instead of job.failed.
            reportSleep(bound, token, context, chain, sleeping, startedAtNanos);
        } catch (Throwable t) {
            for (Middleware m : chain) {
                try {
                    m.onError(context, t);
                } catch (RuntimeException | Error e) {
                    // Same rule as onSleep below, and as onOutcome: a hook that
                    // throws here escapes runJob without reporting, and the
                    // dispatch stalls until its timeout. One faulty middleware
                    // must not starve the rest of the chain either.
                    LOG.warn("middleware " + m.getClass().getName() + " threw on onError (job " + jobId + ")", e);
                }
            }
            // Canonical structured error (middleware above saw the live Throwable).
            String encoded = TaskErrors.encode(t);
            // job.failed fires per attempt, before the retry/dead-letter decision
            // lands as its own outcome. Listener-only: no middleware fan-out.
            emitter.emit(new OutcomeEvent(EventName.JOB_FAILED, jobId, taskName, encoded, -1, false, 0L));
            bound.failJob(token, encoded, RetryDecision.isRetryable(task.retryOn, t));
        } finally {
            // Warns if the job has recorded steps this code no longer runs, and
            // releases the native session. Never throws — by here the side
            // effects have already happened.
            step.finish();
            JobContext.exit();
            if (scope != null) {
                Resources.exit(scope); // unbind the thread + dispose task-scoped resources (LIFO)
            }
        }
    }

    /**
     * Report an attempt that ended in {@code step.sleep}.
     *
     * <p>Runs on the thread the task slept on, where the middleware chain that
     * opened in {@code before} still exists — which is why the native outcome
     * loop stays silent about a sleep rather than reporting it a second time.
     */
    private void reportSleep(
            WorkerControl bound,
            long token,
            TaskContext context,
            List<Middleware> chain,
            StepSleepSignal sleeping,
            long startedAtNanos) {
        // Nothing here may stop the report below. This runs inside the
        // catch(StepSleepSignal) arm, so anything thrown escapes runJob without
        // an outcome and the dispatch stalls until its timeout. `Error` is not
        // theoretical: a step control signal is one, so a hook that touches
        // ctx.step() throws one by construction.
        try {
            warnUnpairedMiddleware(chain);
            for (Middleware m : chain) {
                try {
                    m.onSleep(context, sleeping.wakeAt());
                } catch (RuntimeException | Error e) {
                    // The sleep is already committed; a hook cannot undo it, and
                    // one faulty middleware must not starve the rest.
                    LOG.warn(
                            "middleware " + m.getClass().getName() + " threw on onSleep (job " + context.jobId + ")",
                            e);
                }
            }
            emitter.emit(new SleepEvent(
                    context.jobId,
                    context.taskName,
                    sleeping.stepKey(),
                    sleeping.wakeAt(),
                    (System.nanoTime() - startedAtNanos) / 1_000_000L));
        } catch (RuntimeException | Error e) {
            LOG.warn("could not announce the sleep of job " + context.jobId + "; reporting it regardless", e);
        }
        try {
            bound.sleepJob(token, sleeping.wakeAt());
        } catch (RuntimeException | Error e) {
            // Unreachable unless the control that handed out the session cannot
            // report a sleep. Report something regardless: a job that reports
            // nothing stalls its dispatch until the timeout. The scheduler's
            // (owner, attempt) fence drops this failure anyway, because the
            // sleep already left the job Pending and unclaimed.
            LOG.warn("could not report the sleep of job " + context.jobId + "; failing the attempt instead", e);
            bound.failJob(token, TaskErrors.encode(e), true);
        }
    }

    /**
     * Warn once per middleware that opens something in {@code before} and
     * implements no {@code onSleep}.
     *
     * <p>A sleep ends the attempt without a result, so such middleware leaks
     * whatever its {@code before} opened — a span, a scope, a timer. Nothing can
     * be done for it automatically: only the middleware knows how to close what
     * it opened. Naming it once is the honest answer.
     */
    private static void warnUnpairedMiddleware(List<Middleware> chain) {
        for (Middleware m : chain) {
            Class<?> type = m.getClass();
            if (!overrides(type, "before", TaskContext.class)
                    || overrides(type, "onSleep", TaskContext.class, long.class)) {
                continue;
            }
            if (WARNED_UNPAIRED.add(type.getName())) {
                LOG.warn(type.getName() + " implements before() but not onSleep(), so an attempt that ends in "
                        + "step.sleep() leaves whatever before() opened unclosed. Every before() is matched by "
                        + "exactly one of after() / onSleep().");
            }
        }
    }

    /** Whether {@code type} supplies its own {@code name}, rather than inheriting the default. */
    private static boolean overrides(Class<?> type, String name, Class<?>... params) {
        try {
            return type.getMethod(name, params).getDeclaringClass() != Middleware.class;
        } catch (NoSuchMethodException | RuntimeException | LinkageError e) {
            // An unreadable method is not evidence of an unpaired hook — and a
            // diagnostic must never be able to fail the attempt it describes.
            // Under a native image a user middleware's methods may not be
            // registered for reflective lookup at all, and this runs on the
            // sleep path, where an escaping Error would leave the job
            // unreported until its dispatch timed out.
            return false;
        }
    }

    /**
     * Where a running job's progress and logs go.
     *
     * <p>The one place the two deployments differ: a worker has the database
     * and writes to it, an executor does not and reports to the scheduler,
     * which writes on its behalf. A task body sees neither.
     */
    private JobContext.Sink sinkFor(WorkerControl bound) {
        QueueBackend storage = backend;
        if (storage == null) {
            return new JobContext.Sink() {
                @Override
                public void setProgress(String jobId, int progress) {
                    bound.reportProgress(jobId, progress);
                }

                @Override
                public void writeTaskLog(
                        String jobId, String taskName, String level, String message, @Nullable String extra) {
                    bound.writeTaskLog(jobId, taskName, level, message, extra);
                }
            };
        }
        return new JobContext.Sink() {
            @Override
            public void setProgress(String jobId, int progress) {
                storage.setProgress(jobId, progress);
            }

            @Override
            public void writeTaskLog(
                    String jobId, String taskName, String level, String message, @Nullable String extra) {
                storage.writeTaskLog(jobId, taskName, level, message, extra);
            }
        };
    }

    /** Parse a metadata blob carried on the dispatch (empty on absence/parse failure). */
    private static Map<String, Object> parseMetadata(String metadataJson) {
        try {
            return metadataJson.isEmpty() ? Collections.emptyMap() : JSON.readValue(metadataJson, MAP);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    /** Reverse a task's payload codecs (last applied, first undone). */
    private byte[] decodePayload(byte[] payload, List<String> codecNames) {
        byte[] bytes = payload;
        for (int i = codecNames.size() - 1; i >= 0; i--) {
            String name = codecNames.get(i);
            PayloadCodec codec = codecs.get(name);
            if (codec == null) {
                throw new IllegalStateException("no codec registered named '" + name + "'");
            }
            bytes = codec.decode(bytes);
        }
        return bytes;
    }

    @Override
    public void onOutcome(
            String kind,
            String jobId,
            String taskName,
            String error,
            int retryCount,
            boolean timedOut,
            long wallTimeNs) {
        EventName name = EventName.fromKind(kind);
        OutcomeEvent event = new OutcomeEvent(name, jobId, taskName, error, retryCount, timedOut, wallTimeNs);
        emitter.emit(event);
        for (Middleware m : disables.resolve(taskName, middleware)) {
            try {
                dispatch(m, name, event);
            } catch (RuntimeException e) {
                // One faulty middleware must not starve the rest of this outcome.
                LOG.warn("middleware " + m.getClass().getName() + " threw on " + name + " (job " + jobId + ")", e);
            }
        }
    }

    private static void dispatch(Middleware m, EventName name, OutcomeEvent event) {
        switch (name) {
            case SUCCESS:
                m.onCompleted(event);
                break;
            case RETRY:
                m.onRetry(event);
                break;
            case DEAD:
                m.onDeadLetter(event);
                break;
            case CANCELLED:
                m.onCancel(event);
                break;
            default:
                break;
        }
    }

    /** Lazily load a job's metadata blob into a map (empty on absence/parse failure). */
    private Map<String, Object> loadMetadata(String jobId) {
        if (backend == null) {
            return Collections.emptyMap();
        }
        try {
            JsonNode view = backend.getJobJson(jobId)
                    .map(WorkerDispatchBridge::readTree)
                    .orElse(null);
            JsonNode blob = view == null ? null : view.get("metadata");
            if (blob == null || blob.isNull()) {
                return Collections.emptyMap();
            }
            String json = blob.asText();
            return json.isEmpty() ? Collections.emptyMap() : JSON.readValue(json, MAP);
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    private static @Nullable JsonNode readTree(String json) {
        try {
            return JSON.readTree(json);
        } catch (Exception e) {
            return null;
        }
    }
}
