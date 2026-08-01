package org.byteveda.taskito.worker;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.byteveda.taskito.JobContext;
import org.byteveda.taskito.errors.TaskErrors;
import org.byteveda.taskito.events.Emitter;
import org.byteveda.taskito.events.EventName;
import org.byteveda.taskito.events.OutcomeEvent;
import org.byteveda.taskito.internal.MiddlewareDisables;
import org.byteveda.taskito.logging.TaskitoLogger;
import org.byteveda.taskito.middleware.JobInfo;
import org.byteveda.taskito.middleware.Middleware;
import org.byteveda.taskito.middleware.TaskContext;
import org.byteveda.taskito.resources.ResourceRuntime;
import org.byteveda.taskito.resources.Resources;
import org.byteveda.taskito.resources.TaskScope;
import org.byteveda.taskito.serialization.PayloadCodec;
import org.byteveda.taskito.serialization.Serializer;
import org.byteveda.taskito.spi.QueueBackend;
import org.byteveda.taskito.spi.WorkerBridge;
import org.byteveda.taskito.spi.WorkerControl;
import org.jspecify.annotations.Nullable;

/**
 * Bridges native job dispatch to registered handlers. {@code onJob} hands work to
 * an executor, runs middleware around the handler, and completes via the
 * {@link WorkerControl}; {@code onOutcome} fans finished jobs out to middleware
 * and event listeners.
 */
final class WorkerDispatchBridge implements WorkerBridge {
    private static final TaskitoLogger LOG = TaskitoLogger.create("worker");
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<Map<String, Object>>() {};

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
        executor.execute(() -> runJob(token, jobId, taskName, payload, metadataJson, disabledMiddlewareJson));
    }

    private void runJob(
            long token,
            String jobId,
            String taskName,
            byte[] payload,
            @Nullable String metadataJson,
            @Nullable String disabledMiddlewareJson) {
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
        JobContext.enter(new JobContext(jobId, taskName, sinkFor(bound)));
        // Empty until resolved, so a failure to read the disable list runs onError
        // on nothing — which is right, because no before() ran either.
        List<Middleware> chain = List.of();
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
            for (Middleware m : chain) {
                m.after(context, result);
            }
            bound.completeJob(token, serializer.serialize(result));
        } catch (Throwable t) {
            for (Middleware m : chain) {
                m.onError(context, t);
            }
            // Canonical structured error (middleware above saw the live Throwable).
            String encoded = TaskErrors.encode(t);
            // job.failed fires per attempt, before the retry/dead-letter decision
            // lands as its own outcome. Listener-only: no middleware fan-out.
            emitter.emit(new OutcomeEvent(EventName.JOB_FAILED, jobId, taskName, encoded, -1, false, 0L));
            bound.failJob(token, encoded, RetryDecision.isRetryable(task.retryOn, t));
        } finally {
            JobContext.exit();
            if (scope != null) {
                Resources.exit(scope); // unbind the thread + dispose task-scoped resources (LIFO)
            }
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
