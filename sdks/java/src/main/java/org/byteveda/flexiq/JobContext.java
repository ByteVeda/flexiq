package org.byteveda.flexiq;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.byteveda.flexiq.internal.ScopeContext;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.byteveda.flexiq.model.TaskLogLevel;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.steps.StepContext;
import org.byteveda.flexiq.steps.StepLatch;
import org.jspecify.annotations.Nullable;

/**
 * The job running on this thread, available to a handler without threading it
 * through every signature.
 *
 * <pre>{@code
 * @TaskHandler("resize")
 * public String resize(Args args) {
 *     JobContext job = JobContext.current();
 *     job.setProgress(50);
 *     job.log("halfway");
 *     job.publish(Map.of("stage", "halfway"));
 *     return "done";
 * }
 * }</pre>
 *
 * <p>Identical under a {@link org.byteveda.flexiq.worker.Worker} and an
 * attached {@link org.byteveda.flexiq.worker.Executor}, which is the whole
 * point: only the route differs. A worker writes straight to its own storage;
 * an executor holds no database credentials, so it reports to the scheduler,
 * which applies the write. A task cannot tell, and should not have to.
 *
 * <p>Every method is best-effort. Losing a progress update is a degradation;
 * failing the job over one would not be, so nothing here throws. {@link #step()}
 * is the one exception, and deliberately: a durable step that quietly did not
 * commit is worse than a failed attempt.
 */
public final class JobContext {
    private static final FlexiQLogger LOG = FlexiQLogger.create("job");
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Level a published partial is stored at, so {@code stream()} can find it. */
    private static final String RESULT_LEVEL = TaskLogLevel.RESULT.wire();

    private static final ScopeContext<JobContext> ACTIVE = new ScopeContext<>();

    /** Where this job's progress and logs actually go. */
    public interface Sink {
        /**
         * Record how far the job has got.
         *
         * @param jobId the running job
         * @param progress the percentage the handler is reporting
         */
        void setProgress(String jobId, int progress);

        /**
         * Append one structured log line for the job.
         *
         * @param jobId the running job
         * @param taskName the task's registered name
         * @param level the severity's wire form
         * @param message the line itself
         * @param extra structured context as JSON, or {@code null}
         */
        void writeTaskLog(String jobId, String taskName, String level, String message, @Nullable String extra);
    }

    private final String jobId;
    private final String taskName;
    private final Sink sink;
    private final StepContext step;

    /**
     * A context whose durable steps refuse.
     *
     * <p>Kept for callers outside the worker: there is no execution claim to
     * fence a step on, so {@link #step()} answers with a retryable refusal
     * rather than committing under an owner nobody holds.
     *
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param sink where progress and logs actually go
     */
    public JobContext(String jobId, String taskName, Sink sink) {
        this(jobId, taskName, sink, new StepContext(jobId, 0, new JsonSerializer(), new StepLatch(), null));
    }

    /**
     * The worker's constructor: {@code step} is bound to this attempt's session.
     *
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param sink where progress and logs actually go
     * @param step the step context bound to this attempt's session
     * @hidden
     */
    public JobContext(String jobId, String taskName, Sink sink, StepContext step) {
        this.jobId = jobId;
        this.taskName = taskName;
        this.sink = sink;
        this.step = step;
    }

    /**
     * The job running on this thread.
     *
     * @return the context bound on this thread
     * @throws IllegalStateException outside a task body, where there is no job
     *     to describe and any answer would be a guess
     */
    public static JobContext current() {
        JobContext active = ACTIVE.get();
        if (active == null) {
            throw new IllegalStateException(
                    "no job is running on this thread; JobContext.current() is only valid inside a task handler");
        }
        return active;
    }

    /**
     * Bind {@code context} for the duration of one task.
     *
     * @param context what {@link #current()} returns until {@link #exit()} runs
     * @hidden
     */
    public static void enter(JobContext context) {
        ACTIVE.set(context);
    }

    /** Unbind the current context. Must run in a {@code finally}. @hidden */
    public static void exit() {
        ACTIVE.clear();
    }

    /**
     * The running job's id.
     *
     * @return the id
     */
    public String jobId() {
        return jobId;
    }

    /**
     * The running task's registered name.
     *
     * @return the name
     */
    public String taskName() {
        return taskName;
    }

    /**
     * Durable inline steps for this job.
     *
     * <pre>{@code
     * JobContext ctx = JobContext.current();
     * Charge charge = ctx.step().run("charge", Charge.class,
     *         () -> stripe.charge(order, ctx.step().idempotencyKey()));
     * ctx.step().sleep(Duration.ofHours(1));
     * }</pre>
     *
     * <p>A step runs once per job rather than once per attempt: its result is
     * committed, and every later attempt returns the committed value. See
     * {@link StepContext} for what that does and does not guarantee.
     *
     * <p>Available on an in-process worker. An attached executor holds no
     * storage and no channel to commit a step on, so every step there refuses
     * with a retryable
     * {@link org.byteveda.flexiq.steps.StepUnavailableError} — the same answer
     * a backend with no step store gives.
     *
     * @return this attempt's step context
     */
    public StepContext step() {
        return step;
    }

    /**
     * Report progress (0-100) for observability. Values outside that range are ignored.
     *
     * @param progress how far the handler has got, as a percentage
     */
    public void setProgress(int progress) {
        if (progress < 0 || progress > 100) {
            LOG.warn("ignoring out-of-range progress " + progress + " for job " + jobId);
            return;
        }
        sink.setProgress(jobId, progress);
    }

    /**
     * Write an {@code info} log line against this job.
     *
     * @param message the line itself
     */
    public void log(String message) {
        log(TaskLogLevel.INFO, message, null);
    }

    /**
     * Write a log line at {@code level}.
     *
     * @param level the severity
     * @param message the line itself
     */
    public void log(TaskLogLevel level, String message) {
        log(level, message, null);
    }

    /**
     * Write a log line at {@code level}, optionally with a pre-encoded JSON {@code extra}.
     *
     * <p>The level is the enum rather than its wire string, matching
     * {@link FlexiQ#writeTaskLog(String, String, TaskLogLevel, String, String)} — the
     * string-typed form there is deprecated, so there is no reason to introduce another.
     *
     * @param level the severity
     * @param message the line itself
     * @param extra structured context as pre-encoded JSON, or {@code null}
     */
    public void log(TaskLogLevel level, String message, @Nullable String extra) {
        sink.writeTaskLog(jobId, taskName, level.wire(), message, extra);
    }

    /**
     * Publish a partial result, consumable live by a {@code stream()} reader.
     *
     * <p>Stored as a {@code result}-level log line, which is what separates it
     * from the job's ordinary logs.
     *
     * @param value anything Jackson can serialize
     */
    public void publish(Object value) {
        String encoded;
        try {
            encoded = JSON.writeValueAsString(value);
        } catch (Exception e) {
            // Better a partial recorded as text than a job failed over how its
            // progress was formatted.
            LOG.warn("published value for job " + jobId + " is not JSON-serializable; storing its toString()");
            encoded = String.valueOf(value);
        }
        sink.writeTaskLog(jobId, taskName, RESULT_LEVEL, "", encoded);
    }
}
