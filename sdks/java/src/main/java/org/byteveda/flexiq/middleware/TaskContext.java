package org.byteveda.flexiq.middleware;

import java.util.HashMap;
import java.util.Map;

/**
 * Identifies a task as it executes on a worker. {@link #attributes()} is a
 * per-execution scratch map shared across {@code before}/{@code after}/
 * {@code onError} for the same job; {@link #job()} exposes its metadata.
 */
public final class TaskContext {
    /** The running job's id. */
    public final String jobId;

    /** The running task's registered name. */
    public final String taskName;

    private final Map<String, Object> attributes = new HashMap<>();
    private final JobInfo job;
    // Monotonic, so a wall-clock adjustment mid-task can't skew the elapsed time.
    private final long startedAtNanos = System.nanoTime();

    /**
     * A context for one execution; the elapsed clock starts here.
     *
     * @param jobId the running job's id
     * @param taskName the running task's registered name
     * @param job the job view, whose metadata is loaded only if a hook asks for it
     */
    public TaskContext(String jobId, String taskName, JobInfo job) {
        this.jobId = jobId;
        this.taskName = taskName;
        this.job = job;
    }

    /**
     * A context whose job carries no metadata, for a caller with none to hand over.
     *
     * @param jobId the running job's id
     * @param taskName the running task's registered name
     */
    public TaskContext(String jobId, String taskName) {
        this(jobId, taskName, new JobInfo(jobId, taskName, java.util.Collections::emptyMap));
    }

    /**
     * Mutable per-execution scratch shared across this job's middleware hooks.
     *
     * @return the live map, the same instance every hook for this execution sees
     */
    public Map<String, Object> attributes() {
        return attributes;
    }

    /**
     * The executing job, including its (lazily loaded) metadata.
     *
     * @return the job view
     */
    public JobInfo job() {
        return job;
    }

    /**
     * Time spent on this execution so far, in milliseconds. Measured from when the
     * worker built this context, so it covers the handler plus the middleware around it.
     *
     * @return elapsed milliseconds — in {@code after}/{@code onError}, the run's duration.
     */
    public long elapsedMs() {
        return (System.nanoTime() - startedAtNanos) / 1_000_000L;
    }
}
