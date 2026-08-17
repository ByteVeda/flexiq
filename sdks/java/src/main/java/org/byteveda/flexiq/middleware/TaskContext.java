package org.byteveda.flexiq.middleware;

import java.util.HashMap;
import java.util.Map;

/**
 * Identifies a task as it executes on a worker. {@link #attributes()} is a
 * per-execution scratch map shared across {@code before}/{@code after}/
 * {@code onError} for the same job; {@link #job()} exposes its metadata.
 */
public final class TaskContext {
    public final String jobId;
    public final String taskName;

    private final Map<String, Object> attributes = new HashMap<>();
    private final JobInfo job;
    // Monotonic, so a wall-clock adjustment mid-task can't skew the elapsed time.
    private final long startedAtNanos = System.nanoTime();

    public TaskContext(String jobId, String taskName, JobInfo job) {
        this.jobId = jobId;
        this.taskName = taskName;
        this.job = job;
    }

    public TaskContext(String jobId, String taskName) {
        this(jobId, taskName, new JobInfo(jobId, taskName, java.util.Collections::emptyMap));
    }

    /** Mutable per-execution scratch shared across this job's middleware hooks. */
    public Map<String, Object> attributes() {
        return attributes;
    }

    /** The executing job, including its (lazily loaded) metadata. */
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
