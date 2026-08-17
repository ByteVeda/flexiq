package org.byteveda.flexiq.workflows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Optional;
import org.byteveda.flexiq.errors.SerializationException;
import org.byteveda.flexiq.errors.WorkflowException;
import org.byteveda.flexiq.spi.QueueBackend;

/** A submitted workflow run. Query {@link #status()}, block on {@link #await}, or {@link #cancel()}. */
public final class WorkflowRun implements AutoCloseable {
    private final QueueBackend backend;
    private final ObjectMapper json;
    private final String id;
    private final String name;

    public WorkflowRun(QueueBackend backend, ObjectMapper json, String id, String name) {
        this.backend = backend;
        this.json = json;
        this.id = id;
        this.name = name;
    }

    public String id() {
        return id;
    }

    /** Alias of {@link #id()} in the guide's vocabulary. */
    public String runId() {
        return id;
    }

    public String name() {
        return name;
    }

    /** Current run + node snapshot, or empty if the run no longer exists. */
    public Optional<WorkflowStatus> status() {
        return backend.getWorkflowStatusJson(id).map(this::decode);
    }

    /** Block until the run reaches a terminal state, polling every 100ms. */
    public WorkflowStatus await(Duration timeout) {
        return await(timeout, Duration.ofMillis(100));
    }

    /** Block until terminal, polling at {@code pollInterval}; throws on timeout. */
    public WorkflowStatus await(Duration timeout, Duration pollInterval) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            WorkflowStatus status = status().orElseThrow(() -> new WorkflowException("workflow run not found: " + id));
            if (status.isTerminal()) {
                return status;
            }
            if (System.nanoTime() >= deadline) {
                throw new WorkflowException("workflow '" + id + "' did not finish within " + timeout);
            }
            try {
                Thread.sleep(pollInterval.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new WorkflowException("interrupted while awaiting workflow " + id, e);
            }
        }
    }

    /** Cancel the run: skip its pending nodes and mark it cancelled. */
    public void cancel() {
        backend.cancelWorkflowRun(id);
    }

    /** No native resources are held; provided so a run can be used in try-with-resources. */
    @Override
    public void close() {
        // Intentionally empty — keeps the API consistent and future-proof.
    }

    private WorkflowStatus decode(String raw) {
        try {
            return json.readValue(raw, WorkflowStatus.class);
        } catch (Exception e) {
            throw new SerializationException("failed to decode workflow status", e);
        }
    }
}
