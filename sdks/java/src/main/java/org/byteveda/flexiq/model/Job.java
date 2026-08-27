package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Optional;
import org.byteveda.flexiq.errors.SerializationException;

/** Immutable view of a job. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Job {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** The job's id, minted at enqueue. */
    public final String id;

    /** The queue it was enqueued to. */
    public final String queue;

    /** The task's registered name. */
    public final String taskName;

    /** Where the job stands right now. */
    public final JobStatus status;

    /** Dispatch priority; higher runs first within a queue. */
    public final int priority;

    /** When it was enqueued, in Unix milliseconds. */
    public final long createdAt;

    /** When it becomes runnable, in Unix milliseconds; later than {@code createdAt} for a delayed or slept job. */
    public final long scheduledAt;

    /** When the current attempt was claimed, or {@code null} before the first. */
    public final Long startedAt;

    /** When it reached a terminal state, or {@code null} while it is still live. */
    public final Long completedAt;

    /** How many attempts have been spent. */
    public final int retryCount;

    /** The retry ceiling before it dead-letters. */
    public final int maxRetries;

    /** How long one attempt may run before it is failed as timed out. */
    public final long timeoutMs;

    /** The percentage the handler last reported, or {@code null} if it reported none. */
    public final Integer progress;

    /** The last attempt's stored error, or {@code null}; decode with {@code TaskErrors}. */
    public final String error;

    /** The idempotency key it was admitted under, or {@code null}. */
    public final String uniqueKey;

    /** The deployment namespace it belongs to. */
    public final String namespace;

    /** The opaque metadata blob attached at enqueue, or {@code null}. */
    public final String metadata;

    /** Structured notes as canonical JSON, or {@code null}. Use {@link #notesMap()} for a parsed view. */
    public final String notes;

    /**
     * Decoded from the core's JSON job view.
     *
     * @param id the job's id, minted at enqueue
     * @param queue the queue it was enqueued to
     * @param taskName the task's registered name
     * @param status where the job stands right now
     * @param priority dispatch priority; higher runs first within a queue
     * @param createdAt when it was enqueued, in Unix milliseconds
     * @param scheduledAt when it becomes runnable, in Unix milliseconds; later than {@code createdAt} for a delayed or
     *     slept job
     * @param startedAt when the current attempt was claimed, or {@code null} before the first
     * @param completedAt when it reached a terminal state, or {@code null} while it is still live
     * @param retryCount how many attempts have been spent
     * @param maxRetries the retry ceiling before it dead-letters
     * @param timeoutMs how long one attempt may run before it is failed as timed out
     * @param progress the percentage the handler last reported, or {@code null} if it reported none
     * @param error the last attempt's stored error, or {@code null}; decode with {@code TaskErrors}
     * @param uniqueKey the idempotency key it was admitted under, or {@code null}
     * @param namespace the deployment namespace it belongs to
     * @param metadata the opaque metadata blob attached at enqueue, or {@code null}
     * @param notes structured notes as canonical JSON, or {@code null}. Use {@link #notesMap()} for a parsed view
     */
    @JsonCreator
    public Job(
            @JsonProperty("id") String id,
            @JsonProperty("queue") String queue,
            @JsonProperty("taskName") String taskName,
            @JsonProperty("status") JobStatus status,
            @JsonProperty("priority") int priority,
            @JsonProperty("createdAt") long createdAt,
            @JsonProperty("scheduledAt") long scheduledAt,
            @JsonProperty("startedAt") Long startedAt,
            @JsonProperty("completedAt") Long completedAt,
            @JsonProperty("retryCount") int retryCount,
            @JsonProperty("maxRetries") int maxRetries,
            @JsonProperty("timeoutMs") long timeoutMs,
            @JsonProperty("progress") Integer progress,
            @JsonProperty("error") String error,
            @JsonProperty("uniqueKey") String uniqueKey,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("metadata") String metadata,
            @JsonProperty("notes") String notes) {
        this.id = id;
        this.queue = queue;
        this.taskName = taskName;
        this.status = status;
        this.priority = priority;
        this.createdAt = createdAt;
        this.scheduledAt = scheduledAt;
        this.startedAt = startedAt;
        this.completedAt = completedAt;
        this.retryCount = retryCount;
        this.maxRetries = maxRetries;
        this.timeoutMs = timeoutMs;
        this.progress = progress;
        this.error = error;
        this.uniqueKey = uniqueKey;
        this.namespace = namespace;
        this.metadata = metadata;
        this.notes = notes;
    }

    /**
     * The structured notes parsed into a map, or empty when the job carries none.
     *
     * @return the notes, or empty when {@link #notes} is {@code null}
     */
    public Optional<Map<String, Object>> notesMap() {
        if (notes == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(MAPPER.readValue(notes, new TypeReference<Map<String, Object>>() {}));
        } catch (Exception e) {
            throw new SerializationException("failed to parse job notes", e);
        }
    }
}
