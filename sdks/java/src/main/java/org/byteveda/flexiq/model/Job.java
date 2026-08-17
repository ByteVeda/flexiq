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

    public final String id;
    public final String queue;
    public final String taskName;
    public final JobStatus status;
    public final int priority;
    public final long createdAt;
    public final long scheduledAt;
    public final Long startedAt;
    public final Long completedAt;
    public final int retryCount;
    public final int maxRetries;
    public final long timeoutMs;
    public final Integer progress;
    public final String error;
    public final String uniqueKey;
    public final String namespace;
    public final String metadata;

    /** Structured notes as canonical JSON, or {@code null}. Use {@link #notesMap()} for a parsed view. */
    public final String notes;

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

    /** The structured notes parsed into a map, or empty when the job carries none. */
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
