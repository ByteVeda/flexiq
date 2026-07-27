package org.byteveda.taskito.model;

import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * One message pulled from a log topic. {@link #id} is the cursor token to pass to
 * {@code ackTopic}. {@link #payload} is the opaque publish payload — decode it
 * with the queue's serializer. {@code createdAt} is Unix milliseconds.
 */
public final class TopicMessage {
    /** Message id; doubles as the cursor token for {@code ackTopic}. */
    public final String id;

    /** Opaque publish payload — decode with the queue's serializer. */
    public final byte[] payload;

    /** Caller metadata, or {@code null} when none was set. */
    public final @Nullable Map<String, Object> metadata;

    /** Structured notes, or {@code null} when none were set. */
    public final @Nullable Map<String, Object> notes;

    /** Unix-millisecond publish time. */
    public final long createdAt;

    public TopicMessage(
            String id,
            byte[] payload,
            @Nullable Map<String, Object> metadata,
            @Nullable Map<String, Object> notes,
            long createdAt) {
        this.id = id;
        this.payload = payload;
        this.metadata = metadata;
        this.notes = notes;
        this.createdAt = createdAt;
    }
}
