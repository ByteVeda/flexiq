package org.byteveda.flexiq.webhooks;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * One attempted webhook delivery, persisted in the per-subscription delivery
 * log. Timestamps are Unix milliseconds. {@code status} is one of
 * {@code pending}, {@code delivered}, {@code failed}, or {@code dead}.
 *
 * <p>Jackson maps this record by component name; unknown fields are ignored so
 * a log written by a newer build still loads.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Delivery(
        String id,
        String subscriptionId,
        String event,
        @Nullable String taskName,
        @Nullable String jobId,
        String status,
        int attempts,
        @Nullable Integer responseCode,
        @Nullable String responseBody,
        @Nullable Long latencyMs,
        @Nullable String error,
        long createdAt,
        @Nullable Long completedAt) {

    static final String DELIVERED = "delivered";
    static final String FAILED = "failed";

    private static final int RESPONSE_BODY_MAX_BYTES = 2048;

    /**
     * Build a completed (or {@code pending}) delivery, minting an id and
     * stamping the timestamps. The response body is truncated to
     * {@value #RESPONSE_BODY_MAX_BYTES} UTF-8 bytes so a chatty endpoint can't
     * bloat the log; {@code completedAt} is set unless the status is pending.
     */
    static Delivery of(
            String subscriptionId,
            DeliveryContext ctx,
            String status,
            int attempts,
            @Nullable Integer responseCode,
            @Nullable String responseBody,
            @Nullable Long latencyMs,
            @Nullable String error) {
        long now = System.currentTimeMillis();
        return new Delivery(
                UUID.randomUUID().toString(),
                subscriptionId,
                ctx.event(),
                ctx.taskName(),
                ctx.jobId(),
                status,
                attempts,
                responseCode,
                truncate(responseBody),
                latencyMs,
                error,
                now,
                "pending".equals(status) ? null : now);
    }

    private static @Nullable String truncate(@Nullable String body) {
        if (body == null) {
            return null;
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= RESPONSE_BODY_MAX_BYTES) {
            return body;
        }
        // Decoding a byte-boundary cut may drop a partial trailing code point;
        // the replacement behaviour matches the cross-SDK truncation contract.
        return new String(bytes, 0, RESPONSE_BODY_MAX_BYTES, StandardCharsets.UTF_8) + "…";
    }
}
