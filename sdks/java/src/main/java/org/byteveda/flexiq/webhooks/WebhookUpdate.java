package org.byteveda.flexiq.webhooks;

import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * A partial webhook edit. Every field is nullable: {@code null} leaves the
 * corresponding webhook field unchanged, while a provided value replaces it
 * wholesale (for {@code events} and {@code headers} the whole collection is
 * swapped, not merged). Consumed by {@link WebhookManager#update}.
 */
public record WebhookUpdate(
        @Nullable String url,
        @Nullable List<String> events,
        @Nullable List<String> taskFilters,
        @Nullable Map<String, String> headers,
        @Nullable String secret,
        @Nullable Integer maxRetries,
        @Nullable Long timeoutMs,
        @Nullable Double retryBackoff,
        @Nullable Boolean enabled,
        @Nullable String description) {

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder so callers set only the fields they intend to change. */
    public static final class Builder {
        private @Nullable String url;
        private @Nullable List<String> events;
        private @Nullable List<String> taskFilters;
        private @Nullable Map<String, String> headers;
        private @Nullable String secret;
        private @Nullable Integer maxRetries;
        private @Nullable Long timeoutMs;
        private @Nullable Double retryBackoff;
        private @Nullable Boolean enabled;
        private @Nullable String description;

        private Builder() {}

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder events(List<String> events) {
            this.events = events;
            return this;
        }

        /** Replace the task restriction wholesale; an empty list clears it. */
        public Builder taskFilters(@Nullable List<String> taskFilters) {
            this.taskFilters = taskFilters;
            return this;
        }

        /**
         * @deprecated a hook can filter on several tasks; use {@link #taskFilters}.
         */
        @Deprecated
        public Builder taskFilter(@Nullable String taskFilter) {
            return taskFilters(taskFilter == null ? null : List.of(taskFilter));
        }

        public Builder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public Builder secret(@Nullable String secret) {
            this.secret = secret;
            return this;
        }

        public Builder maxRetries(Integer maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder timeoutMs(Long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        /** Retry backoff base, in seconds: the Nth wait (from zero) is {@code retryBackoff ^ N}. */
        public Builder retryBackoff(Double retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        public Builder enabled(Boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder description(@Nullable String description) {
            this.description = description;
            return this;
        }

        public WebhookUpdate build() {
            return new WebhookUpdate(
                    url,
                    events,
                    taskFilters,
                    headers,
                    secret,
                    maxRetries,
                    timeoutMs,
                    retryBackoff,
                    enabled,
                    description);
        }
    }
}
