package org.byteveda.taskito.webhooks;

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
        @Nullable String taskFilter,
        @Nullable Map<String, String> headers,
        @Nullable String secret,
        @Nullable Integer maxRetries,
        @Nullable Long timeoutMs,
        @Nullable Boolean enabled,
        @Nullable String description) {

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder so callers set only the fields they intend to change. */
    public static final class Builder {
        private @Nullable String url;
        private @Nullable List<String> events;
        private @Nullable String taskFilter;
        private @Nullable Map<String, String> headers;
        private @Nullable String secret;
        private @Nullable Integer maxRetries;
        private @Nullable Long timeoutMs;
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

        public Builder taskFilter(@Nullable String taskFilter) {
            this.taskFilter = taskFilter;
            return this;
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
                    url, events, taskFilter, headers, secret, maxRetries, timeoutMs, enabled, description);
        }
    }
}
