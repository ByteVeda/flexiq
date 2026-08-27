package org.byteveda.flexiq.webhooks;

import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * A partial webhook edit. Every field is nullable: {@code null} leaves the
 * corresponding webhook field unchanged, while a provided value replaces it
 * wholesale (for {@code events} and {@code headers} the whole collection is
 * swapped, not merged). Consumed by {@link WebhookManager#update}.
 *
 * @param url the endpoint to post to
 * @param events the event names to subscribe to, replacing the whole list; an empty
 *     list is a replacement like any other and unsubscribes from everything
 * @param taskFilters the task names to restrict delivery to, replacing the whole list;
 *     an empty list clears the restriction rather than being ignored
 * @param headers the extra request headers, replacing the whole map; an empty map
 *     removes every header
 * @param secret the HMAC signing secret
 * @param maxRetries how many times a failed delivery is retried
 * @param timeoutMs how long one request may take
 * @param retryBackoff the multiplier between retries
 * @param enabled whether the webhook delivers at all
 * @param description the operator-facing note
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

    /**
     * A builder with every field unset, so nothing changes until one is.
     *
     * @return the builder
     */
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

        /**
         * Repoint the hook.
         *
         * @param url the endpoint to post to
         * @return {@code this}, for chaining
         */
        public Builder url(String url) {
            this.url = url;
            return this;
        }

        /**
         * Replace the subscribed events wholesale; an empty list unsubscribes from everything.
         *
         * @param events the event wire names to fire on
         * @return {@code this}, for chaining
         */
        public Builder events(List<String> events) {
            this.events = events;
            return this;
        }

        /**
         * Replace the task restriction wholesale; an empty list clears it.
         *
         * @param taskFilters the task names to deliver for, or {@code null} to leave
         *     the restriction as it is
         * @return {@code this}, for chaining
         */
        public Builder taskFilters(@Nullable List<String> taskFilters) {
            this.taskFilters = taskFilters;
            return this;
        }

        /**
         * Restrict the hook to one task name.
         *
         * @param taskFilter the task name to deliver for, or {@code null} to leave the
         *     restriction as it is
         * @return {@code this}, for chaining
         * @deprecated a hook can filter on several tasks; use {@link #taskFilters}.
         */
        @Deprecated
        public Builder taskFilter(@Nullable String taskFilter) {
            return taskFilters(taskFilter == null ? null : List.of(taskFilter));
        }

        /**
         * Replace the extra request headers wholesale; an empty map removes every one.
         *
         * @param headers the headers to send with each delivery
         * @return {@code this}, for chaining
         */
        public Builder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        /**
         * Re-key the delivery signature.
         *
         * @param secret the HMAC key, or {@code null} to leave it as it is
         * @return {@code this}, for chaining
         */
        public Builder secret(@Nullable String secret) {
            this.secret = secret;
            return this;
        }

        /**
         * How many times a failed delivery is retried.
         *
         * @param maxRetries the retry ceiling
         * @return {@code this}, for chaining
         */
        public Builder maxRetries(Integer maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * How long one delivery request may take.
         *
         * @param timeoutMs the timeout in milliseconds
         * @return {@code this}, for chaining
         */
        public Builder timeoutMs(Long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        /**
         * Retry backoff base, in seconds: the Nth wait (from zero) is {@code retryBackoff ^ N}.
         *
         * @param retryBackoff the base; {@value Webhook#DEFAULT_RETRY_BACKOFF} gives 1s, 2s, 4s, …
         * @return {@code this}, for chaining
         */
        public Builder retryBackoff(Double retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        /**
         * Whether the hook delivers at all.
         *
         * @param enabled {@code false} to keep the config but stop firing
         * @return {@code this}, for chaining
         */
        public Builder enabled(Boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * An operator-facing note, shown in the dashboard.
         *
         * @param description what this hook is for, or {@code null} to leave it as it is
         * @return {@code this}, for chaining
         */
        public Builder description(@Nullable String description) {
            this.description = description;
            return this;
        }

        /**
         * Freeze the edits collected so far.
         *
         * @return the update, ready for {@link WebhookManager#update}
         */
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
