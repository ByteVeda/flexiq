package org.byteveda.flexiq.webhooks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.events.EventName;
import org.jspecify.annotations.Nullable;

/** A stored webhook subscription. Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Webhook {

    /** Contract default: waits of 1s, 2s, 4s, ... */
    public static final double DEFAULT_RETRY_BACKOFF = 2.0;

    /** Server-assigned identity, minted on create and stable for the hook's life. */
    public final String id;

    /** The endpoint each delivery posts to. */
    public final String url;

    /** Event wire names this hook fires on, e.g. {@code job.completed} (legacy outcome aliases still match). */
    public final List<String> events;

    /** Task names this hook is restricted to. Empty = every task. */
    public final List<String> taskFilters;

    /**
     * The first entry of {@link #taskFilters}, or {@code null} when unrestricted.
     *
     * @deprecated a hook can filter on several tasks; read {@link #taskFilters}.
     *     This field only ever shows the first, so it silently hides the rest.
     */
    @Deprecated
    public final @Nullable String taskFilter;

    /** Extra request headers sent with every delivery. */
    public final Map<String, String> headers;

    /** The HMAC signing secret, or {@code null} when deliveries are unsigned. */
    public final @Nullable String secret;

    /** How many times a failed delivery is retried before it is given up on. */
    public final int maxRetries;

    /** How long one delivery request may take, in milliseconds. */
    public final long timeoutMs;

    /** Retry backoff base, in seconds: the Nth wait (counted from zero) is {@code retryBackoff ^ N}. */
    public final double retryBackoff;

    /** Whether this hook delivers at all; a disabled hook keeps its config and fires nothing. */
    public final boolean enabled;

    /** An operator-facing note, or {@code null}. */
    public final @Nullable String description;

    /** When the hook was created, in Unix milliseconds. */
    public final long createdAt;

    /** When the hook was last edited, in Unix milliseconds. */
    public final long updatedAt;

    /**
     * Decode a stored row, accepting the scalar {@code taskFilter} written before
     * a hook could filter on more than one task. An explicit {@code taskFilters}
     * list wins; the scalar only seeds an absent one.
     */
    @JsonCreator
    static Webhook fromJson(
            @JsonProperty("id") String id,
            @JsonProperty("url") String url,
            @JsonProperty("events") @Nullable List<String> events,
            @JsonProperty("taskFilters") @Nullable List<String> taskFilters,
            @JsonProperty("taskFilter") @Nullable String legacyTaskFilter,
            @JsonProperty("headers") @Nullable Map<String, String> headers,
            @JsonProperty("secret") @Nullable String secret,
            @JsonProperty("maxRetries") int maxRetries,
            @JsonProperty("timeoutMs") long timeoutMs,
            @JsonProperty("retryBackoff") double retryBackoff,
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("description") @Nullable String description,
            @JsonProperty("createdAt") long createdAt,
            @JsonProperty("updatedAt") long updatedAt) {
        List<String> filters = taskFilters != null
                ? taskFilters
                : (legacyTaskFilter == null ? Collections.emptyList() : List.of(legacyTaskFilter));
        return new Webhook(
                id,
                url,
                events,
                filters,
                headers,
                secret,
                maxRetries,
                timeoutMs,
                retryBackoff,
                enabled,
                description,
                createdAt,
                updatedAt);
    }

    Webhook(
            String id,
            String url,
            @Nullable List<String> events,
            @Nullable List<String> taskFilters,
            @Nullable Map<String, String> headers,
            @Nullable String secret,
            int maxRetries,
            long timeoutMs,
            double retryBackoff,
            boolean enabled,
            @Nullable String description,
            long createdAt,
            long updatedAt) {
        this.id = id;
        this.url = url;
        this.events = events == null ? Collections.emptyList() : events;
        this.taskFilters = taskFilters == null ? Collections.emptyList() : List.copyOf(taskFilters);
        this.taskFilter = this.taskFilters.isEmpty() ? null : this.taskFilters.get(0);
        this.headers = headers == null ? Collections.emptyMap() : headers;
        this.secret = secret;
        this.maxRetries = maxRetries;
        this.timeoutMs = timeoutMs;
        // A row written before this field existed decodes as 0.0, which would
        // collapse the curve to no wait at all; fall back to the contract default.
        this.retryBackoff = retryBackoff > 0 ? retryBackoff : DEFAULT_RETRY_BACKOFF;
        this.enabled = enabled;
        this.description = description;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    /**
     * Start a draft hook posting to {@code url}.
     *
     * @param url the endpoint each delivery posts to
     * @return the draft, to be handed to {@link WebhookManager#create}
     */
    public static Builder builder(String url) {
        return new Builder(url);
    }

    /** A draft webhook; the manager assigns its id and timestamps on create. */
    public static final class Builder {
        final String url;
        final List<String> events = new ArrayList<>();
        final Map<String, String> headers = new LinkedHashMap<>();
        final List<String> taskFilters = new ArrayList<>();

        @Nullable
        String secret;

        int maxRetries = 3;
        long timeoutMs = 10_000;
        double retryBackoff = DEFAULT_RETRY_BACKOFF;
        boolean enabled = true;

        @Nullable
        String description;

        Builder(String url) {
            this.url = url;
        }

        /**
         * Fire on these events. Called more than once, they accumulate.
         *
         * @param names the events to subscribe to
         * @return {@code this}, for chaining
         */
        public Builder on(EventName... names) {
            for (EventName name : names) {
                events.add(name.wireName());
            }
            return this;
        }

        /**
         * Restrict the hook to these task names. Called more than once, they accumulate.
         *
         * @param taskFilters the task names to deliver for; none set means every task
         * @return {@code this}, for chaining
         */
        public Builder taskFilters(String... taskFilters) {
            Collections.addAll(this.taskFilters, taskFilters);
            return this;
        }

        /**
         * Restrict the hook to one task name.
         *
         * @param taskFilter the task name to deliver for
         * @return {@code this}, for chaining
         * @deprecated a hook can filter on several tasks; use {@link #taskFilters}.
         */
        @Deprecated
        public Builder taskFilter(String taskFilter) {
            return taskFilters(taskFilter);
        }

        /**
         * Send an extra request header with every delivery.
         *
         * @param name the header name; setting the same one twice keeps the last value
         * @param value the header value
         * @return {@code this}, for chaining
         */
        public Builder header(String name, String value) {
            headers.put(name, value);
            return this;
        }

        /**
         * Sign every delivery with this secret, so the receiver can prove it came from here.
         *
         * @param secret the HMAC key, or {@code null} to leave deliveries unsigned;
         *     {@link WebhookManager#generateSecret()} mints one
         * @return {@code this}, for chaining
         */
        public Builder secret(@Nullable String secret) {
            this.secret = secret;
            return this;
        }

        /**
         * How many times a failed delivery is retried. Defaults to 3.
         *
         * @param maxRetries the retry ceiling
         * @return {@code this}, for chaining
         */
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * How long one delivery request may take. Defaults to 10 seconds.
         *
         * @param timeoutMs the timeout in milliseconds
         * @return {@code this}, for chaining
         */
        public Builder timeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        /**
         * Retry backoff base, in seconds: the Nth wait (from zero) is {@code retryBackoff ^ N}.
         *
         * @param retryBackoff the base; {@value #DEFAULT_RETRY_BACKOFF} gives 1s, 2s, 4s, …
         * @return {@code this}, for chaining
         */
        public Builder retryBackoff(double retryBackoff) {
            this.retryBackoff = retryBackoff;
            return this;
        }

        /**
         * Whether the hook delivers at all. Defaults to {@code true}.
         *
         * @param enabled {@code false} to keep the config but stop firing
         * @return {@code this}, for chaining
         */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * An operator-facing note, shown in the dashboard.
         *
         * @param description what this hook is for
         * @return {@code this}, for chaining
         */
        public Builder description(String description) {
            this.description = description;
            return this;
        }
    }
}
