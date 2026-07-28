package org.byteveda.taskito.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/**
 * The retention windows a worker is actually applying, as reported by the
 * elected cleaner on each sweep. Retention runs in the worker process, so this
 * is the policy that governs the deletes — not this process's configuration.
 * Windows are milliseconds; {@code null} keeps a table forever.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class EffectiveRetention {

    /** False when no table has a window — only per-entry TTLs are swept. */
    public final boolean enabled;

    /** True when the windows are the recommended defaults, set by no one. */
    public final boolean defaulted;

    /** Namespace the windows cover. The purges are not queue-scoped. */
    public final String namespace;

    /** When the cleaner last published this, in Unix milliseconds. */
    public final long reportedAt;

    /** Per-table windows in milliseconds. */
    public final Windows windows;

    @JsonCreator
    public EffectiveRetention(
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("defaulted") boolean defaulted,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("reported_at") long reportedAt,
            @JsonProperty("windows") @Nullable Windows windows) {
        this.enabled = enabled;
        this.defaulted = defaulted;
        this.namespace = namespace;
        this.reportedAt = reportedAt;
        this.windows = windows == null ? Windows.EMPTY : windows;
    }

    /** How long each history table keeps a row, in milliseconds. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Windows {

        static final Windows EMPTY = new Windows(null, null, null, null, null);

        /** Terminal jobs, every terminal status. */
        public final @Nullable Long archivedJobsMs;

        /** Dead-letter entries — the only copy of a payload a human must act on. */
        public final @Nullable Long deadLetterMs;

        /** Task logs — highest write volume, lowest per-row value. */
        public final @Nullable Long taskLogsMs;

        /** Task metrics — feeds the dashboard charts. */
        public final @Nullable Long taskMetricsMs;

        /** Per-attempt job errors. */
        public final @Nullable Long jobErrorsMs;

        @JsonCreator
        public Windows(
                @JsonProperty("archived_jobs_ttl_ms") @Nullable Long archivedJobsMs,
                @JsonProperty("dead_letter_ttl_ms") @Nullable Long deadLetterMs,
                @JsonProperty("task_logs_ttl_ms") @Nullable Long taskLogsMs,
                @JsonProperty("task_metrics_ttl_ms") @Nullable Long taskMetricsMs,
                @JsonProperty("job_errors_ttl_ms") @Nullable Long jobErrorsMs) {
            this.archivedJobsMs = archivedJobsMs;
            this.deadLetterMs = deadLetterMs;
            this.taskLogsMs = taskLogsMs;
            this.taskMetricsMs = taskMetricsMs;
            this.jobErrorsMs = jobErrorsMs;
        }
    }
}
