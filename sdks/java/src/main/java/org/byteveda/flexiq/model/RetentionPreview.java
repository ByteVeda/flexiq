package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/**
 * What a retention purge would delete right now, without deleting anything.
 * Counts are a point-in-time snapshot taken at {@code referenceTime}, computed
 * against the previewed windows. Windows are milliseconds; {@code null} keeps a
 * table forever and its count reflects per-entry TTLs only.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class RetentionPreview {

    /** False when no table has a window — only per-entry TTLs would be swept. */
    public final boolean enabled;

    /** True when the windows are the recommended defaults, set by no one. */
    public final boolean defaulted;

    /** Namespace the windows cover. The purges are not queue-scoped. */
    public final String namespace;

    /** The {@code now} the snapshot was taken at, in Unix milliseconds. */
    public final long referenceTime;

    /** Per-table windows the counts were computed against, in milliseconds. */
    public final Windows windows;

    /** Rows each table's purge would remove. */
    public final Counts counts;

    /** Total rows a purge would delete across every table. */
    public final long total;

    /**
     * Decoded from the core's JSON dry-run view.
     *
     * @param enabled false when no table has a window — only per-entry TTLs would be swept
     * @param defaulted true when the windows are the recommended defaults, set by no one
     * @param namespace the namespace the windows cover
     * @param referenceTime the {@code now} the snapshot was taken at, in Unix milliseconds
     * @param windows the windows the counts were computed against, or {@code null} for none
     * @param counts the per-table counts, or {@code null} for all zero
     * @param total total rows a purge would delete across every table
     */
    @JsonCreator
    public RetentionPreview(
            @JsonProperty("enabled") boolean enabled,
            @JsonProperty("defaulted") boolean defaulted,
            @JsonProperty("namespace") String namespace,
            @JsonProperty("reference_time") long referenceTime,
            @JsonProperty("windows") @Nullable Windows windows,
            @JsonProperty("counts") @Nullable Counts counts,
            @JsonProperty("total") long total) {
        this.enabled = enabled;
        this.defaulted = defaulted;
        this.namespace = namespace;
        this.referenceTime = referenceTime;
        this.windows = windows == null ? Windows.EMPTY : windows;
        this.counts = counts == null ? Counts.EMPTY : counts;
        this.total = total;
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

        /**
         * The windows the preview was computed against.
         *
         * @param archivedJobsMs how long terminal jobs are kept, or {@code null} to keep
         *     them forever
         * @param deadLetterMs how long dead-letter entries are kept, or {@code null} forever
         * @param taskLogsMs how long task logs are kept, or {@code null} forever
         * @param taskMetricsMs how long task metrics are kept, or {@code null} forever
         * @param jobErrorsMs how long per-attempt job errors are kept, or {@code null} forever
         */
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

    /** Rows each history table's purge would remove. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Counts {

        static final Counts EMPTY = new Counts(0, 0, 0, 0, 0);

        /** Terminal jobs that would be purged. */
        public final long archivedJobs;

        /** Dead-letter entries that would be purged. */
        public final long deadLetter;

        /** Task-log lines that would be purged. */
        public final long taskLogs;

        /** Task-metric rows that would be purged. */
        public final long taskMetrics;

        /** Per-attempt job-error rows that would be purged. */
        public final long jobErrors;

        /**
         * The rows each table's purge would remove.
         *
         * @param archivedJobs terminal jobs that would be purged
         * @param deadLetter dead-letter entries that would be purged
         * @param taskLogs task-log lines that would be purged
         * @param taskMetrics task-metric rows that would be purged
         * @param jobErrors per-attempt job-error rows that would be purged
         */
        @JsonCreator
        public Counts(
                @JsonProperty("archived_jobs") long archivedJobs,
                @JsonProperty("dead_letter") long deadLetter,
                @JsonProperty("task_logs") long taskLogs,
                @JsonProperty("task_metrics") long taskMetrics,
                @JsonProperty("job_errors") long jobErrors) {
            this.archivedJobs = archivedJobs;
            this.deadLetter = deadLetter;
            this.taskLogs = taskLogs;
            this.taskMetrics = taskMetrics;
            this.jobErrors = jobErrors;
        }
    }
}
