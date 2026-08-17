package org.byteveda.flexiq.worker;

import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Per-table retention windows for auto-cleanup, in seconds. An unset field
 * keeps that table forever. A job or DLQ entry can still carry its own
 * per-entry {@code resultTtl}, honored independently of these windows.
 */
public final class Retention {

    private final @Nullable Integer archivedJobs;
    private final @Nullable Integer deadLetter;
    private final @Nullable Integer taskLogs;
    private final @Nullable Integer taskMetrics;
    private final @Nullable Integer jobErrors;

    private Retention(Builder builder) {
        this.archivedJobs = builder.archivedJobs;
        this.deadLetter = builder.deadLetter;
        this.taskLogs = builder.taskLogs;
        this.taskMetrics = builder.taskMetrics;
        this.jobErrors = builder.jobErrors;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The set windows as a camelCase map for the native layer (worker options
     *  and the retention dry-run). */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<>();
        if (archivedJobs != null) {
            map.put("archivedJobs", archivedJobs);
        }
        if (deadLetter != null) {
            map.put("deadLetter", deadLetter);
        }
        if (taskLogs != null) {
            map.put("taskLogs", taskLogs);
        }
        if (taskMetrics != null) {
            map.put("taskMetrics", taskMetrics);
        }
        if (jobErrors != null) {
            map.put("jobErrors", jobErrors);
        }
        return map;
    }

    /** Fluent builder; every window is optional. */
    public static final class Builder {
        private @Nullable Integer archivedJobs;
        private @Nullable Integer deadLetter;
        private @Nullable Integer taskLogs;
        private @Nullable Integer taskMetrics;
        private @Nullable Integer jobErrors;

        // A negative window would invert the cleanup cutoff into the future and
        // match every row, so reject it at the boundary. Zero is valid.
        private static int nonNegative(int seconds, String window) {
            if (seconds < 0) {
                throw new IllegalArgumentException("retention window '" + window + "' must be non-negative");
            }
            return seconds;
        }

        /** Terminal jobs, every terminal status — the artifact read after completion. */
        public Builder archivedJobs(int seconds) {
            this.archivedJobs = nonNegative(seconds, "archivedJobs");
            return this;
        }

        /** Dead-letter entries — the only copy of a payload a human must act on. */
        public Builder deadLetter(int seconds) {
            this.deadLetter = nonNegative(seconds, "deadLetter");
            return this;
        }

        /** Task logs — highest write volume, lowest per-row value. */
        public Builder taskLogs(int seconds) {
            this.taskLogs = nonNegative(seconds, "taskLogs");
            return this;
        }

        /** Task metrics — feeds the dashboard charts. */
        public Builder taskMetrics(int seconds) {
            this.taskMetrics = nonNegative(seconds, "taskMetrics");
            return this;
        }

        /** Per-attempt job errors. */
        public Builder jobErrors(int seconds) {
            this.jobErrors = nonNegative(seconds, "jobErrors");
            return this;
        }

        public Retention build() {
            return new Retention(this);
        }
    }
}
