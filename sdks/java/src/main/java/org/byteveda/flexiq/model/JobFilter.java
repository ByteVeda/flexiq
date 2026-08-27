package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/** Immutable filter for {@link org.byteveda.flexiq.FlexiQ#listJobs(JobFilter)}. Unset fields are ignored. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class JobFilter {
    @JsonProperty("status")
    private final @Nullable String status;

    @JsonProperty("queue")
    private final @Nullable String queue;

    @JsonProperty("task")
    private final @Nullable String task;

    @JsonProperty("limit")
    private final @Nullable Integer limit;

    @JsonProperty("offset")
    private final @Nullable Integer offset;

    private JobFilter(Builder b) {
        this.status = b.status;
        this.queue = b.queue;
        this.task = b.task;
        this.limit = b.limit;
        this.offset = b.offset;
    }

    /**
     * No filtering: every job, under the backend's default page size.
     *
     * @return an empty filter
     */
    public static JobFilter all() {
        return builder().build();
    }

    /**
     * A builder with nothing set.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Collects the criteria for one listing. */
    public static final class Builder {
        /** An empty builder; reach it through {@link JobFilter#builder()}. */
        public Builder() {}

        private @Nullable String status;
        private @Nullable String queue;
        private @Nullable String task;
        private @Nullable Integer limit;
        private @Nullable Integer offset;

        /**
         * Keep only jobs in this state.
         *
         * @param status the state to match; sent as its lowercase wire form
         * @return {@code this}, for chaining
         */
        public Builder status(JobStatus status) {
            this.status = status.wire();
            return this;
        }

        /**
         * Keep only jobs on this queue.
         *
         * @param queue the queue name
         * @return {@code this}, for chaining
         */
        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        /**
         * Keep only jobs of this task.
         *
         * @param task the task's registered name
         * @return {@code this}, for chaining
         */
        public Builder task(String task) {
            this.task = task;
            return this;
        }

        /**
         * Cap the page size.
         *
         * @param limit the most rows to return
         * @return {@code this}, for chaining
         */
        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        /**
         * Skip rows before the page.
         *
         * @param offset how many matching rows to skip; prefer keyset paging for a
         *     deep walk
         * @return {@code this}, for chaining
         */
        public Builder offset(int offset) {
            this.offset = offset;
            return this;
        }

        /**
         * Freeze the criteria collected so far.
         *
         * @return the immutable filter
         */
        public JobFilter build() {
            return new JobFilter(this);
        }
    }
}
