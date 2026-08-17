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

    public static JobFilter all() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private @Nullable String status;
        private @Nullable String queue;
        private @Nullable String task;
        private @Nullable Integer limit;
        private @Nullable Integer offset;

        /** Lowercase wire status: pending/running/complete/failed/dead/cancelled. */
        public Builder status(JobStatus status) {
            this.status = status.wire();
            return this;
        }

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder task(String task) {
            this.task = task;
            return this;
        }

        public Builder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public Builder offset(int offset) {
            this.offset = offset;
            return this;
        }

        public JobFilter build() {
            return new JobFilter(this);
        }
    }
}
