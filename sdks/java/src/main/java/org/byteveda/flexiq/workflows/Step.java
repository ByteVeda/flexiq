package org.byteveda.flexiq.workflows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.byteveda.flexiq.task.Task;
import org.jspecify.annotations.Nullable;

/** One step in a {@link Workflow}: a task plus its payload, predecessors, and per-step overrides. */
public final class Step {
    public final String name;
    public final String taskName;
    public final @Nullable Object payload;
    public final List<String> after;
    public final @Nullable String queue;
    public final @Nullable Integer maxRetries;
    public final @Nullable Long timeoutMs;
    public final @Nullable Integer priority;
    public final @Nullable String fanOut;
    public final @Nullable String fanIn;
    public final @Nullable GateConfig gate;
    public final @Nullable String condition;
    public final @Nullable Condition callableCondition;
    public final @Nullable Workflow subWorkflow;
    public final @Nullable String compensate;
    public final @Nullable Long cacheTtlMs;

    private Step(Builder builder) {
        this.name = builder.name;
        this.taskName = builder.taskName;
        this.payload = builder.payload;
        this.after = Collections.unmodifiableList(new ArrayList<>(builder.after));
        this.queue = builder.queue;
        this.maxRetries = builder.maxRetries;
        this.timeoutMs = builder.timeoutMs;
        this.priority = builder.priority;
        this.fanOut = builder.fanOut;
        this.fanIn = builder.fanIn;
        this.gate = builder.gate;
        this.condition = builder.condition;
        this.callableCondition = builder.callableCondition;
        this.subWorkflow = builder.subWorkflow;
        this.compensate = builder.compensate;
        this.cacheTtlMs = builder.cacheTtlMs;
    }

    /** Begin a step bound to a typed task. */
    public static <T> Builder of(String name, Task<T> task, T payload) {
        return new Builder(name, task.name(), payload);
    }

    /** Begin a step bound to a task name (untyped payload). */
    public static Builder of(String name, String taskName, @Nullable Object payload) {
        return new Builder(name, taskName, payload);
    }

    /** Begin a payload-less step (its payload is derived at runtime — fan-out/fan-in). */
    public static Builder of(String name, Task<?> task) {
        return new Builder(name, task.name(), null);
    }

    /** Fluent builder for a {@link Step}. */
    public static final class Builder {
        private final String name;
        private final String taskName;
        private final @Nullable Object payload;
        private final List<String> after = new ArrayList<>();
        private @Nullable String queue;
        private @Nullable Integer maxRetries;
        private @Nullable Long timeoutMs;
        private @Nullable Integer priority;
        private @Nullable String fanOut;
        private @Nullable String fanIn;
        private @Nullable GateConfig gate;
        private @Nullable String condition;
        private @Nullable Condition callableCondition;
        private @Nullable Workflow subWorkflow;
        private @Nullable String compensate;
        private @Nullable Long cacheTtlMs;

        private Builder(String name, String taskName, @Nullable Object payload) {
            this.name = name;
            this.taskName = taskName;
            this.payload = payload;
        }

        /** Predecessor step names that must finish before this step runs. */
        public Builder after(String... predecessors) {
            this.after.addAll(Arrays.asList(predecessors));
            return this;
        }

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder timeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /** Run this step once per item of its predecessor's result (strategy {@code "each"}). */
        public Builder fanOut(String strategy) {
            this.fanOut = strategy;
            return this;
        }

        /** Run this step once per predecessor item using a {@link FanMode}. */
        public Builder fanOut(FanMode mode) {
            return fanOut(mode.wire());
        }

        /** Collect a fan-out predecessor's child results into one list (strategy {@code "all"}). */
        public Builder fanIn(String strategy) {
            this.fanIn = strategy;
            return this;
        }

        /** Collect a fan-out predecessor's results using a {@link FanMode}. */
        public Builder fanIn(FanMode mode) {
            return fanIn(mode.wire());
        }

        /**
         * Park this step for approval before it runs. The node waits until
         * {@code Worker.approveGate}/{@code rejectGate}, or until the gate's
         * timeout elapses.
         */
        public Builder gate(GateConfig gate) {
            this.gate = gate;
            return this;
        }

        /**
         * Run this step only when {@code condition} holds: {@code "on_success"}
         * (every predecessor completed — the default), {@code "on_failure"} (any
         * predecessor failed), or {@code "always"} (once predecessors settle). A
         * conditional step is evaluated by the worker tracker, not pre-enqueued.
         */
        public Builder condition(String condition) {
            if (condition != null
                    && !"on_success".equals(condition)
                    && !"on_failure".equals(condition)
                    && !"always".equals(condition)) {
                throw new IllegalArgumentException("unknown condition '" + condition
                        + "'; use on_success, on_failure, always, a WorkflowCondition,"
                        + " or condition(Condition)");
            }
            this.condition = condition;
            return this;
        }

        /**
         * Type-safe variant of {@link #condition(String)} — run this step only when
         * {@code condition} holds. Prefer this over the string overload.
         */
        public Builder condition(@Nullable WorkflowCondition condition) {
            this.condition = condition == null ? null : condition.wire();
            return this;
        }

        /** Run this step only if every predecessor completed (the default). */
        public Builder onSuccess() {
            return condition("on_success");
        }

        /** Run this step only if a predecessor failed (a recovery branch). */
        public Builder onFailure() {
            return condition("on_failure");
        }

        /** Run this step once predecessors settle, regardless of their outcome. */
        public Builder always() {
            return condition("always");
        }

        /**
         * Run this step only when {@code predicate} holds. The predicate is code,
         * so the workflow must be registered on the running worker via
         * {@code trackWorkflows(workflow)}.
         */
        public Builder condition(Condition predicate) {
            this.callableCondition = predicate;
            this.condition = "callable";
            return this;
        }

        /**
         * Make this step a sub-workflow: instead of running a task it submits
         * {@code child} as a child run and completes when the child finalizes
         * (failing if the child fails). The running worker must
         * {@code trackWorkflows(parent)}.
         */
        public Builder subWorkflow(Workflow child) {
            this.subWorkflow = child;
            return this;
        }

        /**
         * Register a rollback task for this step. If the run later fails, the saga
         * runs {@code compensateTask} (with this step's result as its payload) to
         * compensate it, rolling back completed steps in reverse order.
         */
        public Builder compensate(String compensateTask) {
            this.compensate = compensateTask;
            return this;
        }

        /** Register a typed rollback task; see {@link #compensate(String)}. */
        public Builder compensate(Task<?> compensateTask) {
            return compensate(compensateTask.name());
        }

        /**
         * Cache this step's execution for {@code ttl}: on a later run of the same
         * workflow, if this step's task + payload are unchanged and within the TTL,
         * the worker marks it a cache hit and skips re-running it. (Cache state is
         * per worker process.)
         *
         * <p>A cache hit does not produce a forward result, so a cached step's result
         * is not available downstream: it cannot feed a fan-out/fan-in (rejected at
         * submit) and a callable {@code condition} won't see its result.
         */
        public Builder cache(java.time.Duration ttl) {
            if (ttl == null || ttl.isNegative() || ttl.isZero()) {
                throw new IllegalArgumentException("cache ttl must be positive");
            }
            this.cacheTtlMs = ttl.toMillis();
            return this;
        }

        public Step build() {
            // A cacheable step is a deferred node; deferred roots are never promoted,
            // so a cacheable step with no predecessor would wedge the run in PENDING.
            if (cacheTtlMs != null && after.isEmpty()) {
                throw new IllegalArgumentException("step '" + name
                        + "' cannot be cacheable without a predecessor; a deferred root is never promoted");
            }
            if (fanOut != null && fanIn != null) {
                throw new IllegalArgumentException("step '" + name + "' cannot be both fan-out and fan-in");
            }
            if (gate != null && (fanOut != null || fanIn != null)) {
                throw new IllegalArgumentException("step '" + name + "' cannot be both a gate and a fan-out/fan-in");
            }
            // A gate is a deferred control node (its task never enqueues), valid only
            // on the Workflow.gate(...) sentinel — not on a normal task step.
            if (gate != null && !Workflow.GATE_TASK.equals(taskName)) {
                throw new IllegalArgumentException("step '" + name
                        + "': a gate may only be created via Workflow.gate(...); a gate on a normal task step "
                        + "would defer the node and never enqueue its task");
            }
            if (subWorkflow != null && (fanOut != null || fanIn != null || gate != null)) {
                throw new IllegalArgumentException(
                        "step '" + name + "' cannot be both a sub-workflow and a gate/fan-out/fan-in");
            }
            // Compensation replays the step's forward result as the rollback payload.
            // Gate, sub-workflow, and fan-out nodes never run a task of their own
            // (so they have no forward result) — reject a compensator on them.
            if (compensate != null && (gate != null || subWorkflow != null || fanOut != null)) {
                throw new IllegalArgumentException("step '" + name
                        + "' cannot be compensated: a gate/sub-workflow/fan-out node has no forward result");
            }
            return new Step(this);
        }
    }
}
