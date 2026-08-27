package org.byteveda.flexiq.workflows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.byteveda.flexiq.task.Task;
import org.jspecify.annotations.Nullable;

/** One step in a {@link Workflow}: a task plus its payload, predecessors, and per-step overrides. */
public final class Step {
    /** This step's name within its workflow, and how predecessors refer to it. */
    public final String name;

    /** The task the step runs. */
    public final String taskName;

    /** The payload the task is called with, or {@code null} when it is derived at runtime. */
    public final @Nullable Object payload;

    /** Names of the steps that must finish first; empty for a root. */
    public final List<String> after;

    /** The queue this step's job goes to, or {@code null} for the task's own default. */
    public final @Nullable String queue;

    /** Retry ceiling for this step, or {@code null} for the task's own default. */
    public final @Nullable Integer maxRetries;

    /** Per-attempt timeout for this step, or {@code null} for the task's own default. */
    public final @Nullable Long timeoutMs;

    /** Dispatch priority for this step, or {@code null} for the task's own default. */
    public final @Nullable Integer priority;

    /** Fan-out strategy, or {@code null} when the step runs once. */
    public final @Nullable String fanOut;

    /** Fan-in strategy, or {@code null} when the step takes a single predecessor result. */
    public final @Nullable String fanIn;

    /** Approval gate parking this step, or {@code null} when it runs unattended. */
    public final @Nullable GateConfig gate;

    /** The wire condition gating this step, or {@code null} for the default. */
    public final @Nullable String condition;

    /** The predicate behind a {@code callable} condition, or {@code null}. */
    public final @Nullable Condition callableCondition;

    /** The child workflow this step submits instead of running a task, or {@code null}. */
    public final @Nullable Workflow subWorkflow;

    /** The rollback task the saga runs for this step, or {@code null} when it is not compensated. */
    public final @Nullable String compensate;

    /** How long a cache hit stays valid, in milliseconds, or {@code null} when the step is not cached. */
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

    /**
     * Begin a step bound to a typed task.
     *
     * @param name this step's name within its workflow
     * @param task the task to run
     * @param payload the argument, type-checked against the task
     * @param <T> the task's payload type
     * @return the builder
     */
    public static <T> Builder of(String name, Task<T> task, T payload) {
        return new Builder(name, task.name(), payload);
    }

    /**
     * Begin a step bound to a task name (untyped payload).
     *
     * @param name this step's name within its workflow
     * @param taskName the registered task to run
     * @param payload the argument, or {@code null} for a task that takes none
     * @return the builder
     */
    public static Builder of(String name, String taskName, @Nullable Object payload) {
        return new Builder(name, taskName, payload);
    }

    /**
     * Begin a payload-less step (its payload is derived at runtime — fan-out/fan-in).
     *
     * @param name this step's name within its workflow
     * @param task the task to run
     * @return the builder
     */
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

        /**
         * Predecessor step names that must finish before this step runs.
         *
         * @param predecessors the steps that must settle first; called more than once, they accumulate
         * @return {@code this}, for chaining
         */
        public Builder after(String... predecessors) {
            this.after.addAll(Arrays.asList(predecessors));
            return this;
        }

        /**
         * Send this step's job to a queue other than the task's default.
         *
         * @param queue the queue name
         * @return {@code this}, for chaining
         */
        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        /**
         * Override the task's retry ceiling for this step.
         *
         * @param maxRetries how many attempts this step's job gets
         * @return {@code this}, for chaining
         */
        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        /**
         * Override the task's per-attempt timeout for this step.
         *
         * @param timeoutMs the timeout in milliseconds
         * @return {@code this}, for chaining
         */
        public Builder timeoutMs(long timeoutMs) {
            this.timeoutMs = timeoutMs;
            return this;
        }

        /**
         * Override the task's dispatch priority for this step.
         *
         * @param priority the priority; higher runs first within a queue
         * @return {@code this}, for chaining
         */
        public Builder priority(int priority) {
            this.priority = priority;
            return this;
        }

        /**
         * Run this step once per item of its predecessor's result (strategy {@code "each"}).
         *
         * @param strategy the wire strategy name
         * @return {@code this}, for chaining
         */
        public Builder fanOut(String strategy) {
            this.fanOut = strategy;
            return this;
        }

        /**
         * Run this step once per predecessor item using a {@link FanMode}.
         *
         * @param mode the strategy
         * @return {@code this}, for chaining
         */
        public Builder fanOut(FanMode mode) {
            return fanOut(mode.wire());
        }

        /**
         * Collect a fan-out predecessor's child results into one list (strategy {@code "all"}).
         *
         * @param strategy the wire strategy name
         * @return {@code this}, for chaining
         */
        public Builder fanIn(String strategy) {
            this.fanIn = strategy;
            return this;
        }

        /**
         * Collect a fan-out predecessor's results using a {@link FanMode}.
         *
         * @param mode the strategy
         * @return {@code this}, for chaining
         */
        public Builder fanIn(FanMode mode) {
            return fanIn(mode.wire());
        }

        /**
         * Park this step for approval before it runs. The node waits until
         * {@code Worker.approveGate}/{@code rejectGate}, or until the gate's
         * timeout elapses.
         *
         * @param gate who must approve, and how long the node waits
         * @return {@code this}, for chaining
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
         *
         * @param condition {@code "on_success"}, {@code "on_failure"}, {@code "always"},
         *     or {@code null} for the default
         * @return {@code this}, for chaining
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
         *
         * @param condition when to run this step, or {@code null} for the default
         * @return {@code this}, for chaining
         */
        public Builder condition(@Nullable WorkflowCondition condition) {
            this.condition = condition == null ? null : condition.wire();
            return this;
        }

        /**
         * Run this step only if every predecessor completed (the default).
         *
         * @return {@code this}, for chaining
         */
        public Builder onSuccess() {
            return condition("on_success");
        }

        /**
         * Run this step only if a predecessor failed (a recovery branch).
         *
         * @return {@code this}, for chaining
         */
        public Builder onFailure() {
            return condition("on_failure");
        }

        /**
         * Run this step once predecessors settle, regardless of their outcome.
         *
         * @return {@code this}, for chaining
         */
        public Builder always() {
            return condition("always");
        }

        /**
         * Run this step only when {@code predicate} holds. The predicate is code,
         * so the workflow must be registered on the running worker via
         * {@code trackWorkflows(workflow)}.
         *
         * @param predicate decides at run time whether this step runs
         * @return {@code this}, for chaining
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
         *
         * @param child the workflow submitted as a child run
         * @return {@code this}, for chaining
         */
        public Builder subWorkflow(Workflow child) {
            this.subWorkflow = child;
            return this;
        }

        /**
         * Register a rollback task for this step. If the run later fails, the saga
         * runs {@code compensateTask} (with this step's result as its payload) to
         * compensate it, rolling back completed steps in reverse order.
         *
         * @param compensateTask the task that rolls this step back, called with this
         *     step's result
         * @return {@code this}, for chaining
         */
        public Builder compensate(String compensateTask) {
            this.compensate = compensateTask;
            return this;
        }

        /**
         * Register a typed rollback task; see {@link #compensate(String)}.
         *
         * @param compensateTask the task that rolls this step back
         * @return {@code this}, for chaining
         */
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
         *
         * @param ttl how long a hit stays valid; must be positive
         * @return {@code this}, for chaining
         */
        public Builder cache(java.time.Duration ttl) {
            if (ttl == null || ttl.isNegative() || ttl.isZero()) {
                throw new IllegalArgumentException("cache ttl must be positive");
            }
            this.cacheTtlMs = ttl.toMillis();
            return this;
        }

        /**
         * Freeze the step.
         *
         * @return the immutable step
         */
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
