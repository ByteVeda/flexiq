package org.byteveda.flexiq.workflows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.byteveda.flexiq.task.Task;

/**
 * A workflow definition: a named, versioned DAG of {@link Step}s. Build one, then
 * submit it with {@code FlexiQ.submitWorkflow}. Steps run in topological order;
 * a step waits for every predecessor named in its {@code after} list.
 */
public final class Workflow {
    private final String name;
    private int version = 1;
    private final List<Step> steps = new ArrayList<>();

    private Workflow(String name) {
        this.name = name;
    }

    /**
     * Start a workflow named {@code name} (version defaults to 1).
     *
     * @param name the definition's name, which runs are grouped under
     * @return an empty definition
     */
    public static Workflow named(String name) {
        return new Workflow(name);
    }

    /**
     * Set the definition version. Bumping it lets the DAG change without conflict.
     *
     * @param version the version live runs of the old shape keep referring to
     * @return {@code this}, for chaining
     */
    public Workflow version(int version) {
        this.version = version;
        return this;
    }

    /**
     * Add a step that runs after the named predecessors, using the task's defaults.
     *
     * @param name the step's name within this workflow
     * @param task the task to run
     * @param payload the argument, type-checked against the task
     * @param after the steps that must settle first; none for a root
     * @param <T> the task's payload type
     * @return {@code this}, for chaining
     */
    public <T> Workflow step(String name, Task<T> task, T payload, String... after) {
        return step(Step.of(name, task, payload).after(after).build());
    }

    /**
     * Add a structural step (payload supplied at submit via
     * {@code submitWorkflow(wf, payloads)}) that runs after {@code deps}. For a
     * job priority, use the {@link Step} builder: {@code step(Step.of(name,
     * task).priority(p).after(deps).build())}.
     *
     * @param name the step's name within this workflow
     * @param task the task to run
     * @param deps the steps that must settle first; none for a root
     * @return {@code this}, for chaining
     */
    public Workflow stepAfter(String name, Task<?> task, String... deps) {
        return step(Step.of(name, task).after(deps).build());
    }

    /**
     * Add a fully-configured step.
     *
     * @param step the step, built with {@link Step#of}
     * @return {@code this}, for chaining
     */
    public Workflow step(Step step) {
        steps.add(step);
        return this;
    }

    /**
     * Add a fan-out step: {@code task} runs once per item of its predecessor's
     * result (a list). The predecessor named in {@code after} is the producer.
     *
     * @param name the step's name within this workflow
     * @param task the task run once per item
     * @param strategy the wire strategy name
     * @param after exactly one predecessor — the producer whose result is split
     * @param <T> the task's payload type
     * @return {@code this}, for chaining
     */
    public <T> Workflow fanOut(String name, Task<T> task, String strategy, String... after) {
        requireSinglePredecessor("fan-out", name, after);
        return step(Step.of(name, task).fanOut(strategy).after(after).build());
    }

    /**
     * Fan-out with a {@link FanMode} (typically {@link FanMode#EACH}).
     *
     * @param name the step's name within this workflow
     * @param task the task run once per item
     * @param mode the strategy
     * @param after exactly one predecessor — the producer whose result is split
     * @param <T> the task's payload type
     * @return {@code this}, for chaining
     */
    public <T> Workflow fanOut(String name, Task<T> task, FanMode mode, String... after) {
        return fanOut(name, task, mode.wire(), after);
    }

    /**
     * Add a fan-in step that collects its fan-out predecessor's child results
     * into one list and passes it to {@code task}.
     *
     * @param name the step's name within this workflow
     * @param task the task the collected list is passed to
     * @param strategy the wire strategy name
     * @param after exactly one predecessor — the fan-out whose children are collected
     * @param <T> the task's payload type
     * @return {@code this}, for chaining
     */
    public <T> Workflow fanIn(String name, Task<T> task, String strategy, String... after) {
        requireSinglePredecessor("fan-in", name, after);
        return step(Step.of(name, task).fanIn(strategy).after(after).build());
    }

    /**
     * Fan-in with a {@link FanMode} (typically {@link FanMode#ALL}).
     *
     * @param name the step's name within this workflow
     * @param task the task the collected list is passed to
     * @param mode the strategy
     * @param after exactly one predecessor — the fan-out whose children are collected
     * @param <T> the task's payload type
     * @return {@code this}, for chaining
     */
    public <T> Workflow fanIn(String name, Task<T> task, FanMode mode, String... after) {
        return fanIn(name, task, mode.wire(), after);
    }

    /**
     * Add an approval gate that runs after {@code after}. The gate is a control
     * node — it runs no task; it parks until {@code Worker.approveGate}/
     * {@code rejectGate} (or its timeout) resolves it, then its successors run.
     * The running worker must {@code trackWorkflows(this)} so its tracker holds
     * the downstream steps' payloads.
     *
     * @param name the gate's name within this workflow
     * @param gate who must approve, and how long the node waits
     * @param after the steps that must settle first; none for a root
     * @return {@code this}, for chaining
     */
    public Workflow gate(String name, GateConfig gate, String... after) {
        return step(Step.of(name, GATE_TASK, null).gate(gate).after(after).build());
    }

    /** Sentinel task name for gate control nodes (never enqueued). */
    static final String GATE_TASK = "__gate__";

    /**
     * Add a sub-workflow step: when reached it submits {@code child} as a child
     * run and completes when the child finalizes (failing if the child fails).
     * The running worker must {@code trackWorkflows(this)}.
     *
     * @param name the step's name within this workflow
     * @param child the workflow submitted as a child run
     * @param after the steps that must settle first; none for a root
     * @return {@code this}, for chaining
     */
    public Workflow subWorkflow(String name, Workflow child, String... after) {
        return step(Step.of(name, SUB_WORKFLOW_TASK, null)
                .subWorkflow(child)
                .after(after)
                .build());
    }

    /** Sentinel task name for sub-workflow control nodes (never enqueued). */
    static final String SUB_WORKFLOW_TASK = "__subworkflow__";

    // A fan-out/fan-in node has exactly one runtime trigger — its single producer.
    // Zero predecessors would never enqueue; multiple could fire from the wrong one.
    private static void requireSinglePredecessor(String kind, String name, String[] after) {
        if (after.length != 1) {
            throw new IllegalArgumentException(
                    kind + " step '" + name + "' needs exactly one predecessor, got " + after.length);
        }
    }

    /**
     * The definition's name.
     *
     * @return the name runs are grouped under
     */
    public String name() {
        return name;
    }

    /**
     * The definition's version.
     *
     * @return the version, 1 unless set
     */
    public int version() {
        return version;
    }

    /**
     * The steps added so far.
     *
     * @return the steps, in declaration order, unmodifiable
     */
    public List<Step> steps() {
        return Collections.unmodifiableList(steps);
    }
}
