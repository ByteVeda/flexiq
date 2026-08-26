package org.byteveda.flexiq.events;

import java.util.HashMap;
import java.util.Map;
import org.byteveda.flexiq.errors.SerializationException;

/**
 * The cross-SDK event taxonomy, each constant carrying its dotted wire name
 * (e.g. {@code job.completed}). Most events are emitted by this runtime; a few
 * are reserved contract entries not yet emitted here (noted per constant). The
 * first four constants are the terminal job outcomes; their ordinals are stable
 * for compatibility with persisted data.
 */
public enum EventName {
    /** A job finished successfully ({@code job.completed}). */
    SUCCESS("job.completed"),
    /** A job failed and was rescheduled ({@code job.retrying}). */
    RETRY("job.retrying"),
    /** A job exhausted its retries and was dead-lettered ({@code job.dead}). */
    DEAD("job.dead"),
    /** A job was cancelled ({@code job.cancelled}). */
    CANCELLED("job.cancelled"),
    /**
     * An attempt ended in a durable step sleep and the job is pending until its
     * deadline ({@code job.sleeping}). Carries a
     * {@link org.byteveda.flexiq.events.SleepEvent}; subscribe with
     * {@code onEvent}, not {@code on}, since it is not a terminal outcome.
     */
    JOB_SLEEPING("job.sleeping"),
    /** A job was accepted into a queue ({@code job.enqueued}). */
    JOB_ENQUEUED("job.enqueued"),
    /** A handler threw, before the retry/dead-letter decision ({@code job.failed}). */
    JOB_FAILED("job.failed"),
    /** A worker is starting, before its native loop runs ({@code worker.started}). */
    WORKER_STARTED("worker.started"),
    /** A worker's native loop is running and dispatching ({@code worker.online}). */
    WORKER_ONLINE("worker.online"),
    /** A worker stopped dispatching new jobs ({@code worker.stopped}). */
    WORKER_STOPPED("worker.stopped"),
    /** A worker fully shut down and released its resources ({@code worker.offline}). */
    WORKER_OFFLINE("worker.offline"),
    /**
     * A worker reported an unhealthy state ({@code worker.unhealthy}). Reserved:
     * part of the cross-SDK contract but not yet emitted by this SDK.
     */
    WORKER_UNHEALTHY("worker.unhealthy"),
    /** A named queue was paused ({@code queue.paused}). */
    QUEUE_PAUSED("queue.paused"),
    /** A named queue was resumed ({@code queue.resumed}). */
    QUEUE_RESUMED("queue.resumed"),
    /** A workflow run was submitted ({@code workflow.submitted}). */
    WORKFLOW_SUBMITTED("workflow.submitted"),
    /** A workflow run completed with every node succeeding ({@code workflow.completed}). */
    WORKFLOW_COMPLETED("workflow.completed"),
    /**
     * A workflow run finished with some failed nodes
     * ({@code workflow.completed_with_failures}). Reserved: part of the
     * cross-SDK contract; this runtime's finalizer reports completed/failed today.
     */
    WORKFLOW_COMPLETED_WITH_FAILURES("workflow.completed_with_failures"),
    /** A workflow run failed ({@code workflow.failed}). */
    WORKFLOW_FAILED("workflow.failed"),
    /** A workflow run was cancelled ({@code workflow.cancelled}). */
    WORKFLOW_CANCELLED("workflow.cancelled"),
    /** An approval gate parked, awaiting resolution ({@code workflow.gate_reached}). */
    WORKFLOW_GATE_REACHED("workflow.gate_reached"),
    /** A failed run began rolling back its completed steps ({@code workflow.compensating}). */
    WORKFLOW_COMPENSATING("workflow.compensating"),
    /** A run's rollback finished cleanly ({@code workflow.compensated}). */
    WORKFLOW_COMPENSATED("workflow.compensated"),
    /** A run's rollback itself failed ({@code workflow.compensation_failed}). */
    WORKFLOW_COMPENSATION_FAILED("workflow.compensation_failed"),
    /** One node's compensation job was enqueued ({@code workflow.node_compensating}). */
    WORKFLOW_NODE_COMPENSATING("workflow.node_compensating"),
    /** One node's compensation job succeeded ({@code workflow.node_compensated}). */
    WORKFLOW_NODE_COMPENSATED("workflow.node_compensated"),
    /** One node's compensation job failed ({@code workflow.node_compensation_failed}). */
    WORKFLOW_NODE_COMPENSATION_FAILED("workflow.node_compensation_failed"),
    /** An enqueue was rejected by a predicate or gate ({@code predicate.rejected}). */
    PREDICATE_REJECTED("predicate.rejected"),
    /**
     * A predicate held work back for a delay ({@code predicate.deferred}) — at
     * enqueue, or at dispatch, where the payload also carries the job's id.
     * Reserved: this SDK's predicates are pass/reject only.
     */
    PREDICATE_DEFERRED("predicate.deferred"),
    /**
     * An enqueue was dropped by a predicate without raising
     * ({@code predicate.skipped}). Reserved: this SDK's predicates are
     * pass/reject only.
     */
    PREDICATE_SKIPPED("predicate.skipped"),
    /**
     * A dispatch-time predicate cancelled an already-enqueued job
     * ({@code predicate.cancelled}). Reserved: this SDK gates only at enqueue.
     */
    PREDICATE_CANCELLED("predicate.cancelled");

    /**
     * Wire names (+ the pre-taxonomy outcome aliases) to constants. The legacy
     * alias table lives only here — every other reader normalizes via
     * {@link #fromWire}.
     */
    private static final Map<String, EventName> BY_WIRE = new HashMap<>();

    static {
        for (EventName name : values()) {
            BY_WIRE.put(name.wireName, name);
        }
        BY_WIRE.put("success", SUCCESS);
        BY_WIRE.put("retry", RETRY);
        BY_WIRE.put("dead", DEAD);
        BY_WIRE.put("cancelled", CANCELLED);
    }

    private final String wireName;

    EventName(String wireName) {
        this.wireName = wireName;
    }

    /** The dotted cross-SDK wire name, e.g. {@code job.completed}. */
    public String wireName() {
        return wireName;
    }

    /** Whether events under this name are terminal-or-attempt job outcomes carrying an {@link OutcomeEvent}. */
    public boolean isJobOutcome() {
        return this == SUCCESS || this == RETRY || this == DEAD || this == CANCELLED || this == JOB_FAILED;
    }

    /**
     * Guard for {@link OutcomeEvent}-typed subscription surfaces.
     *
     * @return this name
     * @throws IllegalArgumentException when {@link #isJobOutcome()} is false
     */
    public EventName requireJobOutcome() {
        if (!isJobOutcome()) {
            throw new IllegalArgumentException(
                    this + " does not deliver an OutcomeEvent; subscribe via onEvent(name, listener)");
        }
        return this;
    }

    /**
     * Map a wire name back to its constant. Accepts every {@link #wireName()}
     * plus the legacy outcome aliases {@code success}/{@code retry}/{@code dead}/
     * {@code cancelled} that predate the dotted names.
     *
     * @throws SerializationException on an unknown name
     */
    public static EventName fromWire(String wire) {
        EventName name = BY_WIRE.get(wire);
        if (name == null) {
            throw new SerializationException("unknown event name: " + wire);
        }
        return name;
    }

    /** Map a native outcome kind ("success"/"retry"/"dead"/"cancelled"). */
    public static EventName fromKind(String kind) {
        switch (kind) {
            case "success":
                return SUCCESS;
            case "retry":
                return RETRY;
            case "dead":
                return DEAD;
            case "cancelled":
                return CANCELLED;
            default:
                throw new SerializationException("unknown outcome kind: " + kind);
        }
    }
}
