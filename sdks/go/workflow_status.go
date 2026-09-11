package flexiq

import "strconv"

// WorkflowState is a run's lifecycle state.
//
// The zero value is [WorkflowStateUnspecified], which a server never sends: it
// is what a state this build has no name for decodes to.
type WorkflowState int32

const (
	// WorkflowStateUnspecified is the zero value, which a server never sends.
	// See [WorkflowState.IsKnown].
	WorkflowStateUnspecified WorkflowState = 0
	// WorkflowStatePending means submitted, nothing started yet.
	WorkflowStatePending WorkflowState = 1
	// WorkflowStateRunning means at least one node is in flight or eligible.
	WorkflowStateRunning WorkflowState = 2
	// WorkflowStatePaused means the run is held, waiting to be resumed.
	WorkflowStatePaused WorkflowState = 3
	// WorkflowStateCompleted means every node succeeded.
	WorkflowStateCompleted WorkflowState = 4
	// WorkflowStateCompletedWithFailures means every node reached a terminal
	// state, at least one of them a failure, and the run was built to continue
	// rather than fail fast.
	WorkflowStateCompletedWithFailures WorkflowState = 5
	// WorkflowStateFailed means the run stopped on a failure.
	WorkflowStateFailed WorkflowState = 6
	// WorkflowStateCancelled means the run was cancelled.
	WorkflowStateCancelled WorkflowState = 7
	// WorkflowStateCompensating means a saga rollback is in flight.
	WorkflowStateCompensating WorkflowState = 8
	// WorkflowStateCompensated means the rollback finished successfully.
	WorkflowStateCompensated WorkflowState = 9
	// WorkflowStateCompensationFailed means the rollback itself failed.
	WorkflowStateCompensationFailed WorkflowState = 10
)

// IsTerminal reports whether the run reached a state it will not leave.
//
// A state this build does not recognise is never terminal, [JobStatus.IsTerminal]'s
// rule and the wire's own instruction: a newer server may grow one, and reading
// an unknown state as finished would have a poller stop watching a run that is
// still going. Compensating is not terminal — a rollback is still work.
func (s WorkflowState) IsTerminal() bool {
	switch s {
	case WorkflowStateCompleted, WorkflowStateCompletedWithFailures,
		WorkflowStateFailed, WorkflowStateCancelled,
		WorkflowStateCompensated, WorkflowStateCompensationFailed:
		return true
	default:
		return false
	}
}

// IsKnown reports whether the state is one this build can reason about.
// [WorkflowStateUnspecified] is not; see [JobStatus.IsKnown].
func (s WorkflowState) IsKnown() bool {
	switch s {
	case WorkflowStatePending, WorkflowStateRunning, WorkflowStatePaused,
		WorkflowStateCompleted, WorkflowStateCompletedWithFailures,
		WorkflowStateFailed, WorkflowStateCancelled,
		WorkflowStateCompensating, WorkflowStateCompensated,
		WorkflowStateCompensationFailed:
		return true
	default:
		return false
	}
}

func (s WorkflowState) String() string {
	switch s {
	case WorkflowStateUnspecified:
		return nameUnspecified
	case WorkflowStatePending:
		return namePending
	case WorkflowStateRunning:
		return nameRunning
	case WorkflowStatePaused:
		return "PAUSED"
	case WorkflowStateCompleted:
		return nameCompleted
	case WorkflowStateCompletedWithFailures:
		return "COMPLETED_WITH_FAILURES"
	case WorkflowStateFailed:
		return nameFailed
	case WorkflowStateCancelled:
		return nameCancelled
	case WorkflowStateCompensating:
		return nameCompensating
	case WorkflowStateCompensated:
		return nameCompensated
	case WorkflowStateCompensationFailed:
		return nameCompensationFailed
	default:
		// A state from a newer server. Naming the number is more use than
		// "unknown" when it turns up in a log.
		return "WorkflowState(" + strconv.FormatInt(int64(s), 10) + ")"
	}
}

// WorkflowNodeStatus is one node's state within a run.
//
// The numbers are the wire's, and they have a hole: 2 was READY until 2.0.0
// removed it, because readiness is a predicate over the graph rather than a
// stored status — a runnable node reports [WorkflowNodeStatusPending]. They are
// written out one by one here for that reason, never derived.
type WorkflowNodeStatus int32

const (
	// WorkflowNodeStatusUnspecified is the zero value, which a server never
	// sends. See [WorkflowNodeStatus.IsKnown].
	WorkflowNodeStatusUnspecified WorkflowNodeStatus = 0
	// WorkflowNodeStatusPending means not yet running. A node that is eligible
	// right now reports this too.
	WorkflowNodeStatusPending WorkflowNodeStatus = 1
	// WorkflowNodeStatusRunning means the node's job is executing.
	WorkflowNodeStatusRunning WorkflowNodeStatus = 3
	// WorkflowNodeStatusCompleted means the node's job succeeded.
	WorkflowNodeStatusCompleted WorkflowNodeStatus = 4
	// WorkflowNodeStatusFailed means the node's job failed for good.
	WorkflowNodeStatusFailed WorkflowNodeStatus = 5
	// WorkflowNodeStatusSkipped means an edge condition ruled the node out.
	WorkflowNodeStatusSkipped WorkflowNodeStatus = 6
	// WorkflowNodeStatusWaitingApproval means a gate is holding the node.
	WorkflowNodeStatusWaitingApproval WorkflowNodeStatus = 7
	// WorkflowNodeStatusCacheHit means an earlier run's result was reused and
	// no job was enqueued.
	WorkflowNodeStatusCacheHit WorkflowNodeStatus = 8
	// WorkflowNodeStatusCompensating means the node's rollback is in flight.
	WorkflowNodeStatusCompensating WorkflowNodeStatus = 9
	// WorkflowNodeStatusCompensated means the node's rollback succeeded.
	WorkflowNodeStatusCompensated WorkflowNodeStatus = 10
	// WorkflowNodeStatusCompensationFailed means the node's rollback failed.
	WorkflowNodeStatusCompensationFailed WorkflowNodeStatus = 11
)

// IsTerminal reports whether the node reached a state it will not leave.
// An unrecognised status is never terminal, for [WorkflowState.IsTerminal]'s
// reason.
func (s WorkflowNodeStatus) IsTerminal() bool {
	switch s {
	case WorkflowNodeStatusCompleted, WorkflowNodeStatusFailed,
		WorkflowNodeStatusSkipped, WorkflowNodeStatusCacheHit,
		WorkflowNodeStatusCompensated, WorkflowNodeStatusCompensationFailed:
		return true
	default:
		return false
	}
}

// IsKnown reports whether the status is one this build can reason about.
// [WorkflowNodeStatusUnspecified] is not, and neither is 2 — see the type's own
// comment.
func (s WorkflowNodeStatus) IsKnown() bool {
	switch s {
	case WorkflowNodeStatusPending, WorkflowNodeStatusRunning,
		WorkflowNodeStatusCompleted, WorkflowNodeStatusFailed,
		WorkflowNodeStatusSkipped, WorkflowNodeStatusWaitingApproval,
		WorkflowNodeStatusCacheHit, WorkflowNodeStatusCompensating,
		WorkflowNodeStatusCompensated, WorkflowNodeStatusCompensationFailed:
		return true
	default:
		return false
	}
}

func (s WorkflowNodeStatus) String() string {
	switch s {
	case WorkflowNodeStatusUnspecified:
		return nameUnspecified
	case WorkflowNodeStatusPending:
		return namePending
	case WorkflowNodeStatusRunning:
		return nameRunning
	case WorkflowNodeStatusCompleted:
		return nameCompleted
	case WorkflowNodeStatusFailed:
		return nameFailed
	case WorkflowNodeStatusSkipped:
		return "SKIPPED"
	case WorkflowNodeStatusWaitingApproval:
		return "WAITING_APPROVAL"
	case WorkflowNodeStatusCacheHit:
		return "CACHE_HIT"
	case WorkflowNodeStatusCompensating:
		return nameCompensating
	case WorkflowNodeStatusCompensated:
		return nameCompensated
	case WorkflowNodeStatusCompensationFailed:
		return nameCompensationFailed
	default:
		return "WorkflowNodeStatus(" + strconv.FormatInt(int64(s), 10) + ")"
	}
}
