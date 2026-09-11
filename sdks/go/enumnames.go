package flexiq

// The names more than one of this package's enums prints.
//
// A job, a workflow run and a node in that run are three different things with
// one vocabulary: all three are pending before they start and running while
// they work, and reading "RUNNING" off any of them means the same thing. They
// are constants rather than a literal per switch arm so that stays true by
// construction — a reworded state is reworded everywhere it is the same state,
// and nowhere it is not.
//
// A name only one enum uses stays a literal in that enum's String, where it is
// read beside the value it describes.
const (
	nameUnspecified        = "UNSPECIFIED"
	namePending            = "PENDING"
	nameRunning            = "RUNNING"
	nameCompleted          = "COMPLETED"
	nameFailed             = "FAILED"
	nameCancelled          = "CANCELLED"
	nameCompensating       = "COMPENSATING"
	nameCompensated        = "COMPENSATED"
	nameCompensationFailed = "COMPENSATION_FAILED"
)
