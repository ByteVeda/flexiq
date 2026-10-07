//! What a recorded call acted on.

/// What kind of thing a call acted on. Stored as its [`Self::as_str`] form,
/// which is the `target_kind` a listing filters on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetKind {
    /// A job id.
    Job,
    /// A workflow run id.
    WorkflowRun,
    /// A queue name.
    Queue,
    /// A dead-letter entry id.
    DeadLetter,
    /// A worker id.
    Worker,
    /// A periodic task name.
    Periodic,
    /// A task name.
    Task,
    /// A namespace, for what acts on the tenant as a whole — its quota.
    Namespace,
}

impl TargetKind {
    /// The stored spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::WorkflowRun => "workflow_run",
            Self::Queue => "queue",
            Self::DeadLetter => "dead_letter",
            Self::Worker => "worker",
            Self::Periodic => "periodic",
            Self::Task => "task",
            Self::Namespace => "namespace",
        }
    }
}
