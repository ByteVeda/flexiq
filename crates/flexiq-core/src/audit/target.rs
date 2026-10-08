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
    /// A gRPC token's public id.
    Token,
    /// A webhook subscription id.
    Webhook,
    /// One delivery of a webhook.
    WebhookDelivery,
    /// A dashboard setting's key.
    Setting,
    /// A pub/sub topic name.
    Topic,
    /// A pub/sub subscription name, within its topic.
    Subscription,
    /// A task middleware's name.
    Middleware,
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
            Self::Token => "token",
            Self::Webhook => "webhook",
            Self::WebhookDelivery => "webhook_delivery",
            Self::Setting => "setting",
            Self::Topic => "topic",
            Self::Subscription => "subscription",
            Self::Middleware => "middleware",
        }
    }
}
