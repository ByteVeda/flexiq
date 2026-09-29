//! The per-call slot the audit layer, the auth layer and a handler share.
//!
//! The layer can see a call's path and its answer, but not who made it (the
//! auth layer decides that, inside it) nor what it touched (only a handler has
//! the decoded message). So the layer puts one [`AuditContext`] in the
//! request's extensions, the auth layer names the caller in it, a handler
//! names the targets, and the layer reads it back once the answer is in.

use std::sync::{Arc, Mutex, PoisonError};

use crate::grpc::auth::Principal;

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
        }
    }
}

#[derive(Debug, Default)]
struct Slot {
    principal: Option<Principal>,
    targets: Vec<(TargetKind, String)>,
}

/// One audited call's shared slot. Cloning shares it.
#[derive(Debug, Clone, Default)]
pub struct AuditContext(Arc<Mutex<Slot>>);

impl AuditContext {
    /// The slot a request carries, if the audit layer put one there — it does
    /// only for the calls it audits.
    pub fn of(extensions: &http::Extensions) -> Option<Self> {
        extensions.get::<Self>().cloned()
    }

    /// Name the caller. Called by the auth layer as soon as a credential is
    /// believed, before the scope check, so a refused call is still attributed.
    pub fn identify(&self, principal: &Principal) {
        self.lock().principal = Some(principal.clone());
    }

    /// Record one thing the call acted on.
    pub fn target(&self, kind: TargetKind, id: impl Into<String>) {
        self.lock().targets.push((kind, id.into()));
    }

    /// The caller and the targets, emptying the slot.
    pub(crate) fn take(&self) -> (Option<Principal>, Vec<(TargetKind, String)>) {
        let mut slot = self.lock();
        (slot.principal.take(), std::mem::take(&mut slot.targets))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Slot> {
        // A poisoned slot means a handler panicked mid-push; what it holds is
        // still worth recording, so the poison is stepped over.
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Record a target on `audit`. A call the layer does not audit carries no
/// slot, and this is then a no-op.
pub fn target(audit: Option<&AuditContext>, kind: TargetKind, id: impl Into<String>) {
    if let Some(audit) = audit {
        audit.target(kind, id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::auth::ScopeSet;

    #[test]
    fn clones_share_one_slot() {
        let context = AuditContext::default();
        let handler_side = context.clone();
        context.identify(&Principal::new("tok", "prod", ScopeSet::ALL));
        handler_side.target(TargetKind::Job, "j1");
        handler_side.target(TargetKind::Job, "j2");

        let (principal, targets) = context.take();
        assert_eq!(
            principal.map(|p| p.credential().to_string()).as_deref(),
            Some("tok")
        );
        assert_eq!(
            targets,
            [
                (TargetKind::Job, "j1".into()),
                (TargetKind::Job, "j2".into())
            ]
        );
        assert!(context.take().1.is_empty(), "take empties the slot");
    }

    #[test]
    fn a_request_without_a_slot_records_nowhere() {
        let extensions = http::Extensions::new();
        let audit = AuditContext::of(&extensions);
        assert!(audit.is_none());
        target(audit.as_ref(), TargetKind::Queue, "emails");
    }
}
