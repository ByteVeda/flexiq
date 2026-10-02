//! What a narrowed `execute` credential lets an executor run (#988).
//!
//! Checked twice, for two different reasons. At attach, every task the `hello`
//! declares must be one the grants reach on some queue — the loud refusal, so a
//! misconfigured executor learns at once rather than idling. On every dispatch,
//! the job's queue and task must be reached — the enforcement, because a `hello`
//! names no queue and the dispatcher is what picks one.

use flexiq_core::Admission;
use tonic::{Request, Status};

use crate::grpc::auth::principal::{Access, Principal, Scope};
use crate::grpc::status::WireError;

/// A narrowed `execute` access, as the dispatcher's placement filter.
#[derive(Debug)]
pub struct ExecuteGrant(Access);

impl ExecuteGrant {
    /// The limit `principal` carries behind the executor door, or `None` when
    /// it reaches every queue and task and placement needs no filter.
    pub fn of(principal: &Principal) -> Option<Self> {
        match principal.access() {
            Some(Access::Whole) => None,
            Some(access) => Some(Self(access.clone())),
            // No door fixed reaches nothing: a filter that admits nothing,
            // never a missing one that admits everything.
            None => Some(Self(Access::Narrowed(Vec::new()))),
        }
    }

    /// The first declared task these grants reach on no queue. One is enough
    /// to refuse the whole `hello` — it is never trimmed to what is granted.
    pub fn first_refused<'a>(&self, tasks: &'a [String]) -> Option<&'a str> {
        tasks
            .iter()
            .find(|task| !self.0.reaches_task_somewhere(task))
            .map(String::as_str)
    }
}

impl Admission for ExecuteGrant {
    fn admits(&self, queue: &str, task: &str) -> bool {
        self.0.reaches(Some(queue), Some(task))
    }
}

/// The refusal for a `hello` declaring `task` beyond the caller's grants.
pub fn refused_task(task: &str) -> Status {
    WireError::beyond_grant(Scope::Execute.as_str(), None, Some(task)).into()
}

/// The caller the auth layer established.
pub fn principal<T>(request: &Request<T>) -> Result<&Principal, Status> {
    request.extensions().get::<Principal>().ok_or_else(|| {
        // Only reachable with the service registered without the auth layer;
        // refusing beats guessing what the caller may run.
        log::error!(
            "grpc: an executor request carried no principal; the service is \
             registered without the auth layer"
        );
        Status::from(WireError::internal())
    })
}

/// Refuse a narrowed caller on an RPC that names a lease, not a task.
///
/// The reporting RPCs know a job id and nothing about its queue or task without
/// a storage read, so — like every producer method not taught to check — they
/// are closed to a narrowed credential rather than left unchecked.
pub fn require_whole<T>(request: &Request<T>) -> Result<(), Status> {
    if principal(request)?.reaches_everything() {
        Ok(())
    } else {
        Err(WireError::beyond_grant(Scope::Execute.as_str(), None, None).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::grant::Grants;

    fn grant(spelled: &[&str]) -> Option<ExecuteGrant> {
        let grants = Grants::parse_all(spelled.iter().copied()).expect("valid");
        ExecuteGrant::of(&Principal::new("id", "ns", grants).behind(Scope::Execute))
    }

    fn tasks(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn a_whole_grant_needs_no_filter() {
        assert!(grant(&["execute"]).is_none());
    }

    #[test]
    fn a_hello_is_refused_on_its_first_ungranted_task() {
        let grant = grant(&["execute:task=send_*"]).expect("narrowed");
        assert_eq!(grant.first_refused(&tasks(&["send_receipt"])), None);
        assert_eq!(
            grant.first_refused(&tasks(&["send_receipt", "charge", "refund"])),
            Some("charge")
        );
    }

    #[test]
    fn placement_admits_only_the_granted_queue_and_task() {
        let grant = grant(&["execute:queue=emails,task=send_*"]).expect("narrowed");
        assert!(grant.admits("emails", "send_receipt"));
        assert!(!grant.admits("billing", "send_receipt"));
        assert!(!grant.admits("emails", "charge"));
    }

    fn request_from(spelled: &[&str]) -> Request<()> {
        let grants = Grants::parse_all(spelled.iter().copied()).expect("valid");
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(Principal::new("id", "ns", grants).behind(Scope::Execute));
        request
    }

    /// The reporting RPCs name a lease, not a task, so a narrowed credential
    /// is refused there rather than left unchecked.
    #[test]
    fn a_reporting_rpc_admits_whole_execute_grants_only() {
        assert!(require_whole(&request_from(&["execute"])).is_ok());
        let refused = require_whole(&request_from(&["execute:task=send_*"]))
            .expect_err("a narrowed credential must be refused");
        assert_eq!(refused.code(), tonic::Code::PermissionDenied);
        assert!(require_whole(&Request::new(())).is_err(), "no principal");
    }

    #[test]
    fn a_principal_no_door_was_fixed_for_admits_nothing() {
        let grants = Grants::parse_all(["execute"]).expect("valid");
        let grant = ExecuteGrant::of(&Principal::new("id", "ns", grants)).expect("a filter");
        assert!(!grant.admits("emails", "send_receipt"));
        assert_eq!(grant.first_refused(&tasks(&["x"])), Some("x"));
    }
}
