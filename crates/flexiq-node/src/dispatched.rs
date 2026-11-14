//! The jobs a worker has handed to JS and not yet settled.
//!
//! Classifying an outcome for the workflow tracker used to cost two job reads
//! on the JS thread, where one round trip stalls every in-flight job (#962).
//! The dispatcher records each job here as it runs it, so the result loop
//! attaches the workflow link to the outcome instead.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use crate::convert::JsWorkflowLink;

/// Parse a job's metadata blob into the workflow node it runs for, if any.
pub(crate) fn parse_workflow_link(metadata: Option<&str>) -> Option<JsWorkflowLink> {
    let metadata = metadata?;
    // Nearly every job is not a workflow node; a substring scan rejects those
    // for less than a JSON parse costs on the dispatch path.
    if !metadata.contains("workflow_run_id") {
        return None;
    }
    let parsed: serde_json::Value = serde_json::from_str(metadata).ok()?;
    Some(JsWorkflowLink {
        run_id: parsed.get("workflow_run_id")?.as_str()?.to_string(),
        node_name: parsed.get("workflow_node_name")?.as_str()?.to_string(),
        compensation: parsed
            .get("compensation")
            .and_then(serde_json::Value::as_bool)
            == Some(true),
    })
}

/// Dispatched-but-unsettled jobs, keyed by id, with their workflow link.
///
/// Every result the drain loop sees comes from a job the dispatcher ran, so an
/// entry recorded at dispatch is always there to settle when its result lands.
#[derive(Default)]
pub(crate) struct Dispatched(Mutex<HashMap<String, Option<JsWorkflowLink>>>);

impl Dispatched {
    /// Record a job as it is handed to JS.
    pub fn record(&self, job_id: &str, metadata: Option<&str>) {
        let link = parse_workflow_link(metadata);
        self.lock().insert(job_id.to_string(), link);
    }

    /// Forget a job whose result has landed, handing back its workflow link.
    pub fn settle(&self, job_id: &str) -> Option<JsWorkflowLink> {
        self.lock().remove(job_id).flatten()
    }

    // The map holds no invariant a panicking holder could break mid-write.
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Option<JsWorkflowLink>>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINKED: &str = r#"{"workflow_run_id":"r","workflow_node_name":"n"}"#;

    #[test]
    fn parses_forward_and_compensation_links() {
        let forward = parse_workflow_link(Some(
            r#"{"workflow_run_id":"r1","workflow_node_name":"a","traceparent":"x"}"#,
        ))
        .expect("forward link");
        assert_eq!(
            (forward.run_id.as_str(), forward.node_name.as_str()),
            ("r1", "a")
        );
        assert!(!forward.compensation);

        let rollback = parse_workflow_link(Some(
            r#"{"workflow_run_id":"r1","workflow_node_name":"a","compensation":true}"#,
        ))
        .expect("compensation link");
        assert!(rollback.compensation);
    }

    #[test]
    fn plain_and_malformed_metadata_have_no_link() {
        assert!(parse_workflow_link(None).is_none());
        assert!(parse_workflow_link(Some(r#"{"traceparent":"x"}"#)).is_none());
        assert!(parse_workflow_link(Some(r#"{"workflow_run_id":"r1"}"#)).is_none());
        assert!(parse_workflow_link(Some("workflow_run_id not json")).is_none());
    }

    #[test]
    fn settle_hands_back_the_link_once() {
        let dispatched = Dispatched::default();
        dispatched.record("j1", Some(LINKED));
        dispatched.record("j2", None);
        assert!(dispatched.settle("j2").is_none());
        assert!(dispatched.settle("j1").is_some());
        assert!(dispatched.settle("j1").is_none());
    }
}
