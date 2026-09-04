//! The Prometheus exposition both listeners publish.
//!
//! This process runs no user code, so it has no in-process client registry to
//! proxy the way an SDK dashboard does. What it can see is queue depth, the
//! worker registry, and attached executor capacity — and those numbers are the
//! same whichever door asks for them.
//!
//! The dashboard's `/metrics` and the gRPC door's `/metrics` therefore render
//! through this module rather than each building their own text. They differ
//! only in how they are gated: the dashboard by
//! `FLEXIQ_DASHBOARD_METRICS_TOKEN` or a session, the gRPC door by any valid
//! scoped API token. A deployment that enables only the gRPC role has no
//! dashboard to scrape, which is why the second door exists at all.

use std::collections::HashMap;

use flexiq_core::{Capacity, QueueStats};

/// The content type a Prometheus scraper expects.
pub const EXPOSITION_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Escape a Prometheus label value: backslash, quote, and newline are the
/// three characters the exposition format reserves.
pub fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// The gauges derived from storage, in a stable order.
///
/// `capacity` is absent when this process attaches no executors; the two
/// executor gauges are then omitted rather than published as zero, because
/// "no executors attached" and "this process is not the one they attach to"
/// are different answers and a dashboard reading zero cannot tell them apart.
pub fn storage_gauges(
    per_queue: HashMap<String, QueueStats>,
    workers: usize,
    capacity: Option<Capacity>,
) -> String {
    let mut body = String::new();
    body.push_str("# HELP flexiq_jobs Jobs by queue and status.\n");
    body.push_str("# TYPE flexiq_jobs gauge\n");
    // Sorted, so a diff between two scrapes is about the numbers.
    let mut queues: Vec<_> = per_queue.into_iter().collect();
    queues.sort_by(|(left, _), (right, _)| left.cmp(right));
    for (queue, stats) in queues {
        for (status, count) in [
            ("pending", stats.pending),
            ("running", stats.running),
            ("completed", stats.completed),
            ("failed", stats.failed),
            ("dead", stats.dead),
            ("cancelled", stats.cancelled),
        ] {
            body.push_str(&format!(
                "flexiq_jobs{{queue=\"{}\",status=\"{status}\"}} {count}\n",
                escape_label(&queue)
            ));
        }
    }

    body.push_str("# HELP flexiq_workers Workers in the cluster registry.\n");
    body.push_str("# TYPE flexiq_workers gauge\n");
    body.push_str(&format!("flexiq_workers {workers}\n"));

    if let Some(capacity) = capacity {
        body.push_str("# HELP flexiq_executors Executors attached to this scheduler.\n");
        body.push_str("# TYPE flexiq_executors gauge\n");
        body.push_str(&format!("flexiq_executors {}\n", capacity.executors));
        body.push_str("# HELP flexiq_executor_slots Execution slots advertised by executors.\n");
        body.push_str("# TYPE flexiq_executor_slots gauge\n");
        body.push_str(&format!(
            "flexiq_executor_slots{{state=\"total\"}} {}\n",
            capacity.total_slots
        ));
        body.push_str(&format!(
            "flexiq_executor_slots{{state=\"free\"}} {}\n",
            capacity.free_slots
        ));
    }

    body
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats(pending: i64) -> QueueStats {
        QueueStats {
            pending,
            ..QueueStats::default()
        }
    }

    #[test]
    fn queues_are_emitted_in_name_order() {
        let body = storage_gauges(
            HashMap::from([
                ("beta".to_string(), stats(2)),
                ("alpha".to_string(), stats(1)),
            ]),
            0,
            None,
        );
        let alpha = body.find("queue=\"alpha\"").expect("alpha is emitted");
        let beta = body.find("queue=\"beta\"").expect("beta is emitted");
        assert!(alpha < beta, "queues must sort by name:\n{body}");
    }

    /// A queue name is operator-supplied, so it reaches a label value unescaped
    /// unless something escapes it — and one quote would make the whole scrape
    /// unparseable, not just that line.
    #[test]
    fn a_queue_name_cannot_break_out_of_its_label() {
        let body = storage_gauges(
            HashMap::from([("od\"d\\one".to_string(), stats(1))]),
            0,
            None,
        );
        assert!(
            body.contains("queue=\"od\\\"d\\\\one\""),
            "unexpected exposition:\n{body}"
        );
    }

    /// Zero executors and "this process attaches none" are different answers.
    #[test]
    fn the_executor_gauges_are_absent_without_a_dispatcher() {
        let without = storage_gauges(HashMap::new(), 0, None);
        assert!(!without.contains("flexiq_executors"));

        let with = storage_gauges(
            HashMap::new(),
            0,
            Some(Capacity {
                executors: 0,
                total_slots: 0,
                free_slots: 0,
            }),
        );
        assert!(with.contains("flexiq_executors 0"));
    }
}
