//! `fq tail` — follow jobs, or a queue, over `WatchJobs`.
//!
//! A dropped connection is the ordinary case for a command left running on a
//! laptop, so `tail` reconnects on its own. What it resumes from is the wire's
//! rule: an id watch reopens with the ids still unfinished, and the server's
//! snapshot fills in anything that happened meanwhile; a queue watch reopens
//! from the last cursor, and if the server no longer holds it, says so and
//! carries on from now.

use std::time::Duration;

use anyhow::{anyhow, Result};
use tonic::{Code, Status};
use tonic_types::StatusExt as _;

use crate::cli::TailArgs;
use crate::connect::Client;
use crate::output::watch::{watch_json, watch_line};
use crate::pb::{self, watch_jobs_request, watch_jobs_response::Item};
use crate::{error, safe};

/// The first wait before reconnecting; doubles per failure.
const BACKOFF_START: Duration = Duration::from_secs(1);

/// The longest wait between reconnects.
const BACKOFF_CAP: Duration = Duration::from_secs(30);

/// What is still being followed, and where a queue watch left off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// Ids not yet finished, in the order given.
    Ids(Vec<String>),
    /// A queue, and the last cursor seen on it.
    Queue {
        /// The queue.
        queue: String,
        /// Empty before the first item.
        cursor: String,
    },
}

impl Target {
    /// What the flags ask to follow.
    pub fn from_args(args: &TailArgs) -> Self {
        match &args.queue {
            Some(queue) => Self::Queue {
                queue: queue.clone(),
                cursor: String::new(),
            },
            None => Self::Ids(args.ids.clone()),
        }
    }

    /// The request that (re)opens the watch.
    pub fn request(&self) -> pb::WatchJobsRequest {
        match self {
            Self::Ids(ids) => pb::WatchJobsRequest {
                target: Some(watch_jobs_request::Target::JobIds(pb::WatchJobIds {
                    job_ids: ids.clone(),
                })),
                resume_cursor: String::new(),
            },
            Self::Queue { queue, cursor } => pb::WatchJobsRequest {
                target: Some(watch_jobs_request::Target::Queue(queue.clone())),
                resume_cursor: cursor.clone(),
            },
        }
    }

    /// Record one item: a finished id stops being followed, and a queue
    /// watch moves its cursor on.
    pub fn observe(&mut self, response: &pb::WatchJobsResponse) {
        match self {
            Self::Ids(ids) => {
                let finished = match &response.item {
                    Some(Item::Transition(t)) if t.terminal => Some(&t.job_id),
                    Some(Item::NotFoundJobId(id)) => Some(id),
                    _ => None,
                };
                if let Some(finished) = finished {
                    ids.retain(|id| id != finished);
                }
            }
            Self::Queue { cursor, .. } => {
                if !response.cursor.is_empty() {
                    cursor.clone_from(&response.cursor);
                }
            }
        }
    }

    /// Nothing left to follow.
    pub fn finished(&self) -> bool {
        matches!(self, Self::Ids(ids) if ids.is_empty())
    }
}

/// What to do about a stream that ended with `status`.
#[derive(Debug, PartialEq, Eq)]
pub enum Recovery {
    /// Reopen from where the target stands.
    Reconnect,
    /// The cursor is gone: reopen a queue watch from now.
    FromNow,
    /// Give up.
    Fail,
}

/// Classify a failure. Reopening is always safe — the RPC writes nothing —
/// so anything that says "try again" is retried.
///
/// `WATCH_LIMIT` is fatal only until a watch has opened: after a dropped
/// connection the server may still hold the old stream's slot until it notices,
/// so the cap refusing a reopen is transient.
pub fn recovery(status: &Status, opened_before: bool) -> Recovery {
    let reason = status
        .get_error_details()
        .error_info()
        .map(|info| info.reason.clone())
        .unwrap_or_default();
    match reason.as_str() {
        "WATCH_CURSOR_EXPIRED" => Recovery::FromNow,
        "WATCH_OVERFLOW" | "SHUTTING_DOWN" => Recovery::Reconnect,
        "WATCH_LIMIT" if opened_before => Recovery::Reconnect,
        _ if status.code() == Code::Unavailable => Recovery::Reconnect,
        _ => Recovery::Fail,
    }
}

/// `fq tail`.
pub async fn run(client: &mut Client, args: &TailArgs, json: bool) -> Result<()> {
    let mut target = Target::from_args(args);
    let mut backoff = BACKOFF_START;
    let mut opened = false;
    loop {
        let ended = follow(client, &mut target, json, &mut backoff, &mut opened).await;
        let status = match ended {
            Ok(()) if target.finished() => return Ok(()),
            // A queue watch that ends with OK is a server ending it early;
            // an id watch that ends with ids left is the same.
            Ok(()) => None,
            Err(status) => Some(status),
        };
        let decided = status
            .as_ref()
            .map_or(Recovery::Reconnect, |status| recovery(status, opened));
        match decided {
            Recovery::Fail => {
                let described = status.as_ref().map(error::describe).unwrap_or_default();
                return Err(anyhow!("{described}"));
            }
            Recovery::FromNow => {
                eprintln!("fq: missed transitions while disconnected; following from now");
                if let Target::Queue { cursor, .. } = &mut target {
                    cursor.clear();
                }
            }
            Recovery::Reconnect => {
                if let Some(status) = &status {
                    eprintln!(
                        "fq: stream lost ({}); reconnecting in {}s",
                        error::describe(status),
                        backoff.as_secs()
                    );
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(BACKOFF_CAP);
            }
        }
    }
}

/// Open one stream and print it to its end. A delivered item resets the
/// backoff: the connection works again. `opened` records that a watch was
/// accepted at least once.
async fn follow(
    client: &mut Client,
    target: &mut Target,
    json: bool,
    backoff: &mut Duration,
    opened: &mut bool,
) -> Result<(), Status> {
    let mut stream = client.watch_jobs(target.request()).await?.into_inner();
    *opened = true;
    while let Some(response) = stream.message().await? {
        *backoff = BACKOFF_START;
        print(&response, json);
        target.observe(&response);
    }
    Ok(())
}

fn print(response: &pb::WatchJobsResponse, json: bool) {
    if json {
        println!("{}", watch_json(response));
    } else if let Some(line) = watch_line(response) {
        println!("{line}");
    } else {
        // An arm this build does not know: say so rather than print nothing.
        println!("{}", safe::escape(&format!("{response:?}")));
    }
}

#[cfg(test)]
mod tests {
    use tonic_types::{ErrorDetails, StatusExt};

    use super::*;

    fn args(ids: &[&str], queue: Option<&str>) -> TailArgs {
        TailArgs {
            ids: ids.iter().map(|id| id.to_string()).collect(),
            queue: queue.map(str::to_string),
        }
    }

    fn transition(id: &str, terminal: bool) -> pb::WatchJobsResponse {
        pb::WatchJobsResponse {
            item: Some(Item::Transition(pb::JobTransition {
                job_id: id.into(),
                terminal,
                ..Default::default()
            })),
            cursor: String::new(),
        }
    }

    fn refused(code: Code, reason: &str) -> Status {
        Status::with_error_details(
            code,
            "refused",
            ErrorDetails::with_error_info(reason, "flexiq.byteveda.org", []),
        )
    }

    #[test]
    fn an_id_watch_forgets_each_id_once_it_is_finished() {
        let mut target = Target::from_args(&args(&["a", "b", "c"], None));
        target.observe(&transition("a", false));
        target.observe(&transition("b", true));
        target.observe(&pb::WatchJobsResponse {
            item: Some(Item::NotFoundJobId("c".into())),
            cursor: String::new(),
        });
        assert_eq!(target, Target::Ids(vec!["a".into()]));
        assert!(!target.finished());
        let reopened = target.request();
        assert_eq!(
            reopened.target,
            Some(watch_jobs_request::Target::JobIds(pb::WatchJobIds {
                job_ids: vec!["a".into()]
            }))
        );
        target.observe(&transition("a", true));
        assert!(target.finished());
    }

    #[test]
    fn a_queue_watch_resumes_from_its_last_cursor() {
        let mut target = Target::from_args(&args(&[], Some("orders")));
        assert_eq!(target.request().resume_cursor, "");
        let mut item = transition("a", true);
        item.cursor = "c7".into();
        target.observe(&item);
        assert_eq!(target.request().resume_cursor, "c7");
        assert!(!target.finished(), "a queue watch never finishes");
    }

    #[test]
    fn a_retryable_end_reconnects_and_anything_else_fails() {
        for opened in [false, true] {
            assert_eq!(
                recovery(
                    &refused(Code::FailedPrecondition, "WATCH_CURSOR_EXPIRED"),
                    opened
                ),
                Recovery::FromNow
            );
            assert_eq!(
                recovery(&refused(Code::ResourceExhausted, "WATCH_OVERFLOW"), opened),
                Recovery::Reconnect
            );
            assert_eq!(
                recovery(&refused(Code::Unavailable, "SHUTTING_DOWN"), opened),
                Recovery::Reconnect
            );
            assert_eq!(
                recovery(&Status::unavailable("gone"), opened),
                Recovery::Reconnect
            );
            assert_eq!(
                recovery(&refused(Code::Unauthenticated, "UNAUTHENTICATED"), opened),
                Recovery::Fail
            );
        }
    }

    /// The cap refusing the first open means the credential really is at it;
    /// refusing a reopen can be the dropped stream's slot not yet released.
    #[test]
    fn the_watch_cap_is_fatal_only_before_a_watch_has_opened() {
        let limit = refused(Code::ResourceExhausted, "WATCH_LIMIT");
        assert_eq!(recovery(&limit, false), Recovery::Fail);
        assert_eq!(recovery(&limit, true), Recovery::Reconnect);
    }
}
