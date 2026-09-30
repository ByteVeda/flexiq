//! `WatchJobs` over Server-Sent Events: the one stream the facade serves.
//!
//! ```text
//! curl -N http://localhost:50051/v1/jobs:watch?jobIds=01924f \
//!   -H "authorization: Bearer $FLEXIQ_TOKEN"
//! ```
//!
//! It calls the same `ProducerService::watch_jobs` the gRPC door does, so the
//! bounds, the per-credential cap and the scope are that RPC's. What differs is
//! the framing:
//!
//! - one `data` event per `WatchJobsResponse`, its proto3 JSON, with the
//!   cursor as the event `id` when there is one — so a browser's `EventSource`
//!   resumes a queue watch through `Last-Event-ID` with no code of its own;
//! - a refusal before the stream opens is the facade's ordinary JSON error, and
//!   a failure after it is a final `error` event carrying the same body;
//! - a stream that finishes cleanly ends with an `end` event, because
//!   `EventSource` reconnects after *any* close and would otherwise reopen a
//!   finished id watch forever;
//! - a `: keepalive` comment every [`KEEPALIVE`], for proxies with idle
//!   timeouts.
//!
//! It is routed by hand rather than through [`ROUTES`](super::routes::ROUTES):
//! a server stream has no `google.api.http` mapping the OpenAPI generator or
//! the route drift tests understand, and this path is the documented exception.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use http::request::Parts;
use http::HeaderValue;
use tokio_stream::Stream;
use tonic::Status;

use super::error;
use super::json::response as write;
use super::routes::scoped;
use crate::grpc::pb::producer_service_server::ProducerService;
use crate::grpc::pb::{self, watch_jobs_request::Target};
use crate::grpc::producer::watch::stream::Outlet;
use crate::grpc::producer::Producer;
use crate::grpc::status::WireError;

/// The path a client types. A literal colon, which matchit registers as is.
pub const PATH: &str = "/v1/jobs:watch";

/// How often an idle stream sends a comment, under the common 30 s and 60 s
/// proxy idle timeouts.
pub const KEEPALIVE: Duration = Duration::from_secs(15);

/// The header `EventSource` resends on reconnect, holding the last event `id`.
const LAST_EVENT_ID: &str = "last-event-id";

/// `GET /v1/jobs:watch`.
pub async fn watch(State(producer): State<Producer>, parts: Parts) -> Response {
    let request = match prepare(&parts) {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    match producer.watch_jobs(request).await {
        Ok(response) => stream(response.into_inner()),
        Err(status) => error::response(&status),
    }
}

fn prepare(parts: &Parts) -> Result<tonic::Request<pb::WatchJobsRequest>, WireError> {
    let last_event_id = parts
        .headers
        .get(LAST_EVENT_ID)
        .map(HeaderValue::to_str)
        .transpose()
        .map_err(|_| WireError::invalid_request("`Last-Event-ID` is not a cursor"))?;
    let message = read(parts.uri.query().unwrap_or_default(), last_event_id)?;
    scoped(parts, message)
}

/// The request a query string and a `Last-Event-ID` ask for.
///
/// Parsed by hand because `jobIds` repeats, which `serde_urlencoded` cannot
/// read into a list. What is missing or contradictory in the target is left to
/// `WatchJobs` to refuse, so both doors refuse it in the same words.
fn read(query: &str, last_event_id: Option<&str>) -> Result<pb::WatchJobsRequest, WireError> {
    let mut ids = Vec::new();
    let mut queue = None;
    let mut resume = None;
    for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
        let slot = match key.as_ref() {
            "jobIds" | "job_ids" => {
                ids.push(value.into_owned());
                continue;
            }
            "queue" => &mut queue,
            "resumeCursor" | "resume_cursor" => &mut resume,
            other => {
                return Err(WireError::invalid_request(format!(
                    "the query string is not one this method takes: unknown field `{other}`"
                )))
            }
        };
        if slot.replace(value.into_owned()).is_some() {
            return Err(WireError::invalid_request(format!(
                "`{key}` is given more than once"
            )));
        }
    }
    let target = match (ids.is_empty(), queue) {
        (false, Some(_)) => {
            return Err(WireError::invalid_request(
                "set `jobIds` or `queue`, not both",
            ))
        }
        (false, None) => Some(Target::JobIds(pb::WatchJobIds { job_ids: ids })),
        (true, Some(queue)) => Some(Target::Queue(queue)),
        (true, None) => None,
    };
    // A reconnecting `EventSource` resends the URL it opened with, so the
    // header is the newer cursor. An id watch hands out none, and reopening is
    // how it resumes, so a stray header there is ignored rather than refused.
    if matches!(target, Some(Target::Queue(_))) {
        if let Some(cursor) = last_event_id.filter(|cursor| !cursor.is_empty()) {
            resume = Some(cursor.to_string());
        }
    }
    Ok(pb::WatchJobsRequest {
        target,
        resume_cursor: resume.unwrap_or_default(),
    })
}

/// An open watch, as an event stream.
fn stream(outlet: Outlet) -> Response {
    let mut response = Sse::new(Events {
        outlet,
        finished: false,
    })
    .keep_alive(KeepAlive::new().interval(KEEPALIVE).text("keepalive"))
    .into_response();
    // nginx buffers a proxied response unless told otherwise, which would hold
    // every event until the buffer fills.
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

/// The outlet's items as events, closed by exactly one `error` or `end`.
struct Events {
    outlet: Outlet,
    finished: bool,
}

impl Stream for Events {
    type Item = Result<Event, std::convert::Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }
        let event = match Pin::new(&mut self.outlet).poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(response))) => item(&response),
            Poll::Ready(Some(Err(status))) => {
                self.finished = true;
                failure(&status)
            }
            Poll::Ready(None) => {
                self.finished = true;
                Event::default().event("end").data("{}")
            }
        };
        Poll::Ready(Some(Ok(event)))
    }
}

/// One `WatchJobsResponse`. An empty cursor sets no `id`, which would otherwise
/// reset the client's `Last-Event-ID`.
fn item(response: &pb::WatchJobsResponse) -> Event {
    let event = Event::default().data(write::watch_jobs(response).to_string());
    if response.cursor.is_empty() {
        event
    } else {
        event.id(&response.cursor)
    }
}

/// How a stream failed, in the body a refusal before it would have carried.
fn failure(status: &Status) -> Event {
    Event::default()
        .event("error")
        .data(error::failure(status).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(request: &pb::WatchJobsRequest) -> &Target {
        request.target.as_ref().expect("a target")
    }

    #[test]
    fn repeated_ids_are_one_id_watch() {
        let request = read("jobIds=a&jobIds=b&job_ids=c", None).unwrap();
        assert_eq!(
            target(&request),
            &Target::JobIds(pb::WatchJobIds {
                job_ids: vec!["a".into(), "b".into(), "c".into()]
            })
        );
        assert!(request.resume_cursor.is_empty());
    }

    #[test]
    fn a_queue_resumes_from_its_cursor() {
        let request = read("queue=orders&resumeCursor=c1", None).unwrap();
        assert_eq!(target(&request), &Target::Queue("orders".into()));
        assert_eq!(request.resume_cursor, "c1");
    }

    /// `EventSource` resends its original URL on reconnect, so the header is
    /// the newer of the two.
    #[test]
    fn last_event_id_beats_the_query_cursor_on_a_queue_watch() {
        let request = read("queue=orders&resumeCursor=old", Some("new")).unwrap();
        assert_eq!(request.resume_cursor, "new");
        let request = read("queue=orders&resumeCursor=old", Some("")).unwrap();
        assert_eq!(request.resume_cursor, "old");
    }

    #[test]
    fn last_event_id_is_ignored_on_an_id_watch() {
        let request = read("jobIds=a", Some("c1")).unwrap();
        assert!(request.resume_cursor.is_empty());
    }

    #[test]
    fn an_unknown_key_a_repeated_one_and_both_targets_are_refused() {
        for query in [
            "jobIds=a&bogus=1",
            "queue=a&queue=b",
            "queue=a&resumeCursor=x&resume_cursor=y",
            "jobIds=a&queue=b",
        ] {
            assert!(read(query, None).is_err(), "query: {query}");
        }
    }

    /// Left for `WatchJobs` to refuse, in the words the gRPC door uses.
    #[test]
    fn no_target_reaches_the_service_unset() {
        assert_eq!(read("", None).unwrap().target, None);
    }

    #[test]
    fn an_id_is_set_only_for_a_cursor() {
        let checkpoint = item(&pb::WatchJobsResponse {
            item: None,
            cursor: "c1".into(),
        });
        let rendered = format!("{checkpoint:?}");
        assert!(rendered.contains("id: c1"), "{rendered}");

        let snapshot = item(&pb::WatchJobsResponse {
            item: Some(pb::watch_jobs_response::Item::NotFoundJobId("a".into())),
            cursor: String::new(),
        });
        let rendered = format!("{snapshot:?}");
        assert!(!rendered.contains("id:"), "{rendered}");
    }
}
