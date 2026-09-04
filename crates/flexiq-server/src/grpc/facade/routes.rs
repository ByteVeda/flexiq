//! The route table, and the handlers it points at.
//!
//! Every handler here does the same three things — read a request, call the
//! `ProducerService` method, render what it answered — and calls **the same
//! trait method a gRPC request reaches**. There is no second implementation of
//! an RPC and no loopback hop: the axum router and the tonic codec are two ways
//! into one `Producer`, and the `AuthLayer` that wraps the whole router has
//! already put the caller's [`Principal`] in the request's extensions by the
//! time either arrives.
//!
//! ## The table is the router
//!
//! [`ROUTES`] is not documentation. [`router`] is built by walking it, and the
//! drift test at the bottom of this file walks it too, against
//! `contracts/descriptor.binpb`: **every RPC the `flexiq.v1` package declares
//! must have a binding, and a binding may serve `GET` only if its RPC is
//! `NO_SIDE_EFFECTS`.** There is no allowlist to forget to add to — adding an
//! RPC to the `.proto` fails this crate's tests until it is routed, and that is
//! the only thing that keeps hand-written transcoding honest.
//!
//! The `flexiq.executor.v1` package is not served here and cannot be: a worker
//! surface has different credentials, different failure modes and no reason to
//! be reachable from a browser. The test asserts that too.
//!
//! ## One wart, and why
//!
//! `POST /v1/jobs/{job_id}:cancel` is registered as `POST /v1/jobs/{job_id}`
//! and the `:cancel` is split off the captured segment. matchit, the router
//! axum matches with, says outright that "dynamic suffixes are not currently
//! supported", so the colon form cannot be registered. The path in [`ROUTES`]
//! stays the one a client types, because that is what the table is for.

use axum::body::{to_bytes, Body as AxumBody, Bytes};
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, Request, State};
use axum::response::Response;
use axum::routing::{get, post, MethodRouter};
use axum::Router;
use http::request::Parts;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tonic::Status;

use super::error;
use super::json::{request as read, response as write};
use crate::grpc::auth::Principal;
use crate::grpc::limits::PRODUCER_MAX_MESSAGE_BYTES;
use crate::grpc::pb;
use crate::grpc::pb::producer_service_server::ProducerService;
use crate::grpc::producer::Producer;
use crate::grpc::status::WireError;

/// The `flexiq.v1` RPCs, by the name the contract gives them.
///
/// A Rust enum rather than a string in the table, so that the match producing
/// a handler is exhaustive: an RPC that gains a variant here does not compile
/// until it has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rpc {
    /// `ProducerService.Enqueue`.
    Enqueue,
    /// `ProducerService.EnqueueBatch`.
    EnqueueBatch,
    /// `ProducerService.GetJob`.
    GetJob,
    /// `ProducerService.ListJobs`.
    ListJobs,
    /// `ProducerService.CancelJob`.
    CancelJob,
    /// `ProducerService.QueueStats`.
    QueueStats,
    /// `ProducerService.SubmitWorkflow`.
    SubmitWorkflow,
    /// `ProducerService.GetWorkflowRun`.
    GetWorkflowRun,
}

impl Rpc {
    /// Every RPC, so a caller that needs the closed set does not restate it.
    pub const ALL: [Self; 8] = [
        Self::Enqueue,
        Self::EnqueueBatch,
        Self::GetJob,
        Self::ListJobs,
        Self::CancelJob,
        Self::QueueStats,
        Self::SubmitWorkflow,
        Self::GetWorkflowRun,
    ];

    /// The method name, exactly as the `.proto` spells it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Enqueue => "Enqueue",
            Self::EnqueueBatch => "EnqueueBatch",
            Self::GetJob => "GetJob",
            Self::ListJobs => "ListJobs",
            Self::CancelJob => "CancelJob",
            Self::QueueStats => "QueueStats",
            Self::SubmitWorkflow => "SubmitWorkflow",
            Self::GetWorkflowRun => "GetWorkflowRun",
        }
    }
}

/// The HTTP method a binding answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verb {
    /// Legal only where the RPC is `NO_SIDE_EFFECTS`.
    Get,
    /// Everything else.
    Post,
}

/// One route.
///
/// A binding rather than an RPC, because one RPC may need more than one path:
/// `QueueStatsRequest.queue` is optional and unset counts the whole namespace,
/// which no single path with a queue in it can express.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Binding {
    /// `POST /v1/jobs`.
    Enqueue,
    /// `POST /v1/jobs:batchEnqueue`.
    EnqueueBatch,
    /// `GET /v1/jobs/{job_id}`.
    GetJob,
    /// `GET /v1/jobs`.
    ListJobs,
    /// `POST /v1/jobs/{job_id}:cancel`.
    CancelJob,
    /// `GET /v1/queues/{queue}/stats`.
    QueueStats,
    /// `GET /v1/stats` — every queue in the namespace.
    NamespaceStats,
    /// `POST /v1/workflows`.
    SubmitWorkflow,
    /// `GET /v1/workflows/{run_id}`.
    GetWorkflowRun,
}

impl Binding {
    /// The RPC this binding calls.
    pub const fn rpc(self) -> Rpc {
        match self {
            Self::Enqueue => Rpc::Enqueue,
            Self::EnqueueBatch => Rpc::EnqueueBatch,
            Self::GetJob => Rpc::GetJob,
            Self::ListJobs => Rpc::ListJobs,
            Self::CancelJob => Rpc::CancelJob,
            Self::QueueStats | Self::NamespaceStats => Rpc::QueueStats,
            Self::SubmitWorkflow => Rpc::SubmitWorkflow,
            Self::GetWorkflowRun => Rpc::GetWorkflowRun,
        }
    }

    /// The method it answers.
    pub const fn verb(self) -> Verb {
        match self {
            Self::Enqueue | Self::EnqueueBatch | Self::CancelJob | Self::SubmitWorkflow => {
                Verb::Post
            }
            Self::GetJob
            | Self::ListJobs
            | Self::QueueStats
            | Self::NamespaceStats
            | Self::GetWorkflowRun => Verb::Get,
        }
    }

    /// The path a client types.
    pub const fn path(self) -> &'static str {
        match self {
            Self::Enqueue => "/v1/jobs",
            Self::EnqueueBatch => "/v1/jobs:batchEnqueue",
            Self::GetJob => "/v1/jobs/{job_id}",
            Self::ListJobs => "/v1/jobs",
            Self::CancelJob => "/v1/jobs/{job_id}:cancel",
            Self::QueueStats => "/v1/queues/{queue}/stats",
            Self::NamespaceStats => "/v1/stats",
            Self::SubmitWorkflow => "/v1/workflows",
            Self::GetWorkflowRun => "/v1/workflows/{run_id}",
        }
    }

    /// The path axum registers, which is [`Self::path`] except where matchit
    /// cannot express it.
    const fn pattern(self) -> &'static str {
        match self {
            Self::CancelJob => "/v1/jobs/{job_id}",
            other => other.path(),
        }
    }

    /// The handler, as a method router. Exhaustive on purpose.
    fn service(self) -> MethodRouter<Producer> {
        match self {
            Self::Enqueue => post(enqueue),
            Self::EnqueueBatch => post(enqueue_batch),
            Self::GetJob => get(get_job),
            Self::ListJobs => get(list_jobs),
            Self::CancelJob => post(job_custom_method),
            Self::QueueStats => get(queue_stats),
            Self::NamespaceStats => get(namespace_stats),
            Self::SubmitWorkflow => post(submit_workflow),
            Self::GetWorkflowRun => get(get_workflow_run),
        }
    }
}

/// Every route this door serves.
pub const ROUTES: &[Binding] = &[
    Binding::Enqueue,
    Binding::EnqueueBatch,
    Binding::GetJob,
    Binding::ListJobs,
    Binding::CancelJob,
    Binding::QueueStats,
    Binding::NamespaceStats,
    Binding::SubmitWorkflow,
    Binding::GetWorkflowRun,
];

/// Which binding a concrete request path and method reach, if any.
///
/// axum answers this during routing, but the answer lands in the request's
/// extensions where only a handler can see it — and the thing that needs it is
/// the metrics layer, which runs outside the router so that a refused call is
/// still counted. So it is answered here instead, against the same table axum
/// is built from.
///
/// The match is exact on literal segments and permissive on `{param}` ones,
/// including the one segment that carries a suffix after its parameter
/// (`{job_id}:cancel`).
pub fn resolve(method: &http::Method, path: &str) -> Option<Binding> {
    ROUTES.iter().copied().find(|binding| {
        let wanted = match binding.verb() {
            Verb::Get => http::Method::GET,
            Verb::Post => http::Method::POST,
        };
        method == wanted && path_matches(binding.path(), path)
    })
}

/// Whether `actual` is an instance of the `{param}`-carrying `template`.
fn path_matches(template: &str, actual: &str) -> bool {
    let mut wanted = template.split('/');
    let mut given = actual.split('/');
    loop {
        match (wanted.next(), given.next()) {
            (None, None) => return true,
            (Some(want), Some(give)) if segment_matches(want, give) => {}
            _ => return false,
        }
    }
}

/// One path segment, which is either a literal, a bare `{param}`, or a
/// `{param}` followed by a literal suffix.
fn segment_matches(template: &str, actual: &str) -> bool {
    let Some(rest) = template.strip_prefix('{') else {
        return template == actual;
    };
    let Some((_, suffix)) = rest.split_once('}') else {
        // An unterminated brace is not a parameter; compare it literally rather
        // than treating a malformed template as a wildcard.
        return template == actual;
    };
    // A parameter never matches nothing: `/v1/jobs/` is not `/v1/jobs/{job_id}`.
    actual.len() > suffix.len() && actual.ends_with(suffix)
}

/// The facade's routes, built from [`ROUTES`].
///
/// axum takes one method router per path, so bindings sharing a pattern are
/// merged before registration — `GET /v1/jobs` and `POST /v1/jobs` are two
/// bindings and one route.
pub fn router(producer: Producer) -> Router {
    let mut router = Router::new();
    let mut registered: Vec<&'static str> = Vec::new();
    for binding in ROUTES {
        let pattern = binding.pattern();
        if registered.contains(&pattern) {
            continue;
        }
        registered.push(pattern);
        let service = ROUTES
            .iter()
            .filter(|other| other.pattern() == pattern)
            .fold(MethodRouter::new(), |merged, other| {
                merged.merge(other.service())
            });
        router = router.route(pattern, service);
    }
    router.with_state(producer)
}

// ── Handlers ─────────────────────────────────────────────────────────
//
// Each is the same three lines: prepare the request, call the trait method,
// render the answer. What differs between them is only where the message comes
// from, which is why the preparation is a function of its own per handler and
// the rest is shared.

async fn enqueue(State(producer): State<Producer>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let request = match prepare_enqueue(&parts, body).await {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.enqueue(request).await, write::enqueue)
}

async fn prepare_enqueue(
    parts: &Parts,
    body: AxumBody,
) -> Result<tonic::Request<pb::EnqueueRequest>, WireError> {
    let message = decode::<read::Enqueue>(body)
        .await?
        .into_message()
        .map_err(WireError::invalid_request)?;
    scoped(parts, message)
}

async fn enqueue_batch(State(producer): State<Producer>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let request = match prepare_enqueue_batch(&parts, body).await {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.enqueue_batch(request).await, write::enqueue_batch)
}

async fn prepare_enqueue_batch(
    parts: &Parts,
    body: AxumBody,
) -> Result<tonic::Request<pb::EnqueueBatchRequest>, WireError> {
    let message = decode::<read::EnqueueBatch>(body)
        .await?
        .into_message()
        .map_err(WireError::invalid_request)?;
    scoped(parts, message)
}

async fn get_job(
    State(producer): State<Producer>,
    job_id: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let request = match prepare_get_job(&parts, job_id) {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.get_job(request).await, write::get_job)
}

fn prepare_get_job(
    parts: &Parts,
    job_id: Result<Path<String>, PathRejection>,
) -> Result<tonic::Request<pb::GetJobRequest>, WireError> {
    let job_id = path_param(job_id)?;
    let blobs: read::GetJob = query(parts)?;
    scoped(parts, blobs.into_message(job_id))
}

async fn list_jobs(State(producer): State<Producer>, parts: Parts) -> Response {
    let request = match prepare_list_jobs(&parts) {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.list_jobs(request).await, write::list_jobs)
}

fn prepare_list_jobs(parts: &Parts) -> Result<tonic::Request<pb::ListJobsRequest>, WireError> {
    let filters: read::ListJobs = query(parts)?;
    let message = filters.into_message().map_err(WireError::invalid_request)?;
    scoped(parts, message)
}

/// `POST` on one job: the custom method is the suffix on the last path segment.
///
/// `cancel` is the only one, so an unrecognised verb is answered exactly as an
/// unrouted path is — there is no RPC at that address.
async fn job_custom_method(
    State(producer): State<Producer>,
    job_id: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let request = match prepare_cancel_job(&parts, job_id) {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.cancel_job(request).await, write::cancel_job)
}

/// The one custom method a job carries.
const CANCEL_VERB: &str = "cancel";

fn prepare_cancel_job(
    parts: &Parts,
    job_id: Result<Path<String>, PathRejection>,
) -> Result<tonic::Request<pb::CancelJobRequest>, WireError> {
    let segment = path_param(job_id)?;
    let unrouted = || WireError::no_such_method("POST", parts.uri.path());
    let (job_id, verb) = segment.rsplit_once(':').ok_or_else(unrouted)?;
    if verb != CANCEL_VERB {
        return Err(unrouted());
    }
    scoped(
        parts,
        pb::CancelJobRequest {
            job_id: job_id.to_string(),
        },
    )
}

async fn queue_stats(
    State(producer): State<Producer>,
    queue: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let request = match path_param(queue)
        .and_then(|queue| scoped(&parts, pb::QueueStatsRequest { queue: Some(queue) }))
    {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.queue_stats(request).await, write::queue_stats)
}

async fn namespace_stats(State(producer): State<Producer>, parts: Parts) -> Response {
    // `queue: None` counts every queue in the namespace. It is never a way to
    // reach another one: the namespace comes from the credential, and this
    // request has no field for it.
    let request = match scoped(&parts, pb::QueueStatsRequest { queue: None }) {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.queue_stats(request).await, write::queue_stats)
}

async fn submit_workflow(State(producer): State<Producer>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let request = match prepare_submit_workflow(&parts, body).await {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(
        producer.submit_workflow(request).await,
        write::submit_workflow,
    )
}

async fn prepare_submit_workflow(
    parts: &Parts,
    body: AxumBody,
) -> Result<tonic::Request<pb::SubmitWorkflowRequest>, WireError> {
    let message = decode::<read::SubmitWorkflow>(body)
        .await?
        .into_message()
        .map_err(WireError::invalid_request)?;
    scoped(parts, message)
}

async fn get_workflow_run(
    State(producer): State<Producer>,
    run_id: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let request = match prepare_get_workflow_run(&parts, run_id) {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(
        producer.get_workflow_run(request).await,
        write::get_workflow_run,
    )
}

fn prepare_get_workflow_run(
    parts: &Parts,
    run_id: Result<Path<String>, PathRejection>,
) -> Result<tonic::Request<pb::GetWorkflowRunRequest>, WireError> {
    let run_id = path_param(run_id)?;
    scoped(parts, pb::GetWorkflowRunRequest { run_id })
}

// ── The three things every handler does ──────────────────────────────

/// Read a JSON body into a request message.
///
/// The cap is [`PRODUCER_MAX_MESSAGE_BYTES`], the same number the gRPC codec is
/// configured with, so the two doors cannot disagree about what is too large.
async fn decode<T: DeserializeOwned>(body: AxumBody) -> Result<T, WireError> {
    /// Enough of a parser's complaint to act on, without echoing a body back.
    const MAX_COMPLAINT: usize = 300;

    let bytes: Bytes = to_bytes(body, PRODUCER_MAX_MESSAGE_BYTES)
        .await
        .map_err(|_| WireError::payload_too_large(PRODUCER_MAX_MESSAGE_BYTES))?;
    if bytes.is_empty() {
        return Err(WireError::malformed_payload(
            "the request body is empty; send a JSON object",
        ));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        let complaint: String = error.to_string().chars().take(MAX_COMPLAINT).collect();
        WireError::malformed_payload(format!(
            "the request body is not the message this method takes: {complaint}"
        ))
    })
}

/// Read the query string into the filters a `GET` carries.
fn query<T: DeserializeOwned + Default>(parts: &Parts) -> Result<T, WireError> {
    match parts.uri.query() {
        None | Some("") => Ok(T::default()),
        Some(raw) => serde_urlencoded::from_str(raw).map_err(|error| {
            WireError::invalid_request(format!(
                "the query string is not one this method takes: {error}"
            ))
        }),
    }
}

/// One percent-decoded path parameter.
fn path_param(param: Result<Path<String>, PathRejection>) -> Result<String, WireError> {
    // Only reachable through a route that declared the parameter, so a
    // rejection means the segment did not decode — a client-side mistake, and
    // one this door has nothing better to say about than what axum found.
    param
        .map(|Path(value)| value)
        .map_err(|rejection| WireError::invalid_request(rejection.body_text()))
}

/// Attach the caller's principal to the request the service will see.
///
/// The same value the gRPC path carries, taken from the same place: the auth
/// layer wraps the whole router, so a facade request has been through it too. A
/// request that somehow arrives without one fails closed here rather than
/// reaching a handler that would have to choose a namespace.
fn scoped<T>(parts: &Parts, message: T) -> Result<tonic::Request<T>, WireError> {
    let Some(principal) = parts.extensions.get::<Principal>() else {
        log::error!(
            "grpc: a facade request carried no principal; the router is registered \
             without the auth layer"
        );
        return Err(WireError::internal());
    };
    let mut request = tonic::Request::new(message);
    request.extensions_mut().insert(principal.clone());
    Ok(request)
}

/// Render whatever the service answered.
fn finish<T>(outcome: Result<tonic::Response<T>, Status>, render: fn(&T) -> Value) -> Response {
    match outcome {
        Ok(response) => error::ok(&render(response.get_ref())),
        Err(status) => error::response(&status),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::facade::descriptor;

    /// The check the issue asks for, and the one §11 fails the PR without:
    /// every RPC the package declares is routed. Read off the descriptor, so
    /// there is no second list to keep in step — adding an RPC to the `.proto`
    /// fails here until it has a binding.
    #[test]
    fn every_producer_rpc_has_a_route() {
        for rpc in descriptor::rpcs(descriptor::PRODUCER_PACKAGE) {
            assert!(
                ROUTES
                    .iter()
                    .any(|binding| binding.rpc().as_str() == rpc.method),
                "{}.{} has no route in the JSON facade",
                rpc.service,
                rpc.method
            );
        }
    }

    /// And nothing is routed that the package does not declare — a binding for
    /// an RPC that was removed, or misspelled, fails here.
    #[test]
    fn every_route_names_an_rpc_the_package_declares() {
        let declared = descriptor::rpcs(descriptor::PRODUCER_PACKAGE);
        for binding in ROUTES {
            assert!(
                declared
                    .iter()
                    .any(|rpc| rpc.method == binding.rpc().as_str()),
                "{:?} routes to {}, which {} does not declare",
                binding,
                binding.rpc().as_str(),
                descriptor::PRODUCER_PACKAGE
            );
        }
    }

    /// D15, stated as an iff so that adding an RPC needs no judgement call.
    #[test]
    fn a_get_serves_exactly_the_no_side_effects_rpcs() {
        let declared = descriptor::rpcs(descriptor::PRODUCER_PACKAGE);
        for binding in ROUTES {
            let rpc = declared
                .iter()
                .find(|rpc| rpc.method == binding.rpc().as_str())
                .expect("every binding names a declared RPC");
            assert_eq!(
                binding.verb() == Verb::Get,
                rpc.no_side_effects,
                "{:?} serves {} on the wrong method for its idempotency level",
                binding,
                rpc.method
            );
        }
    }

    /// The worker surface has different credentials and different failure
    /// modes; the facade transcodes one package and not the other.
    ///
    /// Vacuous until #720 puts RPCs in that package, and deliberately written
    /// so that it starts biting the moment it does rather than having to be
    /// remembered then. What holds today is the test above: every binding
    /// resolves against `flexiq.v1`, and nothing else.
    #[test]
    fn no_executor_rpc_is_reachable_over_http() {
        for rpc in descriptor::rpcs(descriptor::EXECUTOR_PACKAGE) {
            assert!(
                !ROUTES
                    .iter()
                    .any(|binding| binding.rpc().as_str() == rpc.method),
                "{}.{} is an executor RPC and must not have a route",
                rpc.service,
                rpc.method
            );
        }
    }

    /// Two bindings may share a pattern (`GET` and `POST /v1/jobs`), but two
    /// bindings must never share a pattern *and* a method — axum would panic at
    /// startup, and this says so at test time instead.
    #[test]
    fn no_two_bindings_answer_the_same_method_on_the_same_pattern() {
        let mut seen = Vec::new();
        for binding in ROUTES {
            let key = (binding.pattern(), binding.verb());
            assert!(
                !seen.contains(&key),
                "{binding:?} collides with an earlier binding"
            );
            seen.push(key);
        }
    }

    /// The public path and the registered pattern differ in exactly one place,
    /// and it is the one matchit cannot express.
    #[test]
    fn only_the_custom_method_route_registers_a_different_pattern() {
        for binding in ROUTES {
            if *binding == Binding::CancelJob {
                assert_eq!(binding.path(), "/v1/jobs/{job_id}:cancel");
                assert_eq!(binding.pattern(), "/v1/jobs/{job_id}");
            } else {
                assert_eq!(binding.path(), binding.pattern());
            }
        }
    }
}
