//! The route table, and the producer handlers it points at.
//!
//! Every handler does the same three things — read a request, call the service
//! method, render what it answered — and calls **the same trait method a gRPC
//! request reaches**. There is no second implementation of an RPC and no
//! loopback hop: the axum router and the tonic codec are two ways into one
//! `Producer` and one `Admin`, and the `AuthLayer` that wraps the whole router
//! has already put the caller's [`Principal`] in the request's extensions by
//! the time either arrives. The producer's handlers live here; the operator
//! door's live in [`super::admin`].
//!
//! ## The table is the router
//!
//! [`ROUTES`] is not documentation. [`router`] is built by walking it, and the
//! drift tests at the bottom of this file walk it too, against
//! `contracts/descriptor.binpb`: **every RPC the `flexiq.v1` and
//! `flexiq.admin.v1` packages declare must have a binding, and a binding may
//! serve `GET` only if its RPC is `NO_SIDE_EFFECTS`.** There is no allowlist to
//! forget to add to — adding an RPC to either `.proto` fails this crate's tests
//! until it is routed, and that is the only thing that keeps hand-written
//! transcoding honest.
//!
//! The `flexiq.executor.v1` package is not served here and cannot be: a worker
//! surface has different credentials, different failure modes and no reason to
//! be reachable from a browser. The test asserts that too.
//!
//! ## One wart, and why
//!
//! A custom method on a resource — `POST /v1/jobs/{job_id}:cancel`,
//! `POST /v1/admin/queues/{queue}:pause` — is registered without its verb, as
//! `POST /v1/jobs/{job_id}`, and one handler, [`custom_method`], splits the
//! verb off the captured segment. matchit, the router axum matches with, says
//! outright that "dynamic suffixes are not currently supported", so the colon
//! form cannot be registered. Which binding a verb names is answered by
//! [`resolve`] — the function the metrics layer labels with — so dispatch and
//! telemetry cannot disagree about which RPC a path reached. The path in
//! [`ROUTES`] stays the one a client types, because that is what the table is
//! for.
//!
//! A verb after a *literal* segment (`/v1/admin/deadLetters:purge`,
//! `/v1/admin/queues/{queue}/override:clear`) is no wart at all: to matchit it
//! is just a static segment with a colon in it.

use axum::body::{to_bytes, Body as AxumBody, Bytes};
use axum::extract::rejection::PathRejection;
use axum::extract::{FromRef, Path, Request, State};
use axum::response::Response;
use axum::routing::{get, post, MethodRouter};
use axum::Router;
use http::request::Parts;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tonic::Status;

use super::admin as operator;
use super::error;
use super::json::{request as read, response as write};
use crate::grpc::admin::Admin;
use crate::grpc::auth::Principal;
use crate::grpc::limits::PRODUCER_MAX_MESSAGE_BYTES;
use crate::grpc::pb;
use crate::grpc::pb::producer_service_server::ProducerService;
use crate::grpc::producer::Producer;
use crate::grpc::status::WireError;

/// A service this door transcodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Service {
    /// `flexiq.v1.ProducerService`.
    Producer,
    /// `flexiq.admin.v1.AdminService`.
    Admin,
}

impl Service {
    /// The protobuf package it is declared in.
    pub const fn package(self) -> &'static str {
        match self {
            Self::Producer => "flexiq.v1",
            Self::Admin => "flexiq.admin.v1",
        }
    }

    /// The fully-qualified service name, as a gRPC path spells it.
    pub const fn full_name(self) -> &'static str {
        match self {
            Self::Producer => "flexiq.v1.ProducerService",
            Self::Admin => "flexiq.admin.v1.AdminService",
        }
    }
}

/// The transcoded RPCs, by the name the contract gives them.
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
    /// `AdminService.ListQueues`.
    ListQueues,
    /// `AdminService.PauseQueue`.
    PauseQueue,
    /// `AdminService.ResumeQueue`.
    ResumeQueue,
    /// `AdminService.GetThroughput`.
    GetThroughput,
    /// `AdminService.ListDeadLetters`.
    ListDeadLetters,
    /// `AdminService.GetDeadLetter`.
    GetDeadLetter,
    /// `AdminService.ReplayDeadLetter`.
    ReplayDeadLetter,
    /// `AdminService.DeleteDeadLetter`.
    DeleteDeadLetter,
    /// `AdminService.PurgeDeadLetters`.
    PurgeDeadLetters,
    /// `AdminService.ListWorkers`.
    ListWorkers,
    /// `AdminService.DrainWorker`.
    DrainWorker,
    /// `AdminService.ListPeriodicTasks`.
    ListPeriodicTasks,
    /// `AdminService.GetPeriodicTask`.
    GetPeriodicTask,
    /// `AdminService.PutPeriodicTask`.
    PutPeriodicTask,
    /// `AdminService.DeletePeriodicTask`.
    DeletePeriodicTask,
    /// `AdminService.PausePeriodicTask`.
    PausePeriodicTask,
    /// `AdminService.ResumePeriodicTask`.
    ResumePeriodicTask,
    /// `AdminService.TriggerPeriodicTask`.
    TriggerPeriodicTask,
    /// `AdminService.ListOverrides`.
    ListOverrides,
    /// `AdminService.SetTaskOverride`.
    SetTaskOverride,
    /// `AdminService.ClearTaskOverride`.
    ClearTaskOverride,
    /// `AdminService.SetQueueOverride`.
    SetQueueOverride,
    /// `AdminService.ClearQueueOverride`.
    ClearQueueOverride,
}

impl Rpc {
    /// Every RPC, so a caller that needs the closed set does not restate it.
    pub const ALL: [Self; 31] = [
        Self::Enqueue,
        Self::EnqueueBatch,
        Self::GetJob,
        Self::ListJobs,
        Self::CancelJob,
        Self::QueueStats,
        Self::SubmitWorkflow,
        Self::GetWorkflowRun,
        Self::ListQueues,
        Self::PauseQueue,
        Self::ResumeQueue,
        Self::GetThroughput,
        Self::ListDeadLetters,
        Self::GetDeadLetter,
        Self::ReplayDeadLetter,
        Self::DeleteDeadLetter,
        Self::PurgeDeadLetters,
        Self::ListWorkers,
        Self::DrainWorker,
        Self::ListPeriodicTasks,
        Self::GetPeriodicTask,
        Self::PutPeriodicTask,
        Self::DeletePeriodicTask,
        Self::PausePeriodicTask,
        Self::ResumePeriodicTask,
        Self::TriggerPeriodicTask,
        Self::ListOverrides,
        Self::SetTaskOverride,
        Self::ClearTaskOverride,
        Self::SetQueueOverride,
        Self::ClearQueueOverride,
    ];

    /// The service that declares it.
    pub const fn service(self) -> Service {
        match self {
            Self::Enqueue
            | Self::EnqueueBatch
            | Self::GetJob
            | Self::ListJobs
            | Self::CancelJob
            | Self::QueueStats
            | Self::SubmitWorkflow
            | Self::GetWorkflowRun => Service::Producer,
            Self::ListQueues
            | Self::PauseQueue
            | Self::ResumeQueue
            | Self::GetThroughput
            | Self::ListDeadLetters
            | Self::GetDeadLetter
            | Self::ReplayDeadLetter
            | Self::DeleteDeadLetter
            | Self::PurgeDeadLetters
            | Self::ListWorkers
            | Self::DrainWorker
            | Self::ListPeriodicTasks
            | Self::GetPeriodicTask
            | Self::PutPeriodicTask
            | Self::DeletePeriodicTask
            | Self::PausePeriodicTask
            | Self::ResumePeriodicTask
            | Self::TriggerPeriodicTask
            | Self::ListOverrides
            | Self::SetTaskOverride
            | Self::ClearTaskOverride
            | Self::SetQueueOverride
            | Self::ClearQueueOverride => Service::Admin,
        }
    }

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
            Self::ListQueues => "ListQueues",
            Self::PauseQueue => "PauseQueue",
            Self::ResumeQueue => "ResumeQueue",
            Self::GetThroughput => "GetThroughput",
            Self::ListDeadLetters => "ListDeadLetters",
            Self::GetDeadLetter => "GetDeadLetter",
            Self::ReplayDeadLetter => "ReplayDeadLetter",
            Self::DeleteDeadLetter => "DeleteDeadLetter",
            Self::PurgeDeadLetters => "PurgeDeadLetters",
            Self::ListWorkers => "ListWorkers",
            Self::DrainWorker => "DrainWorker",
            Self::ListPeriodicTasks => "ListPeriodicTasks",
            Self::GetPeriodicTask => "GetPeriodicTask",
            Self::PutPeriodicTask => "PutPeriodicTask",
            Self::DeletePeriodicTask => "DeletePeriodicTask",
            Self::PausePeriodicTask => "PausePeriodicTask",
            Self::ResumePeriodicTask => "ResumePeriodicTask",
            Self::TriggerPeriodicTask => "TriggerPeriodicTask",
            Self::ListOverrides => "ListOverrides",
            Self::SetTaskOverride => "SetTaskOverride",
            Self::ClearTaskOverride => "ClearTaskOverride",
            Self::SetQueueOverride => "SetQueueOverride",
            Self::ClearQueueOverride => "ClearQueueOverride",
        }
    }

    /// `package.Service/Method`, the gRPC path without its leading slash — one
    /// spelling for an RPC however it was reached.
    pub fn full_method(self) -> String {
        format!("{}/{}", self.service().full_name(), self.as_str())
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
/// which no single path with a queue in it can express. Every admin RPC has
/// exactly one, named after it.
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
    /// `GET /v1/stats` — every queue in the namespace, or the one `?queue=`
    /// names.
    NamespaceStats,
    /// `POST /v1/workflows`.
    SubmitWorkflow,
    /// `GET /v1/workflows/{run_id}`.
    GetWorkflowRun,
    /// `GET /v1/admin/queues`.
    ListQueues,
    /// `POST /v1/admin/queues/{queue}:pause`.
    PauseQueue,
    /// `POST /v1/admin/queues/{queue}:resume`.
    ResumeQueue,
    /// `GET /v1/admin/throughput`.
    GetThroughput,
    /// `GET /v1/admin/deadLetters`.
    ListDeadLetters,
    /// `GET /v1/admin/deadLetters/{dead_letter_id}`.
    GetDeadLetter,
    /// `POST /v1/admin/deadLetters/{dead_letter_id}:replay`.
    ReplayDeadLetter,
    /// `POST /v1/admin/deadLetters/{dead_letter_id}:delete`.
    DeleteDeadLetter,
    /// `POST /v1/admin/deadLetters:purge`.
    PurgeDeadLetters,
    /// `GET /v1/admin/workers`.
    ListWorkers,
    /// `POST /v1/admin/workers/{worker_id}:drain`.
    DrainWorker,
    /// `GET /v1/admin/periodicTasks`.
    ListPeriodicTasks,
    /// `GET /v1/admin/periodicTasks/{name}`.
    GetPeriodicTask,
    /// `POST /v1/admin/periodicTasks`.
    PutPeriodicTask,
    /// `POST /v1/admin/periodicTasks/{name}:delete`.
    DeletePeriodicTask,
    /// `POST /v1/admin/periodicTasks/{name}:pause`.
    PausePeriodicTask,
    /// `POST /v1/admin/periodicTasks/{name}:resume`.
    ResumePeriodicTask,
    /// `POST /v1/admin/periodicTasks/{name}:trigger`.
    TriggerPeriodicTask,
    /// `GET /v1/admin/overrides`.
    ListOverrides,
    /// `POST /v1/admin/tasks/{task_name}/override`.
    SetTaskOverride,
    /// `POST /v1/admin/tasks/{task_name}/override:clear`.
    ClearTaskOverride,
    /// `POST /v1/admin/queues/{queue}/override`.
    SetQueueOverride,
    /// `POST /v1/admin/queues/{queue}/override:clear`.
    ClearQueueOverride,
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
            Self::ListQueues => Rpc::ListQueues,
            Self::PauseQueue => Rpc::PauseQueue,
            Self::ResumeQueue => Rpc::ResumeQueue,
            Self::GetThroughput => Rpc::GetThroughput,
            Self::ListDeadLetters => Rpc::ListDeadLetters,
            Self::GetDeadLetter => Rpc::GetDeadLetter,
            Self::ReplayDeadLetter => Rpc::ReplayDeadLetter,
            Self::DeleteDeadLetter => Rpc::DeleteDeadLetter,
            Self::PurgeDeadLetters => Rpc::PurgeDeadLetters,
            Self::ListWorkers => Rpc::ListWorkers,
            Self::DrainWorker => Rpc::DrainWorker,
            Self::ListPeriodicTasks => Rpc::ListPeriodicTasks,
            Self::GetPeriodicTask => Rpc::GetPeriodicTask,
            Self::PutPeriodicTask => Rpc::PutPeriodicTask,
            Self::DeletePeriodicTask => Rpc::DeletePeriodicTask,
            Self::PausePeriodicTask => Rpc::PausePeriodicTask,
            Self::ResumePeriodicTask => Rpc::ResumePeriodicTask,
            Self::TriggerPeriodicTask => Rpc::TriggerPeriodicTask,
            Self::ListOverrides => Rpc::ListOverrides,
            Self::SetTaskOverride => Rpc::SetTaskOverride,
            Self::ClearTaskOverride => Rpc::ClearTaskOverride,
            Self::SetQueueOverride => Rpc::SetQueueOverride,
            Self::ClearQueueOverride => Rpc::ClearQueueOverride,
        }
    }

    /// The method it answers.
    pub const fn verb(self) -> Verb {
        match self {
            Self::GetJob
            | Self::ListJobs
            | Self::QueueStats
            | Self::NamespaceStats
            | Self::GetWorkflowRun
            | Self::ListQueues
            | Self::GetThroughput
            | Self::ListDeadLetters
            | Self::GetDeadLetter
            | Self::ListWorkers
            | Self::ListPeriodicTasks
            | Self::GetPeriodicTask
            | Self::ListOverrides => Verb::Get,
            Self::Enqueue
            | Self::EnqueueBatch
            | Self::CancelJob
            | Self::SubmitWorkflow
            | Self::PauseQueue
            | Self::ResumeQueue
            | Self::ReplayDeadLetter
            | Self::DeleteDeadLetter
            | Self::PurgeDeadLetters
            | Self::DrainWorker
            | Self::PutPeriodicTask
            | Self::DeletePeriodicTask
            | Self::PausePeriodicTask
            | Self::ResumePeriodicTask
            | Self::TriggerPeriodicTask
            | Self::SetTaskOverride
            | Self::ClearTaskOverride
            | Self::SetQueueOverride
            | Self::ClearQueueOverride => Verb::Post,
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
            Self::ListQueues => "/v1/admin/queues",
            Self::PauseQueue => "/v1/admin/queues/{queue}:pause",
            Self::ResumeQueue => "/v1/admin/queues/{queue}:resume",
            Self::GetThroughput => "/v1/admin/throughput",
            Self::ListDeadLetters => "/v1/admin/deadLetters",
            Self::GetDeadLetter => "/v1/admin/deadLetters/{dead_letter_id}",
            Self::ReplayDeadLetter => "/v1/admin/deadLetters/{dead_letter_id}:replay",
            Self::DeleteDeadLetter => "/v1/admin/deadLetters/{dead_letter_id}:delete",
            Self::PurgeDeadLetters => "/v1/admin/deadLetters:purge",
            Self::ListWorkers => "/v1/admin/workers",
            Self::DrainWorker => "/v1/admin/workers/{worker_id}:drain",
            Self::ListPeriodicTasks => "/v1/admin/periodicTasks",
            Self::GetPeriodicTask => "/v1/admin/periodicTasks/{name}",
            Self::PutPeriodicTask => "/v1/admin/periodicTasks",
            Self::DeletePeriodicTask => "/v1/admin/periodicTasks/{name}:delete",
            Self::PausePeriodicTask => "/v1/admin/periodicTasks/{name}:pause",
            Self::ResumePeriodicTask => "/v1/admin/periodicTasks/{name}:resume",
            Self::TriggerPeriodicTask => "/v1/admin/periodicTasks/{name}:trigger",
            Self::ListOverrides => "/v1/admin/overrides",
            Self::SetTaskOverride => "/v1/admin/tasks/{task_name}/override",
            Self::ClearTaskOverride => "/v1/admin/tasks/{task_name}/override:clear",
            Self::SetQueueOverride => "/v1/admin/queues/{queue}/override",
            Self::ClearQueueOverride => "/v1/admin/queues/{queue}/override:clear",
        }
    }

    /// The custom-method verb after a `{param}`, if the path ends in one:
    /// `cancel` for `/v1/jobs/{job_id}:cancel`. A verb after a literal segment
    /// is not one of these — matchit can register it as it stands.
    pub fn custom_verb(self) -> Option<&'static str> {
        let path = self.path();
        let last = &path[path.rfind('/').map_or(0, |slash| slash + 1)..];
        let (param, verb) = last.split_once("}:")?;
        param.starts_with('{').then_some(verb)
    }

    /// The path axum registers: [`Self::path`] without its custom verb, which
    /// matchit cannot express.
    fn pattern(self) -> &'static str {
        let path = self.path();
        match self.custom_verb() {
            Some(verb) => &path[..path.len() - verb.len() - 1],
            None => path,
        }
    }

    /// The handler, as a method router. Exhaustive on purpose.
    fn service(self) -> MethodRouter<Doors> {
        match self {
            Self::Enqueue => post(enqueue),
            Self::EnqueueBatch => post(enqueue_batch),
            Self::GetJob => get(get_job),
            Self::ListJobs => get(list_jobs),
            Self::QueueStats => get(queue_stats),
            Self::NamespaceStats => get(namespace_stats),
            Self::SubmitWorkflow => post(submit_workflow),
            Self::GetWorkflowRun => get(get_workflow_run),
            Self::ListQueues => get(operator::list_queues),
            Self::GetThroughput => get(operator::get_throughput),
            Self::ListDeadLetters => get(operator::list_dead_letters),
            Self::GetDeadLetter => get(operator::get_dead_letter),
            Self::PurgeDeadLetters => post(operator::purge_dead_letters),
            Self::ListWorkers => get(operator::list_workers),
            Self::ListPeriodicTasks => get(operator::list_periodic_tasks),
            Self::GetPeriodicTask => get(operator::get_periodic_task),
            Self::PutPeriodicTask => post(operator::put_periodic_task),
            Self::ListOverrides => get(operator::list_overrides),
            Self::SetTaskOverride => post(operator::set_task_override),
            Self::ClearTaskOverride => post(operator::clear_task_override),
            Self::SetQueueOverride => post(operator::set_queue_override),
            Self::ClearQueueOverride => post(operator::clear_queue_override),
            Self::CancelJob
            | Self::PauseQueue
            | Self::ResumeQueue
            | Self::ReplayDeadLetter
            | Self::DeleteDeadLetter
            | Self::DrainWorker
            | Self::DeletePeriodicTask
            | Self::PausePeriodicTask
            | Self::ResumePeriodicTask
            | Self::TriggerPeriodicTask => post(custom_method),
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
    Binding::ListQueues,
    Binding::PauseQueue,
    Binding::ResumeQueue,
    Binding::GetThroughput,
    Binding::ListDeadLetters,
    Binding::GetDeadLetter,
    Binding::ReplayDeadLetter,
    Binding::DeleteDeadLetter,
    Binding::PurgeDeadLetters,
    Binding::ListWorkers,
    Binding::DrainWorker,
    Binding::ListPeriodicTasks,
    Binding::GetPeriodicTask,
    Binding::PutPeriodicTask,
    Binding::DeletePeriodicTask,
    Binding::PausePeriodicTask,
    Binding::ResumePeriodicTask,
    Binding::TriggerPeriodicTask,
    Binding::ListOverrides,
    Binding::SetTaskOverride,
    Binding::ClearTaskOverride,
    Binding::SetQueueOverride,
    Binding::ClearQueueOverride,
];

/// Which binding a concrete request path and method reach, if any.
///
/// axum answers this during routing, but the answer lands in the request's
/// extensions where only a handler can see it — and the thing that needs it is
/// the metrics layer, which runs outside the router so that a refused call is
/// still counted. So it is answered here instead, against the same table axum
/// is built from, and [`custom_method`] asks it too.
///
/// The match is exact on literal segments and permissive on `{param}` ones,
/// including a segment that carries a verb after its parameter
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

/// What the facade's handlers reach: the two services it transcodes, each
/// extracted on its own by the handlers that need it.
#[derive(Clone)]
pub struct Doors {
    producer: Producer,
    admin: Admin,
}

impl FromRef<Doors> for Producer {
    fn from_ref(doors: &Doors) -> Self {
        doors.producer.clone()
    }
}

impl FromRef<Doors> for Admin {
    fn from_ref(doors: &Doors) -> Self {
        doors.admin.clone()
    }
}

/// The facade's routes, built from [`ROUTES`].
///
/// axum takes one method router per path and one handler per method on it, so
/// bindings are merged before registration: `GET /v1/jobs` and `POST /v1/jobs`
/// are two bindings and one route, and `:pause` and `:resume` on a queue are
/// two bindings and one handler.
pub fn router(producer: Producer, admin: Admin) -> Router {
    let mut router = Router::new();
    let mut registered: Vec<&'static str> = Vec::new();
    for binding in ROUTES {
        let pattern = binding.pattern();
        if registered.contains(&pattern) {
            continue;
        }
        registered.push(pattern);
        let mut verbs: Vec<Verb> = Vec::new();
        let mut service = MethodRouter::new();
        for other in ROUTES.iter().filter(|other| other.pattern() == pattern) {
            if !verbs.contains(&other.verb()) {
                verbs.push(other.verb());
                service = service.merge(other.service());
            }
        }
        router = router.route(pattern, service);
    }
    router.with_state(Doors { producer, admin })
}

// ── Custom methods ───────────────────────────────────────────────────

/// `POST` on one resource: the custom method is the verb after the last path
/// parameter, and [`resolve`] says which binding it names.
///
/// A verb nobody implements — or none at all — is answered exactly as an
/// unrouted path is: there is no RPC at that address.
async fn custom_method(
    State(doors): State<Doors>,
    segment: Result<Path<String>, PathRejection>,
    parts: Parts,
) -> Response {
    let (binding, id) = match custom_target(&parts, segment) {
        Ok(target) => target,
        Err(error) => return error::refuse(error),
    };
    match binding {
        Binding::CancelJob => cancel_job(&doors.producer, &parts, id).await,
        Binding::PauseQueue => operator::pause_queue(&doors.admin, &parts, id).await,
        Binding::ResumeQueue => operator::resume_queue(&doors.admin, &parts, id).await,
        Binding::ReplayDeadLetter => operator::replay_dead_letter(&doors.admin, &parts, id).await,
        Binding::DeleteDeadLetter => operator::delete_dead_letter(&doors.admin, &parts, id).await,
        Binding::DrainWorker => operator::drain_worker(&doors.admin, &parts, id).await,
        Binding::DeletePeriodicTask => {
            operator::delete_periodic_task(&doors.admin, &parts, id).await
        }
        Binding::PausePeriodicTask => operator::pause_periodic_task(&doors.admin, &parts, id).await,
        Binding::ResumePeriodicTask => {
            operator::resume_periodic_task(&doors.admin, &parts, id).await
        }
        Binding::TriggerPeriodicTask => {
            operator::trigger_periodic_task(&doors.admin, &parts, id).await
        }
        // Listed rather than `_`, so a new custom method does not compile until
        // it is dispatched. `custom_target` never answers one of these: none of
        // them carries a verb after a parameter.
        Binding::Enqueue
        | Binding::EnqueueBatch
        | Binding::GetJob
        | Binding::ListJobs
        | Binding::QueueStats
        | Binding::NamespaceStats
        | Binding::SubmitWorkflow
        | Binding::GetWorkflowRun
        | Binding::ListQueues
        | Binding::GetThroughput
        | Binding::ListDeadLetters
        | Binding::GetDeadLetter
        | Binding::PurgeDeadLetters
        | Binding::ListWorkers
        | Binding::ListPeriodicTasks
        | Binding::GetPeriodicTask
        | Binding::PutPeriodicTask
        | Binding::ListOverrides
        | Binding::SetTaskOverride
        | Binding::ClearTaskOverride
        | Binding::SetQueueOverride
        | Binding::ClearQueueOverride => error::refuse(unrouted(&parts)),
    }
}

/// The custom-method binding a request reached, and the resource id with the
/// verb split off it.
fn custom_target(
    parts: &Parts,
    segment: Result<Path<String>, PathRejection>,
) -> Result<(Binding, String), WireError> {
    let segment = path_param(segment)?;
    let binding = resolve(&parts.method, parts.uri.path()).ok_or_else(|| unrouted(parts))?;
    let verb = binding.custom_verb().ok_or_else(|| unrouted(parts))?;
    // The decoded segment, so an id is percent-decoded exactly as a bare
    // `{param}` route's is.
    let id = segment
        .strip_suffix(verb)
        .and_then(|rest| rest.strip_suffix(':'))
        .filter(|id| !id.is_empty())
        .ok_or_else(|| unrouted(parts))?;
    Ok((binding, id.to_string()))
}

/// There is no RPC at this address.
fn unrouted(parts: &Parts) -> WireError {
    WireError::no_such_method(parts.method.as_str(), parts.uri.path())
}

// ── Producer handlers ────────────────────────────────────────────────
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

/// `CancelJob`, reached through [`custom_method`].
async fn cancel_job(producer: &Producer, parts: &Parts, job_id: String) -> Response {
    let request = match scoped(parts, pb::CancelJobRequest { job_id }) {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.cancel_job(request).await, write::cancel_job)
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
    let request = match prepare_namespace_stats(&parts) {
        Ok(request) => request,
        Err(error) => return error::refuse(error),
    };
    finish(producer.queue_stats(request).await, write::queue_stats)
}

/// An unset `queue` counts every queue in the namespace, which is the reason
/// this binding exists. It is never a way to reach another namespace: that
/// comes from the credential, and the request has no field for it.
fn prepare_namespace_stats(
    parts: &Parts,
) -> Result<tonic::Request<pb::QueueStatsRequest>, WireError> {
    let filter: read::QueueStats = query(parts)?;
    scoped(parts, filter.into_message())
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
/// The cap is [`PRODUCER_MAX_MESSAGE_BYTES`], the same number both gRPC
/// services are configured with, so the two doors cannot disagree about what is
/// too large.
pub(super) async fn decode<T: DeserializeOwned>(body: AxumBody) -> Result<T, WireError> {
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
pub(super) fn query<T: DeserializeOwned + Default>(parts: &Parts) -> Result<T, WireError> {
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
pub(super) fn path_param(param: Result<Path<String>, PathRejection>) -> Result<String, WireError> {
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
pub(super) fn scoped<T>(parts: &Parts, message: T) -> Result<tonic::Request<T>, WireError> {
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
pub(super) fn finish<T>(
    outcome: Result<tonic::Response<T>, Status>,
    render: fn(&T) -> Value,
) -> Response {
    match outcome {
        Ok(response) => error::ok(&render(response.get_ref())),
        Err(status) => error::response(&status),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::facade::descriptor;

    /// The packages this door transcodes, each in full.
    const SERVED: [Service; 2] = [Service::Producer, Service::Admin];

    /// Whether `binding` calls the RPC `rpc` of `package`.
    fn calls(binding: &Binding, package: &str, method: &str) -> bool {
        binding.rpc().service().package() == package && binding.rpc().as_str() == method
    }

    /// The check the issue asks for, and the one §11 fails the PR without:
    /// every RPC a served package declares is routed. Read off the descriptor,
    /// so there is no second list to keep in step — adding an RPC to the
    /// `.proto` fails here until it has a binding.
    #[test]
    fn every_served_rpc_has_a_route() {
        for service in SERVED {
            let declared = descriptor::rpcs(service.package());
            assert!(
                !declared.is_empty(),
                "{} declares no RPCs",
                service.package()
            );
            for rpc in declared {
                assert!(
                    ROUTES
                        .iter()
                        .any(|binding| calls(binding, service.package(), &rpc.method)),
                    "{}.{} has no route in the JSON facade",
                    rpc.service,
                    rpc.method
                );
            }
        }
    }

    /// The package constants the descriptor reader uses are the ones this table
    /// uses, so the two cannot quietly check different packages.
    #[test]
    fn the_served_packages_are_the_descriptors() {
        assert_eq!(Service::Producer.package(), descriptor::PRODUCER_PACKAGE);
        assert_eq!(Service::Admin.package(), descriptor::ADMIN_PACKAGE);
    }

    /// And nothing is routed that its package does not declare — a binding for
    /// an RPC that was removed, or misspelled, fails here.
    #[test]
    fn every_route_names_an_rpc_its_package_declares() {
        for binding in ROUTES {
            let service = binding.rpc().service();
            let declared = descriptor::rpcs(service.package());
            assert!(
                declared
                    .iter()
                    .any(|rpc| rpc.method == binding.rpc().as_str()
                        && format!("{}.{}", service.package(), rpc.service) == service.full_name()),
                "{:?} routes to {}, which {} does not declare",
                binding,
                binding.rpc().as_str(),
                service.full_name()
            );
        }
    }

    /// D15, stated as an iff so that adding an RPC needs no judgement call.
    #[test]
    fn a_get_serves_exactly_the_no_side_effects_rpcs() {
        for binding in ROUTES {
            let declared = descriptor::rpcs(binding.rpc().service().package());
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

    /// `Rpc::ALL` is the closed set the metrics layer labels from, so an RPC
    /// missing from it would be counted as `other`.
    #[test]
    fn every_routed_rpc_is_in_the_closed_set() {
        for binding in ROUTES {
            assert!(Rpc::ALL.contains(&binding.rpc()), "{binding:?}");
        }
        assert_eq!(
            Rpc::ALL.len(),
            SERVED
                .iter()
                .map(|service| descriptor::rpcs(service.package()).len())
                .sum::<usize>()
        );
    }

    /// The worker surface has different credentials and different failure
    /// modes; the facade transcodes two packages and not the third.
    ///
    /// Written by name as well as by package, so that it bites even if an
    /// executor RPC shared a method name with a served one.
    #[test]
    fn no_executor_rpc_is_reachable_over_http() {
        for rpc in descriptor::rpcs(descriptor::EXECUTOR_PACKAGE) {
            assert!(
                !ROUTES.iter().any(|binding| calls(
                    binding,
                    descriptor::EXECUTOR_PACKAGE,
                    &rpc.method
                )),
                "{}.{} is an executor RPC and must not have a route",
                rpc.service,
                rpc.method
            );
        }
        assert!(SERVED
            .iter()
            .all(|service| service.package() != descriptor::EXECUTOR_PACKAGE));
    }

    /// Two bindings may share a pattern (`GET` and `POST /v1/jobs`), and may
    /// share a pattern *and* a method only as two custom methods of one
    /// resource, which one handler dispatches by verb. Anything else would be
    /// two handlers for one address — axum would panic at startup, or worse,
    /// one of them would never be reached.
    #[test]
    fn bindings_sharing_a_method_on_a_pattern_are_custom_methods() {
        for (index, binding) in ROUTES.iter().enumerate() {
            for other in &ROUTES[index + 1..] {
                if binding.pattern() == other.pattern() && binding.verb() == other.verb() {
                    assert!(
                        binding.custom_verb().is_some() && other.custom_verb().is_some(),
                        "{binding:?} collides with {other:?}"
                    );
                    assert_ne!(binding.custom_verb(), other.custom_verb());
                }
            }
        }
    }

    /// The public path and the registered pattern differ exactly where matchit
    /// cannot express the path: a verb after a parameter.
    #[test]
    fn only_a_verb_after_a_parameter_registers_a_different_pattern() {
        for binding in ROUTES {
            match binding.custom_verb() {
                Some(verb) => {
                    assert_eq!(binding.verb(), Verb::Post, "{binding:?}");
                    assert_eq!(
                        format!("{}:{verb}", binding.pattern()),
                        binding.path(),
                        "{binding:?}"
                    );
                    assert!(binding.pattern().ends_with('}'), "{binding:?}");
                }
                None => assert_eq!(binding.path(), binding.pattern(), "{binding:?}"),
            }
        }
        assert_eq!(Binding::CancelJob.pattern(), "/v1/jobs/{job_id}");
        assert_eq!(Binding::PurgeDeadLetters.custom_verb(), None);
        assert_eq!(Binding::ClearQueueOverride.custom_verb(), None);
        assert_eq!(Binding::PauseQueue.custom_verb(), Some("pause"));
    }

    /// Every binding resolves from a concrete instance of its own path, and to
    /// itself — which is what the custom-method dispatcher and the metrics
    /// labels both rely on.
    #[test]
    fn every_binding_resolves_from_its_own_path() {
        for binding in ROUTES {
            let concrete = binding
                .path()
                .split('/')
                .map(|segment| match segment.strip_prefix('{') {
                    Some(rest) => {
                        let suffix = rest.split_once('}').map_or("", |(_, suffix)| suffix);
                        format!("x1{suffix}")
                    }
                    None => segment.to_string(),
                })
                .collect::<Vec<_>>()
                .join("/");
            let method = match binding.verb() {
                Verb::Get => http::Method::GET,
                Verb::Post => http::Method::POST,
            };
            assert_eq!(
                resolve(&method, &concrete),
                Some(*binding),
                "{concrete} resolved elsewhere"
            );
        }
    }

    /// The addresses that sit close to one another and must not be confused.
    #[test]
    fn neighbouring_admin_paths_resolve_apart() {
        let post = http::Method::POST;
        assert_eq!(
            resolve(&post, "/v1/admin/queues/emails:pause"),
            Some(Binding::PauseQueue)
        );
        assert_eq!(
            resolve(&post, "/v1/admin/queues/emails/override"),
            Some(Binding::SetQueueOverride)
        );
        assert_eq!(
            resolve(&post, "/v1/admin/queues/emails/override:clear"),
            Some(Binding::ClearQueueOverride)
        );
        assert_eq!(
            resolve(&post, "/v1/admin/deadLetters:purge"),
            Some(Binding::PurgeDeadLetters)
        );
        assert_eq!(
            resolve(&post, "/v1/admin/workers/w-1:drain"),
            Some(Binding::DrainWorker)
        );
        assert_eq!(resolve(&post, "/v1/admin/workers:drain"), None);
        assert_eq!(resolve(&post, "/v1/admin/queues/emails:drain"), None);
        assert_eq!(resolve(&post, "/v1/admin/queues/emails"), None);
        assert_eq!(resolve(&post, "/v1/admin/queues/:pause"), None);
        assert_eq!(
            resolve(&http::Method::GET, "/v1/admin/queues/emails:pause"),
            None
        );
    }

    /// The verb an annotation spells, as this table spells it.
    fn annotated_verb(verb: Verb) -> flexiq_openapi::Verb {
        match verb {
            Verb::Get => flexiq_openapi::Verb::Get,
            Verb::Post => flexiq_openapi::Verb::Post,
        }
    }

    /// Every `google.api.http` binding the served packages declare, with the
    /// package each came from.
    fn annotated() -> Vec<(Service, flexiq_openapi::Binding)> {
        SERVED
            .iter()
            .flat_map(|service| {
                flexiq_openapi::bindings(pb::FILE_DESCRIPTOR_SET, service.package())
                    .expect("the committed descriptor carries the http annotations")
                    .into_iter()
                    .map(|rule| (*service, rule))
            })
            .collect()
    }

    /// This table and the contract's annotations are one mapping written
    /// twice, and `contracts/openapi.json` is generated from the annotation
    /// half. So this is what stops that document describing a door nobody
    /// serves: change a path here without changing the `.proto` and the two
    /// tests below fail.
    #[test]
    fn every_binding_is_annotated_on_its_rpc() {
        let annotated = annotated();
        for binding in ROUTES {
            let rules: Vec<_> = annotated
                .iter()
                .filter(|(service, rule)| {
                    *service == binding.rpc().service()
                        && rule.method == binding.rpc().as_str()
                        && rule.path == binding.path()
                })
                .collect();
            assert_eq!(
                rules.len(),
                1,
                "{:?} is served at {}, which {} does not annotate on {}",
                binding,
                binding.path(),
                binding.rpc().service().package(),
                binding.rpc().as_str()
            );
            assert_eq!(
                rules[0].1.verb,
                annotated_verb(binding.verb()),
                "{binding:?} answers a different method than the contract annotates"
            );
        }
    }

    /// And the other direction: an annotation with no binding is a path the
    /// document would advertise and the router would answer `NO_SUCH_METHOD`.
    #[test]
    fn every_annotation_is_a_binding_this_table_serves() {
        for (service, rule) in annotated() {
            let bindings: Vec<_> = ROUTES
                .iter()
                .filter(|binding| {
                    binding.rpc().service() == service
                        && binding.rpc().as_str() == rule.method
                        && binding.path() == rule.path
                        && annotated_verb(binding.verb()) == rule.verb
                })
                .collect();
            assert_eq!(
                bindings.len(),
                1,
                "{} {} is annotated on {}.{} and is not served",
                rule.verb.as_str().to_uppercase(),
                rule.path,
                rule.service,
                rule.method
            );
        }
    }

    /// A `GET` carries no body on either door, so an annotation that gave one
    /// a request body would describe a call the handler cannot read.
    #[test]
    fn no_annotated_get_declares_a_body() {
        for (_, rule) in annotated() {
            assert!(
                rule.verb != flexiq_openapi::Verb::Get || rule.body.is_none(),
                "GET {} declares a request body",
                rule.path
            );
        }
    }

    /// A custom method's handler reads no body — its only field is the id in
    /// the path — so the contract must not declare one for it either.
    #[test]
    fn no_custom_method_declares_a_body() {
        for (service, rule) in annotated() {
            let custom = ROUTES.iter().any(|binding| {
                binding.rpc().service() == service
                    && binding.path() == rule.path
                    && binding.custom_verb().is_some()
            });
            assert!(
                !custom || rule.body.is_none(),
                "{} declares a body the custom-method handler never reads",
                rule.path
            );
        }
    }

    /// The worker surface is not transcoded, and an annotation is the only way
    /// it could become so by accident.
    #[test]
    fn no_executor_rpc_is_annotated_with_a_path() {
        let annotated =
            flexiq_openapi::bindings(pb::FILE_DESCRIPTOR_SET, descriptor::EXECUTOR_PACKAGE)
                .expect("the committed descriptor is readable");
        assert!(
            annotated.is_empty(),
            "{} annotates HTTP paths: {annotated:?}",
            descriptor::EXECUTOR_PACKAGE
        );
    }
}
