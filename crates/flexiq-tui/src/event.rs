//! Background worker thread + the command/message protocol between it and the
//! UI thread. Storage calls block, so they run here; the UI thread only ever
//! drains [`Msg`] values and stays responsive.

use std::sync::mpsc::{Receiver, Sender};
use std::thread::{self, JoinHandle};

use flexiq_core::JobStatus;

use crate::source::{
    DagNode, DataSource, DeadRow, JobDetail, JobRow, StatsSnapshot, WorkerView, WorkflowRunRow,
};

/// A data request for the active view.
pub enum FetchReq {
    Stats,
    Jobs(Option<JobStatus>),
    Dead,
    Workers,
    Runs,
    JobDetail(String),
    WorkflowDag {
        run_id: String,
        definition_id: String,
    },
}

/// A mutating operation, carrying enough context to describe itself in the
/// confirmation prompt.
#[derive(Clone)]
pub enum PendingAction {
    CancelJob { id: String },
    ReplayJob { id: String },
    RetryDead { id: String },
    DeleteDead { id: String },
    PurgeDead,
    PauseQueue(String),
    ResumeQueue(String),
}

impl PendingAction {
    /// Human-readable confirmation prompt.
    pub fn prompt(&self) -> String {
        match self {
            PendingAction::CancelJob { id } => format!("cancel job {}?", short(id)),
            PendingAction::ReplayJob { id } => format!("replay job {} as a new job?", short(id)),
            PendingAction::RetryDead { id } => format!("retry dead-letter {}?", short(id)),
            PendingAction::DeleteDead { id } => format!("delete dead-letter {}?", short(id)),
            PendingAction::PurgeDead => "purge ALL dead-letter entries?".to_string(),
            PendingAction::PauseQueue(q) => format!("pause queue '{q}'?"),
            PendingAction::ResumeQueue(q) => format!("resume queue '{q}'?"),
        }
    }
}

/// UI → worker.
pub enum Cmd {
    Fetch(FetchReq),
    Act(PendingAction),
    Shutdown,
}

/// Worker → UI.
pub enum Msg {
    Stats(StatsSnapshot),
    Jobs(Vec<JobRow>),
    Dead(Vec<DeadRow>),
    Workers(Vec<WorkerView>),
    Runs(Vec<WorkflowRunRow>),
    /// Detail responses echo the requested id so the UI can drop stale results
    /// for a selection the user has already moved off. `JobDetail` is boxed —
    /// it dwarfs the other variants.
    JobDetail(String, Option<Box<JobDetail>>),
    WorkflowDag(String, Vec<DagNode>),
    ActionOk(String),
    Error(String),
}

/// Spawn the worker. It owns the [`DataSource`] and services commands until a
/// [`Cmd::Shutdown`] (or the command channel closing).
pub fn spawn_worker(
    source: Box<dyn DataSource>,
    cmd_rx: Receiver<Cmd>,
    msg_tx: Sender<Msg>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        while let Ok(cmd) = cmd_rx.recv() {
            match cmd {
                Cmd::Shutdown => break,
                Cmd::Fetch(req) => handle_fetch(source.as_ref(), req, &msg_tx),
                Cmd::Act(action) => handle_act(source.as_ref(), action, &msg_tx),
            }
        }
    })
}

fn handle_fetch(source: &dyn DataSource, req: FetchReq, tx: &Sender<Msg>) {
    let msg = match req {
        FetchReq::Stats => source.stats().map(Msg::Stats),
        FetchReq::Jobs(status) => source.jobs(status, 200).map(Msg::Jobs),
        FetchReq::Dead => source.dead_letters(200).map(Msg::Dead),
        FetchReq::Workers => source.workers().map(Msg::Workers),
        FetchReq::Runs => source.workflow_runs(200).map(Msg::Runs),
        FetchReq::JobDetail(id) => source
            .job_detail(&id)
            .map(|d| Msg::JobDetail(id, d.map(Box::new))),
        FetchReq::WorkflowDag {
            run_id,
            definition_id,
        } => source
            .workflow_dag(&run_id, &definition_id)
            .map(|dag| Msg::WorkflowDag(run_id, dag)),
    };
    let _ = tx.send(msg.unwrap_or_else(|e| Msg::Error(e.to_string())));
}

fn handle_act(source: &dyn DataSource, action: PendingAction, tx: &Sender<Msg>) {
    let result = match action {
        PendingAction::CancelJob { id } => source.cancel(&id).map(|ok| {
            if ok {
                format!("cancelled {}", short(&id))
            } else {
                format!("no-op: {} already gone or wrong state", short(&id))
            }
        }),
        PendingAction::ReplayJob { id } => source
            .replay(&id)
            .map(|new_id| format!("replayed {} → {}", short(&id), short(&new_id))),
        PendingAction::RetryDead { id } => source
            .retry_dead(&id)
            .map(|new_id| format!("retried {} → {}", short(&id), short(&new_id))),
        PendingAction::DeleteDead { id } => source.delete_dead(&id).map(|ok| {
            if ok {
                format!("deleted dead-letter {}", short(&id))
            } else {
                format!("dead-letter {} not found", short(&id))
            }
        }),
        PendingAction::PurgeDead => source
            .purge_dead()
            .map(|n| format!("purged {n} dead-letters")),
        PendingAction::PauseQueue(q) => source.pause_queue(&q).map(|()| format!("paused '{q}'")),
        PendingAction::ResumeQueue(q) => source.resume_queue(&q).map(|()| format!("resumed '{q}'")),
    };
    let _ = tx.send(match result {
        Ok(m) => Msg::ActionOk(m),
        Err(e) => Msg::Error(e.to_string()),
    });
}

/// Short id form for prompts/notices (first 8 chars of a UUID).
fn short(id: &str) -> String {
    id.chars().take(8).collect()
}
