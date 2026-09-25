//! End-to-end: `ProducerService.WatchJobs` over a real socket.
//!
//! The rules pinned here are the ones the issue leads with: a watch on a job
//! that is already terminal sends it and closes; a missing id and another
//! namespace's read the same; a transition this process makes arrives at once
//! and one another process makes arrives by the re-read; a queue watch resumes
//! from its cursor; and a stream is a bounded resource.
#![cfg(feature = "grpc")]

mod support;

use std::time::Duration;

use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::storage::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::config::watch::WatchConfig;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{
    enqueue_request, watch_jobs_request, watch_jobs_response, CancelJobRequest, EnqueueOptions,
    EnqueueRequest, JobStatus, JobTransition, JobTransitionKind, WatchJobIds, WatchJobsRequest,
    WatchJobsResponse,
};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use tonic::transport::Channel;
use tonic::{Code, Status, Streaming};
use tonic_types::StatusExt;

use support::{mint_token, temp_storage, temp_workflows, Bearer, TempStorage};

const NAMESPACE: &str = "grpc-watch-tests";

/// How long any one expectation may take. A failure deadline, not a delay.
const WAIT: Duration = Duration::from_secs(20);

type Client =
    ProducerServiceClient<tonic::service::interceptor::InterceptedService<Channel, Bearer>>;

struct Harness {
    client: Client,
    channel: Channel,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str, watch: WatchConfig) -> Self {
        let storage = temp_storage(label);
        let token = mint_token(&storage, NAMESPACE, flexiq_server::tokens::ScopeSet::ALL);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig {
            watch,
            ..GrpcConfig::new(
                ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
                NAMESPACE,
            )
        })
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener knows its port");
        let served = tokio::spawn(listener.serve(
            (*storage).clone(),
            temp_workflows(&storage),
            None,
            shutdown.clone(),
        ));
        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("a valid endpoint")
            .connect()
            .await
            .expect("the listener must accept a connection");
        Self {
            client: ProducerServiceClient::with_interceptor(channel.clone(), Bearer::new(&token)),
            channel,
            storage,
            shutdown,
            served,
        }
    }

    /// A second client, presenting a credential of its own.
    fn other_credential(&self) -> Client {
        let token = mint_token(
            &self.storage,
            NAMESPACE,
            flexiq_server::tokens::ScopeSet::ALL,
        );
        ProducerServiceClient::with_interceptor(self.channel.clone(), Bearer::new(&token))
    }

    async fn enqueue(&mut self, queue: &str) -> String {
        self.client
            .enqueue(EnqueueRequest {
                task_name: "task".into(),
                body: Some(enqueue_request::Body::Raw(vec![0x02, 0x82, 0x80, 0xa0])),
                options: Some(EnqueueOptions {
                    queue: queue.into(),
                    ..Default::default()
                }),
            })
            .await
            .expect("enqueue")
            .into_inner()
            .job
            .expect("a job")
            .id
    }

    async fn cancel(&mut self, id: &str) {
        self.client
            .cancel_job(CancelJobRequest { job_id: id.into() })
            .await
            .expect("cancel");
    }

    async fn watch(
        &mut self,
        request: WatchJobsRequest,
    ) -> Result<Streaming<WatchJobsResponse>, Status> {
        self.client
            .watch_jobs(request)
            .await
            .map(tonic::Response::into_inner)
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

fn ids(ids: &[&str]) -> WatchJobsRequest {
    WatchJobsRequest {
        target: Some(watch_jobs_request::Target::JobIds(WatchJobIds {
            job_ids: ids.iter().map(|id| id.to_string()).collect(),
        })),
        resume_cursor: String::new(),
    }
}

fn queue(name: &str, resume: &str) -> WatchJobsRequest {
    WatchJobsRequest {
        target: Some(watch_jobs_request::Target::Queue(name.into())),
        resume_cursor: resume.into(),
    }
}

/// The next item, failing the test if none arrives in time.
async fn next(
    stream: &mut Streaming<WatchJobsResponse>,
) -> Option<Result<WatchJobsResponse, Status>> {
    tokio::time::timeout(WAIT, stream.message())
        .await
        .expect("the stream must answer in time")
        .transpose()
}

async fn transition(stream: &mut Streaming<WatchJobsResponse>) -> (JobTransition, String) {
    let response = next(stream)
        .await
        .expect("an item, not the end")
        .expect("an item, not an error");
    match response.item {
        Some(watch_jobs_response::Item::Transition(t)) => (t, response.cursor),
        other => panic!("expected a transition, got {other:?}"),
    }
}

async fn closes_ok(stream: &mut Streaming<WatchJobsResponse>) {
    assert!(next(stream).await.is_none(), "the stream must end with OK");
}

fn reason_of(status: &Status) -> String {
    status
        .get_error_details()
        .error_info()
        .map(|info| info.reason.clone())
        .unwrap_or_default()
}

#[tokio::test]
async fn a_watch_on_a_terminal_job_sends_it_and_closes() {
    let mut harness = Harness::start("watch-terminal", WatchConfig::default()).await;
    let id = harness.enqueue("q").await;
    harness.cancel(&id).await;

    let mut stream = harness.watch(ids(&[&id])).await.expect("watch");
    let (t, cursor) = transition(&mut stream).await;
    assert_eq!(t.job_id, id);
    assert_eq!(t.kind, JobTransitionKind::Snapshot as i32);
    assert_eq!(t.status, JobStatus::Cancelled as i32);
    assert!(t.terminal);
    assert!(cursor.is_empty(), "an id watch hands out no cursor");
    closes_ok(&mut stream).await;
    harness.stop().await;
}

#[tokio::test]
async fn a_missing_id_and_another_namespaces_read_the_same() {
    let mut harness = Harness::start("watch-not-found", WatchConfig::default()).await;
    let foreign = harness
        .storage
        .enqueue(NewJob {
            queue: "q".into(),
            task_name: "task".into(),
            payload: vec![9],
            priority: 0,
            scheduled_at: now_millis(),
            max_retries: 0,
            timeout_ms: 1_000,
            unique_key: None,
            metadata: None,
            notes: None,
            depends_on: vec![],
            expires_at: None,
            result_ttl_ms: None,
            namespace: Some("someone-else".into()),
            debounce_key: None,
        })
        .expect("seed")
        .id;

    let mut stream = harness
        .watch(ids(&["no-such-job", &foreign]))
        .await
        .expect("watch");
    let mut seen = Vec::new();
    for _ in 0..2 {
        let response = next(&mut stream).await.expect("an item").expect("no error");
        match response.item {
            Some(watch_jobs_response::Item::NotFoundJobId(id)) => seen.push(id),
            other => panic!("expected not_found, got {other:?}"),
        }
    }
    assert_eq!(seen, ["no-such-job", foreign.as_str()]);
    closes_ok(&mut stream).await;
    harness.stop().await;
}

#[tokio::test]
async fn a_cancel_on_this_server_reaches_an_open_watch() {
    let mut harness = Harness::start("watch-live", WatchConfig::default()).await;
    let id = harness.enqueue("q").await;

    let mut stream = harness.watch(ids(&[&id])).await.expect("watch");
    let (snapshot, _) = transition(&mut stream).await;
    assert_eq!(snapshot.status, JobStatus::Pending as i32);
    assert!(!snapshot.terminal);

    harness.cancel(&id).await;
    let (cancelled, _) = transition(&mut stream).await;
    assert_eq!(cancelled.kind, JobTransitionKind::Cancelled as i32);
    assert!(cancelled.terminal);
    closes_ok(&mut stream).await;
    harness.stop().await;
}

#[tokio::test]
async fn a_job_another_process_finished_arrives_by_the_re_read() {
    let watch = WatchConfig {
        reconcile_interval: Duration::from_secs(1),
        ..WatchConfig::default()
    };
    let mut harness = Harness::start("watch-reconcile", watch).await;
    let id = harness.enqueue("elsewhere").await;
    let mut stream = harness.watch(ids(&[&id])).await.expect("watch");
    let (snapshot, _) = transition(&mut stream).await;
    assert!(!snapshot.terminal);

    // Straight through storage, as a worker in another process would: nothing
    // this server emits hears it.
    let claimed = harness
        .storage
        .dequeue("elsewhere", now_millis() + 1_000, Some(NAMESPACE))
        .expect("dequeue")
        .expect("the job is due");
    assert_eq!(claimed.id, id);
    harness
        .storage
        .complete(&id, None, Some(NAMESPACE))
        .expect("complete");

    let (last, _) = loop {
        let (t, cursor) = transition(&mut stream).await;
        if t.terminal {
            break (t, cursor);
        }
        // A re-read may catch the job running first.
        assert_eq!(t.status, JobStatus::Running as i32);
    };
    assert_eq!(last.kind, JobTransitionKind::Snapshot as i32);
    assert_eq!(last.status, JobStatus::Complete as i32);
    closes_ok(&mut stream).await;
    harness.stop().await;
}

#[tokio::test]
async fn a_queue_watch_follows_its_queue_and_resumes_from_a_cursor() {
    let mut harness = Harness::start("watch-queue", WatchConfig::default()).await;
    let mut stream = harness.watch(queue("orders", "")).await.expect("watch");
    let first = harness.enqueue("orders").await;
    harness.enqueue("unrelated").await;
    let second = harness.enqueue("orders").await;

    let (t1, cursor1) = transition(&mut stream).await;
    let (t2, _) = transition(&mut stream).await;
    assert_eq!(
        (t1.job_id.as_str(), t2.job_id.as_str()),
        (first.as_str(), second.as_str())
    );
    assert_eq!(t1.kind, JobTransitionKind::Enqueued as i32);
    drop(stream);

    // Reconnect after the first item: the second comes back, then new work.
    let third = harness.enqueue("orders").await;
    let mut resumed = harness
        .watch(queue("orders", &cursor1))
        .await
        .expect("resume");
    assert_eq!(transition(&mut resumed).await.0.job_id, second);
    assert_eq!(transition(&mut resumed).await.0.job_id, third);
    drop(resumed);
    harness.stop().await;
}

#[tokio::test]
async fn a_cursor_this_server_did_not_issue_is_refused() {
    let mut harness = Harness::start("watch-cursor", WatchConfig::default()).await;

    let garbled = harness
        .watch(queue("q", "not-a-cursor"))
        .await
        .expect_err("refused");
    assert_eq!(garbled.code(), Code::InvalidArgument);
    assert_eq!(reason_of(&garbled), reason::INVALID_REQUEST);

    // Well-formed, but numbered by some other process.
    let foreign = flexiq_server::grpc::producer::watch::cursor::encode(0, 0);
    let expired = harness
        .watch(queue("q", &foreign))
        .await
        .expect_err("refused");
    assert_eq!(expired.code(), Code::FailedPrecondition);
    assert_eq!(reason_of(&expired), reason::WATCH_CURSOR_EXPIRED);

    let mut on_ids = ids(&["x"]);
    on_ids.resume_cursor = foreign;
    let refused = harness.watch(on_ids).await.expect_err("refused");
    assert_eq!(refused.code(), Code::InvalidArgument);
    harness.stop().await;
}

#[tokio::test]
async fn a_credential_holds_at_most_its_cap_of_watches() {
    let watch = WatchConfig {
        max_per_credential: 1,
        ..WatchConfig::default()
    };
    let mut harness = Harness::start("watch-quota", watch).await;
    let held = harness
        .watch(queue("q", ""))
        .await
        .expect("the first watch");

    let refused = harness
        .watch(queue("q", ""))
        .await
        .expect_err("over the cap");
    assert_eq!(refused.code(), Code::ResourceExhausted);
    assert_eq!(reason_of(&refused), reason::WATCH_LIMIT);

    let mut other = harness.other_credential();
    let _theirs = other
        .watch_jobs(queue("q", ""))
        .await
        .expect("the cap is per credential");

    // Ending the stream gives the slot back, once the server notices.
    drop(held);
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match harness.watch(queue("q", "")).await {
            Ok(_) => break,
            Err(status) if reason_of(&status) == reason::WATCH_LIMIT => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the slot was never released"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(status) => panic!("unexpected refusal {status:?}"),
        }
    }
    harness.stop().await;
}

#[tokio::test]
async fn a_shutdown_ends_an_open_watch_unavailable() {
    let mut harness = Harness::start("watch-shutdown", WatchConfig::default()).await;
    let mut stream = harness.watch(queue("q", "")).await.expect("watch");
    harness.shutdown.trigger();
    let status = next(&mut stream)
        .await
        .expect("an error, not a clean end")
        .expect_err("an error, not an item");
    assert_eq!(status.code(), Code::Unavailable);
    assert_eq!(reason_of(&status), reason::SHUTTING_DOWN);
    harness.stop().await;
}
