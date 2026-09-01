//! End-to-end: `flexiq.v1.ProducerService` over a real socket.
//!
//! The client is the generated one over a real channel, so what these tests
//! exercise is the wire — encoding, presence, the error details — and not the
//! handler functions with the transport cut out. The rules being pinned are the
//! ones a client depends on and cannot see: a listing carries no payload, a
//! blob not asked for is absent rather than empty, another namespace's job is
//! indistinguishable from a missing one, and the same `unique_key` twice
//! returns one job id twice while still telling the two calls apart.
#![cfg(feature = "grpc")]

mod support;

use flexiq_core::job::{now_millis, NewJob};
use flexiq_core::storage::Storage;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{
    enqueue_batch_item_result, enqueue_request, CancelJobRequest, EnqueueBatchRequest,
    EnqueueOptions, EnqueueRequest, GetJobRequest, JobStatus, ListJobsRequest, QueueStatsRequest,
};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use tonic::transport::Channel;
use tonic::Code;
use tonic_types::StatusExt;

use support::{temp_storage, TempStorage};

/// The one namespace this door serves.
const NAMESPACE: &str = "grpc-producer-tests";

/// A running listener and a client pointed at it.
struct Harness {
    client: ProducerServiceClient<Channel>,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Harness {
    async fn start(label: &str) -> Self {
        let storage = temp_storage(label);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig {
            listen: ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            namespace: NAMESPACE.to_string(),
        })
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener knows what it bound");
        let served = tokio::spawn(listener.serve((*storage).clone(), shutdown.clone()));

        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("a valid endpoint")
            .connect()
            .await
            .expect("the listener must accept a connection");

        Self {
            client: ProducerServiceClient::new(channel),
            storage,
            shutdown,
            served,
        }
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

fn request(task: &str, payload: Vec<u8>, options: EnqueueOptions) -> EnqueueRequest {
    EnqueueRequest {
        task_name: task.to_string(),
        body: Some(enqueue_request::Body::Raw(payload)),
        options: Some(options),
    }
}

fn in_queue(queue: &str) -> EnqueueOptions {
    EnqueueOptions {
        queue: queue.to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn a_job_enqueued_over_grpc_is_readable_by_id() {
    let mut harness = Harness::start("grpc-producer-roundtrip").await;

    let enqueued = harness
        .client
        .enqueue(request("send_email", vec![1, 2, 3], in_queue("emails")))
        .await
        .expect("enqueue")
        .into_inner();
    assert!(!enqueued.deduplicated);
    let job = enqueued.job.expect("a response carries its job");
    assert_eq!(job.queue, "emails");
    assert_eq!(job.task_name, "send_email");
    assert_eq!(job.status, JobStatus::Pending as i32);
    // Always the caller's own, and output-only.
    assert_eq!(job.namespace, NAMESPACE);
    // The response to a write is not a read: the producer already has these.
    assert_eq!(job.payload, None);
    assert_eq!(job.result, None);

    // The job is a real row an unmodified worker would claim: same database,
    // same namespace, no gRPC involved in the read.
    let stored = harness
        .storage
        .get_job(&job.id, Some(NAMESPACE))
        .expect("read")
        .expect("the job exists in storage");
    assert_eq!(stored.payload, vec![1, 2, 3]);

    let read = harness
        .client
        .get_job(GetJobRequest {
            job_id: job.id.clone(),
            include_payload: true,
            include_result: false,
        })
        .await
        .expect("get_job")
        .into_inner()
        .job
        .expect("a response carries its job");
    assert_eq!(read.payload, Some(vec![1, 2, 3]));
    // Not asked for, so absent — and absent is not the same answer as empty.
    assert_eq!(read.result, None);

    harness.stop().await;
}

#[tokio::test]
async fn the_same_unique_key_twice_returns_one_job_id_twice() {
    let mut harness = Harness::start("grpc-producer-unique").await;

    let options = EnqueueOptions {
        unique_key: Some("nightly-report".to_string()),
        ..in_queue("reports")
    };

    let first = harness
        .client
        .enqueue(request("report", vec![1], options.clone()))
        .await
        .expect("first enqueue")
        .into_inner();
    let second = harness
        .client
        .enqueue(request("report", vec![2], options))
        .await
        .expect("second enqueue")
        .into_inner();

    assert_eq!(
        first.job.expect("job").id,
        second.job.expect("job").id,
        "a unique_key must not enqueue a second job"
    );
    // The whole reason a producer sets a unique_key: it can still tell the two
    // calls apart.
    assert!(!first.deduplicated);
    assert!(second.deduplicated);

    harness.stop().await;
}

#[tokio::test]
async fn a_listing_pages_and_never_carries_a_payload() {
    let mut harness = Harness::start("grpc-producer-listing").await;

    for index in 0..5 {
        harness
            .client
            .enqueue(request(
                "indexed",
                vec![index as u8; 64],
                in_queue("listing"),
            ))
            .await
            .expect("enqueue");
    }

    let mut seen = Vec::new();
    let mut page_token = String::new();
    loop {
        let page = harness
            .client
            .list_jobs(ListJobsRequest {
                status: Some(JobStatus::Pending as i32),
                queue: Some("listing".to_string()),
                task_name: None,
                page_size: 2,
                page_token: page_token.clone(),
            })
            .await
            .expect("list_jobs")
            .into_inner();

        for job in &page.jobs {
            // Without this rule a page of a hundred jobs is a page of a hundred
            // payloads.
            assert_eq!(job.payload, None, "a listing must not carry payloads");
            assert_eq!(job.result, None);
            seen.push(job.id.clone());
        }

        page_token = page.next_page_token;
        if page_token.is_empty() {
            break;
        }
    }

    assert_eq!(
        seen.len(),
        5,
        "every job must appear exactly once: {seen:?}"
    );
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 5, "the cursor must not repeat a row");

    harness.stop().await;
}

#[tokio::test]
async fn a_job_in_another_namespace_is_indistinguishable_from_a_missing_one() {
    let harness = Harness::start("grpc-producer-namespace").await;
    let mut client = harness.client.clone();

    // Written straight to storage, in a namespace this door does not serve.
    let elsewhere = harness
        .storage
        .enqueue(NewJob {
            queue: "q".into(),
            task_name: "t".into(),
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
            namespace: Some("some-other-tenant".to_string()),
            debounce_key: None,
        })
        .expect("enqueue elsewhere");

    let existing = client
        .get_job(GetJobRequest {
            job_id: elsewhere.id.clone(),
            include_payload: false,
            include_result: false,
        })
        .await
        .expect_err("a cross-namespace read must not succeed");
    let missing = client
        .get_job(GetJobRequest {
            job_id: "0192f3c4-0000-7000-8000-000000000000".to_string(),
            include_payload: false,
            include_result: false,
        })
        .await
        .expect_err("a missing job must not succeed");

    // Identical answers, deliberately: anything that told them apart would be
    // an oracle for ids outside the caller's namespace. The messages differ
    // only where they echo the id the caller itself sent.
    assert_eq!(existing.code(), Code::NotFound);
    assert_eq!(missing.code(), existing.code());
    for (error, id) in [
        (&existing, elsewhere.id.as_str()),
        (&missing, "0192f3c4-0000-7000-8000-000000000000"),
    ] {
        assert_eq!(error.message(), format!("job not found: {id}"));
        assert_eq!(
            error.get_details_error_info().expect("an ErrorInfo").reason,
            reason::JOB_NOT_FOUND
        );
    }

    // A listing does not leak it either.
    let listed = client
        .list_jobs(ListJobsRequest::default())
        .await
        .expect("list_jobs")
        .into_inner();
    assert!(
        !listed.jobs.iter().any(|job| job.id == elsewhere.id),
        "a listing must be scoped to the door's own namespace"
    );

    harness.stop().await;
}

#[tokio::test]
async fn cancelling_describes_the_resulting_state_and_is_safe_to_repeat() {
    let mut harness = Harness::start("grpc-producer-cancel").await;

    let job = harness
        .client
        .enqueue(request("cancel_me", vec![], in_queue("cancels")))
        .await
        .expect("enqueue")
        .into_inner()
        .job
        .expect("job");

    let first = harness
        .client
        .cancel_job(CancelJobRequest {
            job_id: job.id.clone(),
        })
        .await
        .expect("cancel")
        .into_inner()
        .job
        .expect("job");
    assert_eq!(first.status, JobStatus::Cancelled as i32);

    // A bool would say "I did not cancel it" here, about a job it cancelled a
    // moment ago. The resulting state says the same thing both times.
    let second = harness
        .client
        .cancel_job(CancelJobRequest { job_id: job.id })
        .await
        .expect("cancel again")
        .into_inner()
        .job
        .expect("job");
    assert_eq!(second.status, first.status);

    let missing = harness
        .client
        .cancel_job(CancelJobRequest {
            job_id: "0192f3c4-0000-7000-8000-000000000000".to_string(),
        })
        .await
        .expect_err("cancelling nothing is NOT_FOUND");
    assert_eq!(missing.code(), Code::NotFound);

    harness.stop().await;
}

#[tokio::test]
async fn a_batch_answers_per_item_and_carries_deduplication() {
    let mut harness = Harness::start("grpc-producer-batch").await;

    let keyed = |key: &str| EnqueueOptions {
        unique_key: Some(key.to_string()),
        ..in_queue("batched")
    };

    let first = harness
        .client
        .enqueue_batch(EnqueueBatchRequest {
            items: vec![
                request("a", vec![1], keyed("batch-a")),
                request("b", vec![2], keyed("batch-b")),
                request("c", vec![3], in_queue("batched")),
            ],
        })
        .await
        .expect("enqueue_batch")
        .into_inner();
    assert_eq!(first.results.len(), 3);
    for result in &first.results {
        let Some(enqueue_batch_item_result::Outcome::Enqueued(enqueued)) = &result.outcome else {
            panic!("every item must have landed: {result:?}");
        };
        assert!(!enqueued.deduplicated);
    }

    // The second batch repeats one key, so exactly one item deduplicates — the
    // per-item answer a producer needs just as much as the per-call one.
    let second = harness
        .client
        .enqueue_batch(EnqueueBatchRequest {
            items: vec![
                request("a", vec![1], keyed("batch-a")),
                request("d", vec![4], keyed("batch-d")),
            ],
        })
        .await
        .expect("enqueue_batch")
        .into_inner();
    let flags: Vec<bool> = second
        .results
        .iter()
        .map(|result| match &result.outcome {
            Some(enqueue_batch_item_result::Outcome::Enqueued(enqueued)) => enqueued.deduplicated,
            other => panic!("expected an enqueued arm, got {other:?}"),
        })
        .collect();
    assert_eq!(flags, vec![true, false]);

    harness.stop().await;
}

#[tokio::test]
async fn a_malformed_request_names_the_item_it_came_from() {
    let mut harness = Harness::start("grpc-producer-invalid").await;

    // No body arm at all. An absent body is not an empty one.
    let error = harness
        .client
        .enqueue(EnqueueRequest {
            task_name: "t".to_string(),
            body: None,
            options: None,
        })
        .await
        .expect_err("a request with no body must be refused");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        error.get_details_error_info().expect("an ErrorInfo").reason,
        reason::INVALID_REQUEST
    );

    // In a batch the same failure carries its position, because a client that
    // has to guess which item broke has to resend all of them.
    let error = harness
        .client
        .enqueue_batch(EnqueueBatchRequest {
            items: vec![
                request("fine", vec![], in_queue("q")),
                EnqueueRequest {
                    task_name: "t".to_string(),
                    body: None,
                    options: None,
                },
            ],
        })
        .await
        .expect_err("a malformed item must be refused");
    let info = error.get_details_error_info().expect("an ErrorInfo");
    assert_eq!(info.reason, reason::INVALID_REQUEST);
    assert_eq!(info.metadata[reason::KEY_INDEX], "1");

    // And nothing was written: the batch is refused before it is submitted.
    let stats = harness
        .client
        .queue_stats(QueueStatsRequest {
            queue: Some("q".to_string()),
        })
        .await
        .expect("queue_stats")
        .into_inner();
    assert_eq!(stats.pending, 0);

    harness.stop().await;
}

#[tokio::test]
async fn stats_count_one_queue_or_the_whole_namespace() {
    let mut harness = Harness::start("grpc-producer-stats").await;

    for _ in 0..3 {
        harness
            .client
            .enqueue(request("t", vec![], in_queue("counted")))
            .await
            .expect("enqueue");
    }
    harness
        .client
        .enqueue(request("t", vec![], in_queue("other")))
        .await
        .expect("enqueue");

    let one = harness
        .client
        .queue_stats(QueueStatsRequest {
            queue: Some("counted".to_string()),
        })
        .await
        .expect("queue_stats")
        .into_inner();
    assert_eq!(one.pending, 3);

    let all = harness
        .client
        .queue_stats(QueueStatsRequest { queue: None })
        .await
        .expect("queue_stats")
        .into_inner();
    assert_eq!(all.pending, 4);

    harness.stop().await;
}
