//! `Enqueue` and `EnqueueBatch`.
//!
//! Uniqueness, debouncing and batching are options and shapes here, not verbs:
//! there is one `Enqueue`, and which storage entry point it reaches is decided
//! by what the request's options carry. The SDKs expose one `enqueue` with
//! options for the same reason, and the wire should not expose the dispatch
//! inside it.

use flexiq_core::job::{now_millis, Job, NewJob};
use flexiq_core::storage::Storage;
use flexiq_core::StorageBackend;
use tonic::{Response, Status};

use super::convert::{self, Blobs};
use super::Producer;
use crate::grpc::blocking::on_storage;
use crate::grpc::pb;
use crate::grpc::status::WireError;

/// Submit one job.
pub async fn one(
    producer: &Producer,
    request: pb::EnqueueRequest,
) -> Result<Response<pb::EnqueueResponse>, Status> {
    let prepared = prepare(request, producer.namespace())?;
    let (job, deduplicated) =
        on_storage(producer.storage(), move |storage| prepared.submit(storage)).await?;

    Ok(Response::new(pb::EnqueueResponse {
        // A producer that just submitted a job already has the payload it sent,
        // and asking for it back would double every enqueue's response size.
        job: Some(convert::job_to_wire(job, Blobs::NONE)),
        deduplicated,
    }))
}

/// Submit many jobs, one result per input item in input order.
///
/// The failure shape follows the backend rather than papering over it, because
/// no promise of atomicity would hold on both:
///
/// * where a batch is one transaction, one item's failure rolls back every
///   insert, so the **RPC** fails — returning the earlier items as `enqueued`
///   would report jobs that do not exist;
/// * where a batch can partially apply, the RPC succeeds and an `error` arm
///   means that one item did not land.
///
/// A client that treats an `enqueued` arm as durable is correct under both,
/// which is the property the split exists to give it.
pub async fn batch(
    producer: &Producer,
    request: pb::EnqueueBatchRequest,
) -> Result<Response<pb::EnqueueBatchResponse>, Status> {
    if request.items.is_empty() {
        return Ok(Response::new(pb::EnqueueBatchResponse {
            results: Vec::new(),
        }));
    }

    // Request-shape failures are attributable to one item, so they name it —
    // and they are refused before anything is written, whatever the backend.
    let mut prepared = Vec::with_capacity(request.items.len());
    for (index, item) in request.items.into_iter().enumerate() {
        let item = prepare(item, producer.namespace()).map_err(|error| error.at_index(index))?;
        // Storage has no batched debounce, so honouring one here would mean
        // leaving the batch to submit the rest — which silently costs the
        // transactional backends the atomicity that is the reason to send a
        // batch at all. Refusing says so instead of trading it away quietly.
        if matches!(item.dispatch, Dispatch::Debounced(_)) {
            return Err(WireError::invalid_request(
                "options.debounce is not available in a batch; enqueue a debounced job on its own",
            )
            .at_index(index)
            .into());
        }
        prepared.push(item);
    }

    let atomic = batch_is_atomic(producer.storage());
    let results = on_storage(producer.storage(), move |storage| {
        Ok(if atomic {
            submit_atomically(storage, prepared)
        } else {
            submit_one_at_a_time(storage, prepared)
        })
    })
    .await?;

    match results {
        Ok(results) => Ok(Response::new(pb::EnqueueBatchResponse { results })),
        // The whole batch rolled back. The error keeps the reason storage
        // raised; it carries no `index`, because nothing storage returns
        // attributes an all-or-nothing failure to one item, and inventing a
        // position would be worse than admitting there is none.
        Err(error) => Err(error.into()),
    }
}

/// Whether this backend's batch enqueue is one transaction.
///
/// The Diesel backends run dependency validation and every chunked insert
/// inside one `write_transaction`. Redis issues its writes as a pipeline, which
/// travels in one round trip and rolls nothing back — and its unique variant
/// does not even pipeline, it loops the single-job path.
fn batch_is_atomic(storage: &StorageBackend) -> bool {
    match storage {
        StorageBackend::Sqlite(_) => true,
        #[cfg(feature = "postgres")]
        StorageBackend::Postgres(_) => true,
        #[cfg(feature = "redis")]
        StorageBackend::Redis(_) => false,
    }
}

fn submit_atomically(
    storage: &StorageBackend,
    prepared: Vec<Prepared>,
) -> Result<Vec<pb::EnqueueBatchItemResult>, WireError> {
    // One call, so one transaction. Debounce has no batch entry point, so a
    // debounced item is refused in `prepare` rather than quietly costing the
    // rest of the batch its atomicity.
    let jobs = storage
        .enqueue_unique_batch_reporting(prepared.into_iter().map(|item| item.job).collect())
        .map_err(|error| WireError::from_queue_error(&error))?;

    Ok(jobs.into_iter().map(enqueued).collect())
}

fn submit_one_at_a_time(
    storage: &StorageBackend,
    prepared: Vec<Prepared>,
) -> Result<Vec<pb::EnqueueBatchItemResult>, WireError> {
    Ok(prepared
        .into_iter()
        .enumerate()
        .map(|(index, item)| match item.submit(storage) {
            Ok(result) => enqueued(result),
            Err(error) => pb::EnqueueBatchItemResult {
                outcome: Some(pb::enqueue_batch_item_result::Outcome::Error(
                    WireError::from_queue_error(&error).at_index(index).into(),
                )),
            },
        })
        .collect())
}

fn enqueued((job, deduplicated): (Job, bool)) -> pb::EnqueueBatchItemResult {
    pb::EnqueueBatchItemResult {
        outcome: Some(pb::enqueue_batch_item_result::Outcome::Enqueued(
            pb::EnqueueResponse {
                job: Some(convert::job_to_wire(job, Blobs::NONE)),
                deduplicated,
            },
        )),
    }
}

/// A validated request, and which storage entry point it goes to.
///
/// Validation happens before the blocking hop so a malformed request never
/// occupies a pool thread, and so a batch can attribute a bad item to its
/// index before anything is written.
struct Prepared {
    job: NewJob,
    dispatch: Dispatch,
}

enum Dispatch {
    /// No key and no window: a plain insert.
    Plain,
    /// A `unique_key`, so the enqueue may return a job that was already active.
    Unique,
    /// A debounce window to open or slide.
    Debounced(flexiq_core::storage::records::DebounceOptions),
}

impl Prepared {
    fn submit(self, storage: &StorageBackend) -> flexiq_core::Result<(Job, bool)> {
        match self.dispatch {
            // A debounced enqueue can also answer with a job that already
            // existed, but that is a slid window and not a `unique_key` match,
            // and `deduplicated` says only the latter.
            Dispatch::Debounced(options) => storage
                .enqueue_debounced(self.job, options)
                .map(|job| (job, false)),
            Dispatch::Unique => storage.enqueue_unique_reporting(self.job),
            Dispatch::Plain => storage.enqueue(self.job).map(|job| (job, false)),
        }
    }
}

/// Validate one request and decide where it goes.
fn prepare(request: pb::EnqueueRequest, namespace: &str) -> Result<Prepared, WireError> {
    // An absent body is not an empty one: `raw = ""` is a zero-length payload,
    // and no arm at all is a request that forgot to say what to run.
    let payload = match request.body {
        Some(pb::enqueue_request::Body::Raw(bytes)) => bytes,
        None => {
            return Err(WireError::invalid_request(
                "no body arm is set; send raw = \"\" for a job with no payload",
            ))
        }
    };

    let options = request.options.unwrap_or_default();
    let debounce = convert::debounce_options(&options)?;
    let has_unique_key = options.unique_key.is_some();

    let dispatch = match debounce {
        Some(options) => Dispatch::Debounced(options),
        None if has_unique_key => Dispatch::Unique,
        None => Dispatch::Plain,
    };

    let job = convert::new_job(
        request.task_name,
        payload,
        Some(options),
        namespace,
        now_millis(),
    )?;

    Ok(Prepared { job, dispatch })
}
