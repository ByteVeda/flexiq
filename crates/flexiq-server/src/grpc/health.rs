//! `grpc.health.v1`, answered from the one thing that can make this door
//! useless: whether storage responds.
//!
//! The question is the dashboard's `/readiness` question, and the answer is
//! deliberately the same one. Storage is checked; the worker registry is not.
//! A producer door with no workers behind it still accepts enqueues — the jobs
//! wait, which is what a queue is for — but a door that cannot reach the
//! database can only fail every call it is given, and an orchestrator should
//! take it out of rotation rather than route to it.
//!
//! The first probe runs before the listener binds, so the port never answers
//! `SERVING` on a database nobody has spoken to yet.

use std::time::Duration;

use flexiq_core::{Storage, StorageBackend};
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::server::{health_reporter, HealthReporter};
use tonic_health::ServingStatus;

use crate::runtime::shutdown::Shutdown;

/// The empty service name is overall server health, per the health protocol.
const OVERALL: &str = "";

/// How often storage is re-checked. Slow enough to be free, fast enough that a
/// database that comes back is serving again within one probe interval.
const POLL: Duration = Duration::from_secs(10);

/// Build the health service, having established its first answer.
pub async fn serve(
    storage: StorageBackend,
    namespace: String,
    shutdown: Shutdown,
) -> HealthServer<impl Health> {
    let (reporter, service) = health_reporter();
    let serving = probe(&storage, &namespace).await;
    report(&reporter, serving).await;
    tokio::spawn(watch(reporter, storage, namespace, shutdown, serving));
    service
}

/// Re-probe until shutdown, reporting only the transitions.
async fn watch(
    reporter: HealthReporter,
    storage: StorageBackend,
    namespace: String,
    shutdown: Shutdown,
    initial: bool,
) {
    let mut serving = initial;
    loop {
        tokio::select! {
            _ = shutdown.wait() => break,
            _ = tokio::time::sleep(POLL) => {}
        }

        let latest = probe(&storage, &namespace).await;
        if latest != serving {
            report(&reporter, latest).await;
            serving = latest;
        }
    }

    // Say the door is closing rather than letting the connection drop and
    // leaving a watching client to infer it.
    report(&reporter, false).await;
}

/// One storage round trip, on the blocking pool because every `Storage` call is
/// synchronous and would otherwise park a runtime worker.
async fn probe(storage: &StorageBackend, namespace: &str) -> bool {
    let storage = storage.clone();
    let namespace = namespace.to_string();
    match tokio::task::spawn_blocking(move || storage.stats(Some(&namespace))).await {
        Ok(Ok(_)) => true,
        // The cause is for the operator's log, never the response: a storage
        // error can carry a DSN or a query fragment.
        Ok(Err(error)) => {
            log::warn!("[flexiq] gRPC health: storage did not answer: {error}");
            false
        }
        Err(error) => {
            log::error!("[flexiq] gRPC health: the storage probe task failed: {error}");
            false
        }
    }
}

async fn report(reporter: &HealthReporter, serving: bool) {
    let status = if serving {
        ServingStatus::Serving
    } else {
        ServingStatus::NotServing
    };
    reporter.set_service_status(OVERALL, status).await;
}
