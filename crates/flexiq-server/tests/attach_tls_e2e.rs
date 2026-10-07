//! End-to-end: executors attaching over TLS and mTLS (#838).
//!
//! The executor side is core's own `AttachAddress` dial and `ExecutorClient`,
//! the path every SDK shell takes, so a pass here is a pass for the shells'
//! transport too. What is pinned: a queued job runs over a TLS attach; mTLS
//! refuses an executor without a certificate before its `hello` is read; and
//! the attach token is still demanded of one with a certificate.

mod support;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use flexiq_core::{
    AttachAddress, AttachTls, ExecutorClient, ExecutorConfig, ExecutorError, JobStatus,
    NativeDispatcher, NewJob, RemoteConfig, RemoteDispatcher, Secret, Storage, TaskRegistry,
};
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::runtime::listener;
use flexiq_server::runtime::scheduler::{DispatchPath, SchedulerSettings, SchedulerSupervisor};
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tls::{ServerTls, TlsFiles};

use support::{poll_until, temp_storage};

const ATTACH_TOKEN: &str = "attach-token-0123456789abcdef";

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../flexiq-core/tests/fixtures/tls")
        .join(name)
}

/// A TLS attach listener and the scheduler it starts.
struct Harness {
    supervisor: Arc<SchedulerSupervisor>,
    listener: Option<listener::ListenerHandle>,
    shutdown: Shutdown,
}

impl Harness {
    fn start(storage: &flexiq_core::StorageBackend, mtls: bool) -> Self {
        let dispatcher = RemoteDispatcher::new(RemoteConfig {
            auth_token: Some(Secret::new(ATTACH_TOKEN)),
            placement_timeout: Duration::from_secs(5),
            ..RemoteConfig::default()
        });
        let supervisor = Arc::new(SchedulerSupervisor::new(
            storage.clone(),
            DispatchPath::Attach(dispatcher.clone()),
            SchedulerSettings {
                queues: vec!["default".to_string()],
                namespace: None,
                workers: Some(2),
                maintenance: false,
                push_dispatch: None,
                events: None,
            },
        ));
        let tls = ServerTls::load(
            TlsFiles {
                cert: fixture("server.pem"),
                key: fixture("server-key.pem"),
                client_ca: mtls.then(|| fixture("ca.pem")),
            },
            &[],
        )
        .expect("load the fixture pair");
        let shutdown = Shutdown::default();
        let listener = listener::spawn(
            ListenAddress::Tcp("127.0.0.1:0".parse().expect("address")),
            Some(tls),
            dispatcher,
            supervisor.clone(),
            shutdown.clone(),
        )
        .expect("bind");
        Self {
            supervisor,
            listener: Some(listener),
            shutdown,
        }
    }

    /// `tls://localhost:<port>`, so the name verified is the certificate's.
    fn address(&self) -> AttachAddress {
        let port = self
            .listener
            .as_ref()
            .and_then(listener::ListenerHandle::local_addr)
            .expect("tcp")
            .port();
        AttachAddress::parse(&format!("tls://localhost:{port}")).expect("parse")
    }

    fn stop(mut self) {
        self.shutdown.trigger();
        if let Some(listener) = self.listener.take() {
            listener.join();
        }
        self.supervisor.shutdown();
    }
}

fn attach(
    address: &AttachAddress,
    tls: &AttachTls,
    token: Option<&str>,
) -> Result<ExecutorClient, String> {
    let transport = address
        .connect_with(Duration::from_secs(5), Some(tls))
        .map_err(|error| format!("dial: {error}"))?;
    ExecutorClient::connect(
        transport,
        ExecutorConfig {
            tasks: vec!["greet".to_string()],
            slots: 1,
            token: token.map(Secret::new),
            ..ExecutorConfig::new("test", "0.0.0")
        },
    )
    .map_err(|error| match error {
        ExecutorError::Refused => "refused".to_string(),
        other => other.to_string(),
    })
}

fn trusting_ca() -> AttachTls {
    AttachTls {
        ca: Some(fixture("ca.pem")),
        ..AttachTls::default()
    }
}

fn with_certificate(cert: &str, key: &str) -> AttachTls {
    AttachTls {
        cert: Some(fixture(cert)),
        key: Some(fixture(key)),
        ..trusting_ca()
    }
}

fn new_job() -> NewJob {
    NewJob {
        queue: "default".to_string(),
        task_name: "greet".to_string(),
        payload: b"payload".to_vec(),
        priority: 0,
        scheduled_at: flexiq_core::now_millis(),
        max_retries: 0,
        timeout_ms: 30_000,
        unique_key: None,
        metadata: None,
        notes: None,
        depends_on: vec![],
        expires_at: None,
        result_ttl_ms: None,
        namespace: None,
        debounce_key: None,
        enqueued_by: None,
    }
}

#[test]
fn a_job_runs_on_an_executor_attached_over_tls() {
    let storage = temp_storage("attach-tls-job");
    let job = storage.enqueue(new_job()).expect("enqueue");
    let harness = Harness::start(&storage, false);

    let client =
        attach(&harness.address(), &trusting_ca(), Some(ATTACH_TOKEN)).expect("attach over TLS");
    let mut registry = TaskRegistry::new();
    registry.register("greet", |_| Ok(Some(b"hi".to_vec())));
    let handle = client.spawn(Arc::new(NativeDispatcher::new(registry, 1)));

    poll_until(Duration::from_secs(10), || {
        matches!(
            storage.get_job(&job.id, None).expect("read back"),
            Some(ref current) if current.status == JobStatus::Complete
        )
    })
    .expect("the job must complete over the TLS attach");

    handle.shutdown();
    harness.stop();
}

#[test]
fn tls_does_not_stand_in_for_the_attach_token() {
    let storage = temp_storage("attach-tls-token");
    let harness = Harness::start(&storage, false);
    let outcome = attach(&harness.address(), &trusting_ca(), None);
    assert_eq!(outcome.err().as_deref(), Some("refused"));
    harness.stop();
}

#[test]
fn a_plaintext_executor_is_not_served_by_a_tls_listener() {
    let storage = temp_storage("attach-tls-plaintext");
    let harness = Harness::start(&storage, false);
    let AttachAddress::Tls(target) = harness.address() else {
        unreachable!("the harness dials tls://");
    };
    let plaintext = AttachAddress::Tcp(target);
    let transport = plaintext
        .connect(Duration::from_secs(5))
        .expect("TCP connects");
    let outcome = ExecutorClient::connect(
        transport,
        ExecutorConfig {
            tasks: vec!["greet".to_string()],
            slots: 1,
            token: Some(Secret::new(ATTACH_TOKEN)),
            ..ExecutorConfig::new("test", "0.0.0")
        },
    );
    assert!(outcome.is_err(), "a cleartext hello must not be answered");
    harness.stop();
}

#[test]
fn mtls_attaches_an_executor_with_a_trusted_certificate() {
    let storage = temp_storage("attach-mtls-trusted");
    let harness = Harness::start(&storage, true);
    let client = attach(
        &harness.address(),
        &with_certificate("client.pem", "client-key.pem"),
        Some(ATTACH_TOKEN),
    )
    .expect("attach over mTLS");
    drop(client);
    harness.stop();
}

#[test]
fn mtls_refuses_an_executor_without_a_certificate() {
    let storage = temp_storage("attach-mtls-anonymous");
    let harness = Harness::start(&storage, true);
    let outcome = attach(&harness.address(), &trusting_ca(), Some(ATTACH_TOKEN));
    assert!(outcome.is_err(), "no certificate, no attach");
    harness.stop();
}

#[test]
fn mtls_still_demands_the_token() {
    let storage = temp_storage("attach-mtls-token");
    let harness = Harness::start(&storage, true);
    let outcome = attach(
        &harness.address(),
        &with_certificate("client.pem", "client-key.pem"),
        None,
    );
    assert_eq!(outcome.err().as_deref(), Some("refused"));
    harness.stop();
}
