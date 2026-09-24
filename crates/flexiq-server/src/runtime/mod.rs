//! Process orchestration: open storage, start the parts the environment asked
//! for, and stop them in an order that drains work instead of dropping it.

pub mod listener;
pub mod scheduler;
pub mod shutdown;
pub mod upkeep;

use std::sync::Arc;

use anyhow::{Context, Result};
use flexiq_core::{EventHub, RemoteConfig, RemoteDispatcher, StorageSideChannel};
#[cfg(feature = "http-target")]
use flexiq_core::{HttpDispatchTarget, HttpTargetConfig, StorageBackend};

use crate::config::dashboard::{AuthMode, DashboardConfig};
use crate::config::events::EventsSettings;
#[cfg(feature = "http-target")]
use crate::config::push::PushTargetConfig;
use crate::config::{backend, Config};
use crate::dashboard::state::AppState;
use crate::dashboard::static_assets::StaticAssets;
use crate::runtime::scheduler::{DispatchPath, SchedulerSettings, SchedulerSupervisor};
use crate::runtime::shutdown::{wait_for_signal, Shutdown};

/// Create the configured admin, if the deployment asked for one.
fn prepare_auth(storage: &flexiq_core::StorageBackend, config: &DashboardConfig) {
    if config.auth != AuthMode::Session {
        return;
    }
    if let Some((username, password)) = &config.admin_bootstrap {
        crate::dashboard::auth::bootstrap::admin_from_env(storage, username, password);
    }
}

/// Whether this deployment has a door an executor can attach through.
///
/// Either door counts — the gRPC one carries the same executors, and its gate
/// is the token scope rather than a fourth environment variable. A push
/// deployment has neither: it dials out, so there is nothing to attach to, and
/// this is what leaves both the `RemoteDispatcher` and the gRPC executor door
/// unbuilt rather than registered and never fed. `config.grpc` is `Some` only
/// on a build with the feature — the parser refuses the variable outright
/// otherwise, rather than ignoring it.
pub fn executors_can_attach(config: &Config) -> bool {
    config.push.is_none() && (config.attach.is_some() || config.grpc.is_some())
}

/// Build the push target this deployment dispatches through.
///
/// A target that will not construct — an unparseable URL, a host the
/// allowlist refuses, a credential scheme that cannot build its signer — is a
/// **startup** failure. `HttpDispatchTarget::new` does that validation once,
/// here, so an operator sees it at boot instead of in a dead-letter queue an
/// hour later.
#[cfg(feature = "http-target")]
pub fn push_target(
    config: &PushTargetConfig,
    storage: &StorageBackend,
) -> Result<HttpDispatchTarget> {
    let target = HttpTargetConfig {
        request_timeout: config.request_timeout,
        connect_timeout: config.connect_timeout,
        shutdown_drain: config.shutdown_drain,
        max_request_bytes: config.max_request_bytes,
        max_response_bytes: config.max_response_bytes,
        auth: config.auth.clone().into(),
        // This process holds the database connection the target deliberately
        // does not, so it is the one that resolves the target's per-dispatch
        // middleware toggles — the same argument the attach path's side
        // channel makes.
        side_channel: Some(Arc::new(StorageSideChannel::new(storage.clone()))),
        settle_callbacks: config.settle_callbacks,
        // Everything else takes the core default, `allow_loopback` among them:
        // this path never turns it on, and the variable that asks for it is
        // refused outright rather than honoured.
        ..HttpTargetConfig::new(config.url.clone(), config.capacity, config.allow.clone())
    };
    // Refused at boot, not at the first `202`. A deployment that accepts a
    // hand-off it cannot fence would apply an unfenced settle, which is the
    // one failure this whole path exists to prevent — and it would only find
    // out under load, on a job it had already given away.
    if config.settle_callbacks && !flexiq_core::Storage::supports_settle(storage) {
        anyhow::bail!(
            "{} asks for settle callbacks, but this storage backend cannot record the \
             fence they are checked against. Use a backend that implements it, or unset \
             the variable.",
            crate::config::push::SETTLE_VAR
        );
    }

    HttpDispatchTarget::new(target)
        .map_err(anyhow::Error::from)
        .context("the push target named by FLEXIQ_PUSH_TARGET_URL could not be built")
}

/// Start the event hub the environment asked for.
///
/// Every sink is built here, so one that cannot deliver — a kind this binary
/// was built without, a secret variable that is unset — stops the boot with
/// the file named, instead of dropping every event from the first one on.
pub fn start_events(settings: &EventsSettings) -> Result<Arc<EventHub>> {
    let hub = EventHub::start(settings.config.clone()).with_context(|| {
        format!(
            "{}={} could not start its sinks",
            crate::config::events::FILE_VAR,
            settings.file.display()
        )
    })?;
    log::info!(
        "[flexiq] job events go to {} sink(s) from {}",
        hub.stats().len(),
        settings.file.display()
    );
    Ok(Arc::new(hub))
}

/// Wait for every serving role, returning the first failure.
///
/// Triggering shutdown as soon as one role stops is what lets the others wind
/// down through their own graceful path — an in-flight admission review is
/// answered, an in-flight RPC completes — instead of being dropped mid-request
/// the moment a sibling returns.
async fn drain(mut roles: tokio::task::JoinSet<Result<()>>, shutdown: &Shutdown) -> Result<()> {
    let mut outcome = Ok(());
    while let Some(joined) = roles.join_next().await {
        shutdown.trigger();
        let result = match joined {
            Ok(result) => result,
            Err(error) => Err(anyhow::anyhow!("a server task failed: {error}")),
        };
        // Keep the first error: it is the one that explains why the rest are
        // shutting down.
        if let Err(error) = result {
            if outcome.is_ok() {
                outcome = Err(error);
            }
        }
    }
    outcome
}

/// Run until SIGINT/SIGTERM, then drain and exit.
///
/// `events` is the hub [`start_events`] built from `config.events`. It is
/// started by the caller, before storage is opened or any role spawned, so
/// its sinks read their secrets before `main` scrubs them from the
/// environment. `run` hands it to every emitter and shuts it down last.
pub fn run(config: Config, events: Option<Arc<EventHub>>) -> Result<()> {
    // A webhook-only deployment rewrites pod specs and reads no jobs, so it
    // opens no storage. Config validation has already established that every
    // other role came with a DSN.
    let backend = match &config.dsn {
        Some(dsn) => Some(backend::open(
            dsn,
            config.backend.as_deref(),
            config.namespace.clone(),
            config.auto_migrate,
        )?),
        None => None,
    };
    let shutdown = Shutdown::default();

    // The dispatcher exists only when executors can reach us; without a door
    // there is nothing to dispatch to and the scheduler stays off. See
    // `executors_can_attach` for what counts as a door and why a push
    // deployment has none.
    let dispatcher = match (executors_can_attach(&config), &backend) {
        (true, Some(backend)) => Some(RemoteDispatcher::new(RemoteConfig {
            // Only the socket door has a frame credential to check. The gRPC
            // door's transport vouches for its own peer, so this is never
            // asked of it.
            auth_token: config
                .attach
                .as_ref()
                .and_then(|attach| attach.token.clone()),
            // This process holds the connection an executor deliberately does
            // not, so it is the one that applies its progress and task logs and
            // resolves its middleware toggles.
            side_channel: Some(Arc::new(StorageSideChannel::new(backend.storage.clone()))),
            ..RemoteConfig::default()
        })),
        _ => None,
    };

    // Before anything is spawned: a target that will not construct stops the
    // process here rather than dead-lettering every job it is handed.
    #[cfg(feature = "http-target")]
    let push = match (&config.push, &backend) {
        (Some(push), Some(backend)) => {
            let target = push_target(push, &backend.storage)?;
            log::info!("[flexiq] push dispatch target is {}", target.target());
            Some(Arc::new(target))
        }
        _ => None,
    };

    // `Worker` holds exactly one dispatcher, so this is a choice.
    // Kept past the `path` match below: a settle-only executor door needs the
    // same target the scheduler dispatches through, because the waiting
    // attempt that a settle relieves lives inside it.
    #[cfg(feature = "http-target")]
    let settle_target = push
        .as_ref()
        .filter(|target| target.accepts_settle_callbacks())
        .cloned();

    #[cfg(feature = "http-target")]
    let path = match (dispatcher.clone(), push) {
        (Some(dispatcher), None) => Some(DispatchPath::Attach(dispatcher)),
        (None, Some(target)) => Some(DispatchPath::Push(target)),
        (None, None) => None,
        // Unreachable while `executors_can_attach` gates the dispatcher on
        // `config.push.is_none()`, and an error rather than a silent
        // preference precisely so that a refactor dropping that gate — or a
        // regression in `Config::from_map`'s refusal of the pair — fails at
        // boot instead of racing two dispatchers for the same queues.
        (Some(_), Some(_)) => anyhow::bail!(
            "this process built both an attach dispatcher and a push target, but a \
             Worker holds exactly one. Set FLEXIQ_PUSH_TARGET_URL or FLEXIQ_LISTEN, \
             not both."
        ),
    };
    #[cfg(not(feature = "http-target"))]
    let path = dispatcher.clone().map(DispatchPath::Attach);

    // Read before `path` moves into the supervisor.
    let starts_eagerly = path.as_ref().is_some_and(DispatchPath::starts_eagerly);

    let supervisor = match (path, &backend) {
        (Some(path), Some(backend)) => Some(Arc::new(SchedulerSupervisor::new(
            backend.storage.clone(),
            path,
            SchedulerSettings {
                queues: config.queues.clone(),
                namespace: config.namespace.clone(),
                workers: config.workers,
                maintenance: config.maintenance,
                push_dispatch: config.push_dispatch,
                events: events.clone(),
            },
        ))),
        _ => None,
    };

    let attach_listener = match (config.attach.clone(), &dispatcher, &supervisor) {
        (Some(attach), Some(dispatcher), Some(supervisor)) => Some(listener::spawn(
            attach.listen,
            dispatcher.clone(),
            supervisor.clone(),
            shutdown.clone(),
        )?),
        _ => None,
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build the async runtime")?;

    // Expired sessions and abandoned logins are swept on a cadence, not just
    // at boot: a server that never restarts would otherwise accumulate them.
    let upkeep = match (&config.dashboard, &backend) {
        (Some(dashboard), Some(backend)) if dashboard.auth == AuthMode::Session => {
            Some(upkeep::spawn(backend.storage.clone(), shutdown.clone()))
        }
        _ => None,
    };

    let served = runtime.block_on(async {
        let signals = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                wait_for_signal().await;
                log::info!("[flexiq] shutdown signal received, draining");
                shutdown.trigger();
            }
        });

        // Executors are drained *while* the listeners are winding down, not
        // after. An attach stream is an in-flight gRPC request and a graceful
        // listener waits for it to end — but the stream only ends when the
        // dispatcher closes the connection, which is what this does. Waiting
        // for the roles first would be a circle neither side can leave.
        let draining = supervisor.clone().map(|supervisor| {
            let shutdown = shutdown.clone();
            tokio::spawn(async move {
                shutdown.wait().await;
                // Blocking: it joins the scheduler's threads, and one of them
                // is waiting on this very runtime to finish the drain.
                let _ = tokio::task::spawn_blocking(move || supervisor.shutdown()).await;
            })
        });

        let dashboard = match (&config.dashboard, &backend) {
            (Some(dashboard_config), Some(backend)) => {
                prepare_auth(&backend.storage, dashboard_config);
                let state = Arc::new(AppState {
                    storage: backend.storage.clone(),
                    workflows: backend.workflows.clone(),
                    dispatcher: dispatcher.clone(),
                    assets: StaticAssets::new(dashboard_config.assets_dir.clone()),
                    config: dashboard_config.clone(),
                    oauth: dashboard_config.oauth.clone().map(|oauth| {
                        Arc::new(crate::dashboard::auth::oauth::providers::OAuthRuntime::new(
                            oauth,
                        ))
                    }),
                    namespace: config.namespace.clone(),
                    queues: config.queues.clone(),
                    maintenance: config.maintenance,
                    login_throttle: Default::default(),
                    events: events.clone(),
                });
                Some(crate::dashboard::serve(state, shutdown.clone()))
            }
            _ => None,
        };
        let webhook = config
            .webhook
            .clone()
            .map(|webhook| crate::webhook::serve(webhook, shutdown.clone()));

        // Every server runs to shutdown. The first one to stop — cleanly or not
        // — takes the rest with it, because a half-started deployment should
        // exit rather than linger with one role silently missing.
        let mut roles = tokio::task::JoinSet::new();
        if let Some(dashboard) = dashboard {
            roles.spawn(dashboard);
        }
        if let Some(webhook) = webhook {
            roles.spawn(webhook);
        }
        if let (Some(triggers), Some(backend)) = (config.triggers.clone(), &backend) {
            roles.spawn(crate::trigger::serve(
                triggers,
                backend.storage.clone(),
                events.clone(),
                shutdown.clone(),
            ));
        }
        #[cfg(feature = "grpc")]
        if let (Some(grpc), Some(backend)) = (config.grpc.clone(), &backend) {
            // Said out loud, because "the producer door answers and the
            // executor door does not" is otherwise an hour of an operator's
            // debugging. `dispatcher` is `None` under push — see
            // `executors_can_attach` — so the `zip` below yields no door.
            if config.push.is_some() {
                log::info!(
                    "[flexiq] nothing attaches to this process: it dispatches to a push \
                     target. The executor door serves only the reporting RPCs, and the \
                     producer door is unaffected."
                );
            }
            // Present whenever this process has somewhere to put an executor.
            // A deployment that wants none simply mints no `execute`-scoped
            // token, which is the gate the package already has.
            let door =
                dispatcher
                    .clone()
                    .zip(supervisor.clone())
                    .map(|(dispatcher, supervisor)| {
                        crate::grpc::ExecutorDoor::new(
                            dispatcher,
                            supervisor,
                            crate::grpc::executor::Rotation::new(Some(
                                grpc.executor_stream_max_age,
                            )),
                        )
                    });

            // Under push there is no dispatcher, so the door above is `None`.
            // A deployment that accepts `202` still needs one: it is the only
            // inbound surface a target has to report on work that outlived
            // the request it arrived on.
            #[cfg(feature = "http-target")]
            let door = door.or_else(|| {
                settle_target
                    .clone()
                    .zip(supervisor.clone())
                    .map(|(target, supervisor)| {
                        crate::grpc::ExecutorDoor::settle_only(target, supervisor)
                    })
            });
            roles.spawn(crate::grpc::serve(
                grpc,
                backend.storage.clone(),
                backend.workflows.clone(),
                door,
                events.clone(),
                shutdown.clone(),
            ));
        }

        // A push target is there by configuration, so the scheduler has no
        // peer to wait for and no attach that would ever start it: it starts
        // here, once the roles are up. The attach path stays lazy — see
        // `DispatchPath::starts_eagerly` for the contrast.
        let eager_start = match (starts_eagerly, &supervisor) {
            (true, Some(supervisor)) => supervisor.ensure_started(),
            _ => Ok(()),
        };

        let result = match eager_start {
            Err(error) => {
                // A push deployment whose scheduler will not start has nothing
                // to do; wind the roles down rather than leave a producer door
                // accepting enqueues onto a queue nothing drains.
                shutdown.trigger();
                let _ = drain(roles, &shutdown).await;
                Err(error)
            }
            // No serving role at all — an attach listener on its own, or a
            // push target on its own. Nothing to serve here, so just wait to
            // be told to stop.
            Ok(()) if roles.is_empty() => {
                shutdown.wait().await;
                Ok(())
            }
            Ok(()) => drain(roles, &shutdown).await,
        };

        signals.abort();
        if let Some(draining) = draining {
            // Already finished in the ordinary case — the roles cannot stop
            // until it has closed their streams — but a role that failed its
            // bind never waited for anything, and this is where that drain is
            // still owed.
            let _ = draining.await;
        }
        result
    });

    // Whatever ended the loop — signal or a failed bind — everything else has
    // to come down with it.
    shutdown.trigger();
    if let Some(handle) = attach_listener {
        handle.join();
    }
    if let Some(supervisor) = supervisor {
        supervisor.shutdown();
    }
    if let Some(upkeep) = upkeep {
        let _ = upkeep.join();
    }
    // Last: the scheduler's final outcomes and the doors' last writes are
    // already emitted, so the drain covers them. Called directly because the
    // runtime is no longer driving anything this could stall.
    if let (Some(hub), Some(settings)) = (&events, &config.events) {
        hub.shutdown(settings.drain);
    }
    served
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use flexiq_core::EventsConfig;

    use super::*;

    fn settings(document: &str) -> EventsSettings {
        EventsSettings {
            file: PathBuf::from("/etc/flexiq/events.json"),
            config: EventsConfig::parse(document).expect("a valid document"),
            drain: Duration::from_secs(1),
        }
    }

    /// A document that parses but whose sink cannot be built is still a boot
    /// failure, and it names the file. Without `events-http` the kind itself
    /// is refused; with it, the unset secret variable is.
    #[test]
    fn a_sink_that_cannot_start_fails_the_boot_and_names_the_file() {
        let document = r#"{"sinks": [{
            "kind": "http", "name": "warehouse", "url": "https://events.example.com/in",
            "allow": ["events.example.com"],
            "bearer_token_env": "FLEXIQ_TEST_EVENTS_TOKEN_THAT_IS_NEVER_SET"
        }]}"#;
        let error = start_events(&settings(document)).expect_err("must refuse");
        let message = format!("{error:#}");
        assert!(message.contains("/etc/flexiq/events.json"), "{message}");
        assert!(message.contains("warehouse"), "{message}");
    }

    #[cfg(feature = "events-http")]
    #[test]
    fn a_buildable_sink_starts() {
        let document = r#"{"sinks": [{
            "kind": "http", "name": "warehouse", "url": "https://events.example.com/in",
            "allow": ["events.example.com"]
        }]}"#;
        let hub = start_events(&settings(document)).expect("starts");
        assert_eq!(hub.stats().len(), 1);
        hub.shutdown(Duration::from_millis(10));
    }
}
