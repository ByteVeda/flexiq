//! Process orchestration: open storage, start the parts the environment asked
//! for, and stop them in an order that drains work instead of dropping it.

pub mod listener;
pub mod scheduler;
pub mod shutdown;
pub mod upkeep;

use std::sync::Arc;

use anyhow::{Context, Result};
use flexiq_core::{RemoteConfig, RemoteDispatcher, StorageSideChannel};

use crate::config::dashboard::{AuthMode, DashboardConfig};
use crate::config::{backend, Config};
use crate::dashboard::state::AppState;
use crate::dashboard::static_assets::StaticAssets;
use crate::runtime::scheduler::{SchedulerSettings, SchedulerSupervisor};
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
pub fn run(config: Config) -> Result<()> {
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

    // The dispatcher exists only when executors can reach us; without a
    // listener there is nothing to dispatch to and the scheduler stays off.
    let dispatcher = match (&config.attach, &backend) {
        (Some(attach), Some(backend)) => Some(RemoteDispatcher::new(RemoteConfig {
            auth_token: attach.token.clone(),
            // This process holds the connection an executor deliberately does
            // not, so it is the one that applies its progress and task logs and
            // resolves its middleware toggles.
            side_channel: Some(Arc::new(StorageSideChannel::new(backend.storage.clone()))),
            ..RemoteConfig::default()
        })),
        _ => None,
    };

    let supervisor = match (&dispatcher, &backend) {
        (Some(dispatcher), Some(backend)) => Some(Arc::new(SchedulerSupervisor::new(
            backend.storage.clone(),
            dispatcher.clone(),
            SchedulerSettings {
                queues: config.queues.clone(),
                namespace: config.namespace.clone(),
                workers: config.workers,
                maintenance: config.maintenance,
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
        let result = if roles.is_empty() {
            // Listener-only deployment: nothing to serve, just wait to be told
            // to stop.
            shutdown.wait().await;
            Ok(())
        } else {
            drain(roles, &shutdown).await
        };

        signals.abort();
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
    served
}
