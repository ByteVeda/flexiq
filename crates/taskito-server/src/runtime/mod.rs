//! Process orchestration: open storage, start the parts the environment asked
//! for, and stop them in an order that drains work instead of dropping it.

pub mod listener;
pub mod scheduler;
pub mod shutdown;
pub mod upkeep;

use std::sync::Arc;

use anyhow::{Context, Result};
use taskito_core::{RemoteConfig, RemoteDispatcher, StorageSideChannel};

use crate::config::dashboard::{AuthMode, DashboardConfig};
use crate::config::{backend, Config};
use crate::dashboard::state::AppState;
use crate::dashboard::static_assets::StaticAssets;
use crate::runtime::scheduler::{SchedulerSettings, SchedulerSupervisor};
use crate::runtime::shutdown::{wait_for_signal, Shutdown};

/// Create the configured admin, if the deployment asked for one.
fn prepare_auth(storage: &taskito_core::StorageBackend, config: &DashboardConfig) {
    if config.auth != AuthMode::Session {
        return;
    }
    if let Some((username, password)) = &config.admin_bootstrap {
        crate::dashboard::auth::bootstrap::admin_from_env(storage, username, password);
    }
}

/// Run until SIGINT/SIGTERM, then drain and exit.
pub fn run(config: Config) -> Result<()> {
    let backend = backend::open(&config.dsn, config.backend.as_deref())?;
    let shutdown = Shutdown::default();

    // The dispatcher exists only when executors can reach us; without a
    // listener there is nothing to dispatch to and the scheduler stays off.
    let dispatcher = config.attach.as_ref().map(|attach| {
        RemoteDispatcher::new(RemoteConfig {
            auth_token: attach.token.clone(),
            // This process holds the connection an executor deliberately does
            // not, so it is the one that applies its progress and task logs and
            // resolves its middleware toggles.
            side_channel: Some(Arc::new(StorageSideChannel::new(backend.storage.clone()))),
            ..RemoteConfig::default()
        })
    });

    let supervisor = dispatcher.as_ref().map(|dispatcher| {
        Arc::new(SchedulerSupervisor::new(
            backend.storage.clone(),
            dispatcher.clone(),
            SchedulerSettings {
                queues: config.queues.clone(),
                namespace: config.namespace.clone(),
                workers: config.workers,
                maintenance: config.maintenance,
            },
        ))
    });

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
    let upkeep = config
        .dashboard
        .as_ref()
        .filter(|dashboard| dashboard.auth == AuthMode::Session)
        .map(|_| upkeep::spawn(backend.storage.clone(), shutdown.clone()));

    let served = runtime.block_on(async {
        let signals = tokio::spawn({
            let shutdown = shutdown.clone();
            async move {
                wait_for_signal().await;
                log::info!("[taskito] shutdown signal received, draining");
                shutdown.trigger();
            }
        });

        let result = match &config.dashboard {
            Some(dashboard_config) => {
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
                crate::dashboard::serve(state, shutdown.clone()).await
            }
            // Listener-only deployment: nothing to serve, just wait to be told
            // to stop.
            None => {
                shutdown.wait().await;
                Ok(())
            }
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
