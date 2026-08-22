//! Shared Diesel-based implementations for SQLite and PostgreSQL backends.
//!
//! These macros generate the identical method implementations that both
//! Diesel-backed storage backends share, avoiding code duplication.

mod archival;
mod dashboard_settings;
mod dead_letter;
mod jobs;
mod locks;
mod logs;
mod metrics;
mod pubsub;
pub(crate) mod purge;
mod retention;
mod steps;
mod workers;

pub(crate) use archival::impl_diesel_archival_ops;
pub(crate) use dashboard_settings::impl_diesel_setting_ops;
pub(crate) use dead_letter::impl_diesel_dead_letter_ops;
pub(crate) use jobs::impl_diesel_job_ops;
pub(crate) use locks::impl_diesel_lock_ops;
pub(crate) use logs::impl_diesel_log_ops;
pub(crate) use metrics::impl_diesel_metric_ops;
pub(crate) use pubsub::impl_diesel_pubsub_ops;
pub(crate) use retention::impl_diesel_retention_ops;
pub(crate) use steps::impl_diesel_step_ops;
pub(crate) use workers::impl_diesel_worker_ops;
