use diesel::prelude::*;

use super::super::schema::{archived_jobs, dead_letter, job_errors, task_logs, task_metrics};
use super::SqliteStorage;
use crate::error::Result;

crate::storage::diesel_common::impl_diesel_retention_ops!(SqliteStorage);
