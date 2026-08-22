use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

use super::super::models::*;
use super::super::schema::{execution_claims, job_steps, jobs};
use super::SqliteStorage;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, JobStatus};
use crate::storage::records::NewJobStep;

crate::storage::diesel_common::impl_diesel_step_ops!(SqliteStorage, SqliteConnection);
