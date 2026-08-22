use diesel::pg::PgConnection;
use diesel::prelude::*;

use super::super::models::*;
use super::super::schema::{execution_claims, job_steps, jobs};
use super::PostgresStorage;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, JobStatus};
use crate::storage::records::NewJobStep;

crate::storage::diesel_common::impl_diesel_step_ops!(PostgresStorage, PgConnection);
