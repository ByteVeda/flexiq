use diesel::prelude::*;

use super::super::models::*;
use super::super::schema::{dead_letter, jobs};
use super::SqliteStorage;
use crate::error::{QueueError, Result};
use crate::job::{now_millis, Job, JobStatus, NewJob};
use crate::storage::DeadJob;

crate::storage::diesel_common::impl_diesel_dead_letter_ops!(SqliteStorage);
