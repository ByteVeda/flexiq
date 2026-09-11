use diesel::prelude::*;

use super::super::models::*;
use super::super::records::{NewPeriodicTask, PeriodicTask};
use super::super::schema::periodic_tasks;
use super::SqliteStorage;
use crate::error::{QueueError, Result};

crate::storage::diesel_common::impl_diesel_periodic_ops!(SqliteStorage, diesel::sqlite::Sqlite);
