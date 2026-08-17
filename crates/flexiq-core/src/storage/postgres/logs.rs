use diesel::prelude::*;

use super::super::models::*;
use super::super::schema::task_logs;
use super::PostgresStorage;
use crate::error::Result;
use crate::job::now_millis;

crate::storage::diesel_common::impl_diesel_log_ops!(PostgresStorage);
