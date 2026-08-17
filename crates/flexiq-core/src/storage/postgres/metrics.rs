use diesel::prelude::*;

use super::super::models::*;
use super::super::schema::task_metrics;
use super::PostgresStorage;
use crate::error::Result;
use crate::job::now_millis;

crate::storage::diesel_common::impl_diesel_metric_ops!(PostgresStorage);
