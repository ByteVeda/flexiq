use diesel::prelude::*;

use super::super::models::NewQueueStateRow;
use super::super::schema::queue_state;
use super::PostgresStorage;
use crate::error::{QueueError, Result};
use crate::job::now_millis;

crate::storage::diesel_common::impl_diesel_queue_state_ops!(PostgresStorage, diesel::pg::Pg);
