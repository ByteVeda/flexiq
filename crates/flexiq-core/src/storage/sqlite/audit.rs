use diesel::prelude::*;

use super::super::models::AuditRow;
use super::super::schema::audit_log;
use super::SqliteStorage;
use crate::error::Result;

crate::storage::diesel_common::impl_diesel_audit_ops!(SqliteStorage);
