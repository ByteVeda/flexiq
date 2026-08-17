use diesel::prelude::*;

use super::super::models::DashboardSettingRow;
use super::super::schema::dashboard_settings;
use super::SqliteStorage;
use crate::error::Result;
use crate::job::now_millis;

crate::storage::diesel_common::impl_diesel_setting_ops!(SqliteStorage);
