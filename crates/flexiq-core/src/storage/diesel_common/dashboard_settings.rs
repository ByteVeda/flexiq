/// Generates the dashboard settings key/value store for Diesel-backed backends.
macro_rules! impl_diesel_setting_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// Fetch a single setting value by key, or `None` if unset.
            pub fn get_setting(&self, key: &str) -> Result<Option<String>> {
                let mut conn = self.conn()?;
                let row: Option<DashboardSettingRow> = dashboard_settings::table
                    .filter(dashboard_settings::key.eq(key))
                    .first::<DashboardSettingRow>(&mut conn)
                    .optional()?;
                Ok(row.map(|row| row.value))
            }

            /// Insert or update a setting.
            pub fn set_setting(&self, key: &str, value: &str) -> Result<()> {
                let mut conn = self.conn()?;
                let now = now_millis();
                let row = DashboardSettingRow {
                    key: key.to_string(),
                    value: value.to_string(),
                    updated_at: now,
                };
                diesel::insert_into(dashboard_settings::table)
                    .values(&row)
                    .on_conflict(dashboard_settings::key)
                    .do_update()
                    .set((
                        dashboard_settings::value.eq(value),
                        dashboard_settings::updated_at.eq(now),
                    ))
                    .execute(&mut conn)?;
                Ok(())
            }

            /// Write a setting only if it still holds `expected`. `None` means
            /// the key must be unset.
            ///
            /// Both branches are a single statement, so the check and the write
            /// cannot be split by a concurrent writer.
            pub fn set_setting_if(
                &self,
                key: &str,
                expected: Option<&str>,
                value: &str,
            ) -> Result<bool> {
                let mut conn = self.conn()?;
                let now = now_millis();
                let written = match expected {
                    Some(expected) => diesel::update(
                        dashboard_settings::table
                            .filter(dashboard_settings::key.eq(key))
                            .filter(dashboard_settings::value.eq(expected)),
                    )
                    .set((
                        dashboard_settings::value.eq(value),
                        dashboard_settings::updated_at.eq(now),
                    ))
                    .execute(&mut conn)?,
                    None => diesel::insert_into(dashboard_settings::table)
                        .values(&DashboardSettingRow {
                            key: key.to_string(),
                            value: value.to_string(),
                            updated_at: now,
                        })
                        .on_conflict_do_nothing()
                        .execute(&mut conn)?,
                };
                Ok(written > 0)
            }

            /// Delete a setting. Returns `true` if a row was removed.
            pub fn delete_setting(&self, key: &str) -> Result<bool> {
                let mut conn = self.conn()?;
                let deleted = diesel::delete(
                    dashboard_settings::table.filter(dashboard_settings::key.eq(key)),
                )
                .execute(&mut conn)?;
                Ok(deleted > 0)
            }

            /// All settings as a key-to-value map.
            pub fn list_settings(&self) -> Result<std::collections::HashMap<String, String>> {
                let mut conn = self.conn()?;
                let rows: Vec<DashboardSettingRow> = dashboard_settings::table
                    .select(DashboardSettingRow::as_select())
                    .load(&mut conn)?;
                Ok(rows.into_iter().map(|row| (row.key, row.value)).collect())
            }
        }
    };
}

pub(crate) use impl_diesel_setting_ops;
