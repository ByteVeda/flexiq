/// Generates the audit-trail methods (#840) for Diesel-backed storage backends.
macro_rules! impl_diesel_audit_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// Append audit records in one transaction. A duplicate id fails
            /// the primary key, so a record is never silently overwritten.
            pub fn append_audit(
                &self,
                records: &[$crate::storage::records::AuditRecord],
            ) -> Result<()> {
                if records.is_empty() {
                    return Ok(());
                }
                let rows: Vec<AuditRow> = records.iter().map(AuditRow::from).collect();
                self.write_transaction(|conn| {
                    diesel::insert_into(audit_log::table)
                        .values(&rows)
                        .execute(conn)?;
                    Ok(())
                })
            }

            /// One namespace's audit records, `(at_ms, id)` descending with a
            /// `(at_ms, id) < after` bound.
            pub fn list_audit_after(
                &self,
                namespace: &str,
                filter: &$crate::storage::records::AuditFilter,
                limit: i64,
                after: Option<(i64, &str)>,
            ) -> Result<Vec<$crate::storage::records::AuditRecord>> {
                if limit <= 0 {
                    return Ok(Vec::new());
                }
                let mut conn = self.conn()?;

                let mut query = audit_log::table
                    .filter(audit_log::namespace.eq(namespace))
                    .into_boxed()
                    .order((audit_log::at_ms.desc(), audit_log::id.desc()));

                if let Some((cursor_at, cursor_id)) = after {
                    let cursor_id = cursor_id.to_string();
                    query = query.filter(
                        audit_log::at_ms.lt(cursor_at).or(audit_log::at_ms
                            .eq(cursor_at)
                            .and(audit_log::id.lt(cursor_id))),
                    );
                }
                if let Some(token_id) = &filter.token_id {
                    query = query.filter(audit_log::token_id.eq(token_id.clone()));
                }
                if let Some(kind) = &filter.target_kind {
                    query = query.filter(audit_log::target_kind.eq(kind.clone()));
                }
                if let Some(target) = &filter.target {
                    query = query.filter(audit_log::target.eq(target.clone()));
                }
                if let Some(since) = filter.since_ms {
                    query = query.filter(audit_log::at_ms.ge(since));
                }
                if let Some(until) = filter.until_ms {
                    query = query.filter(audit_log::at_ms.lt(until));
                }

                let rows: Vec<AuditRow> = query
                    .limit(limit)
                    .select(AuditRow::as_select())
                    .load(&mut conn)?;
                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Delete one namespace's audit records older than the cutoff, in
            /// bounded batches — see `diesel_common::purge`.
            pub fn purge_audit(&self, namespace: &str, older_than_ms: i64) -> Result<u64> {
                $crate::storage::diesel_common::purge::drain_batches(|| {
                    self.write_transaction(|conn| {
                        let ids: Vec<String> = audit_log::table
                            .filter(audit_log::namespace.eq(namespace))
                            .filter(audit_log::at_ms.lt(older_than_ms))
                            .select(audit_log::id)
                            .limit($crate::storage::diesel_common::purge::PURGE_BATCH)
                            .load(conn)?;
                        let affected =
                            diesel::delete(audit_log::table.filter(audit_log::id.eq_any(&ids)))
                                .execute(conn)?;
                        Ok(affected as u64)
                    })
                })
            }
        }
    };
}

pub(crate) use impl_diesel_audit_ops;
