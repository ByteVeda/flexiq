/// Generates the shared `list_archived` method for Diesel-backed storage backends.
///
/// `archive_old_jobs` differs between SQLite (`INSERT OR IGNORE`) and Postgres
/// (`ON CONFLICT DO NOTHING` + `$1` bind syntax), so it remains backend-specific.
macro_rules! impl_diesel_archival_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// List archived jobs with pagination. `namespace` of `None`
            /// returns every namespace, matching `list_jobs`.
            pub fn list_archived(
                &self,
                limit: i64,
                offset: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                let mut conn = self.conn()?;

                // Narrow projection: archive listings never render the
                // arg/result blobs, so leave them on TOAST/overflow pages.
                let mut query = archived_jobs::table.into_boxed();
                if let Some(ns) = namespace {
                    query = query.filter(archived_jobs::namespace.eq(ns));
                }
                let rows: Vec<NarrowArchivedJobRow> = query
                    .order(archived_jobs::completed_at.desc())
                    .limit(limit)
                    .offset(offset)
                    .select(NarrowArchivedJobRow::as_select())
                    .load(&mut conn)?;

                Ok(rows.into_iter().map(Job::from_narrow_archived).collect())
            }

            /// Keyset-paginated `list_archived`, ordered by
            /// `(completed_at, id)` descending. `completed_at` is nullable in
            /// the schema (always set for a terminal row), so the cursor bound
            /// compares against `Some(..)`.
            pub fn list_archived_after(
                &self,
                limit: i64,
                after: Option<(i64, &str)>,
                namespace: Option<&str>,
            ) -> Result<Vec<Job>> {
                if limit <= 0 {
                    return Ok(Vec::new());
                }
                let mut conn = self.conn()?;

                let mut query = archived_jobs::table
                    .into_boxed()
                    .order((archived_jobs::completed_at.desc(), archived_jobs::id.desc()));

                if let Some(ns) = namespace {
                    query = query.filter(archived_jobs::namespace.eq(ns));
                }

                if let Some((cursor_completed_at, cursor_id)) = after {
                    let cursor_id = cursor_id.to_string();
                    query = query.filter(
                        archived_jobs::completed_at
                            .lt(Some(cursor_completed_at))
                            .or(archived_jobs::completed_at
                                .eq(Some(cursor_completed_at))
                                .and(archived_jobs::id.lt(cursor_id))),
                    );
                }

                let rows: Vec<NarrowArchivedJobRow> = query
                    .limit(limit)
                    .select(NarrowArchivedJobRow::as_select())
                    .load(&mut conn)?;

                Ok(rows.into_iter().map(Job::from_narrow_archived).collect())
            }
        }
    };
}

pub(crate) use impl_diesel_archival_ops;
