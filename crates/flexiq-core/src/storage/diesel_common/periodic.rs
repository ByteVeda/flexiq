/// Generates the periodic-task registry for Diesel-backed backends.
///
/// One implementation for both. Before #918 the two differed only in their
/// upsert — SQLite `REPLACE INTO`, Postgres `ON CONFLICT (name)` — and both
/// targeted the `name` primary key that `m0018` removed. Uniqueness now lives
/// in an index over expressions (`(namespace IS NULL), COALESCE(namespace, ''),
/// name`), which Diesel cannot name as a conflict target, so registration is an
/// UPDATE followed by an INSERT when it touched nothing. That also ends
/// `REPLACE INTO`'s habit of wiping `last_run` on every re-registration:
/// `REPLACE` deletes the row before inserting, and the insert has no
/// `last_run` to give.
///
/// There are two writes, sharing that shape. `register_periodic` replaces
/// every column the caller supplies; `declare_periodic` (#919) is the write a
/// code declaration makes at every worker start, and its UPDATE omits
/// `enabled` and applies `next_run` through a `CASE` so a pause and a deadline
/// the caller never saw survive it.
macro_rules! impl_diesel_periodic_ops {
    ($storage_type:ty, $backend:ty) => {
        impl $storage_type {
            /// Restrict a `periodic_tasks` query to one namespace.
            ///
            /// `None` is the default namespace — a namespace of its own, not
            /// "match anything". A periodic task is identified by
            /// `(namespace, name)` with no globally unique id behind it, so
            /// every addressed operation follows this scheme rather than
            /// `job_in_namespace`'s "`None` = unscoped read" one.
            fn in_periodic_namespace(
                namespace: Option<&str>,
            ) -> Box<
                dyn diesel::expression::BoxableExpression<
                    periodic_tasks::table,
                    $backend,
                    // `Nullable<Bool>` because comparing a nullable column is:
                    // `.is_null()` is plain `Bool`, so it is lifted to match.
                    SqlType = diesel::sql_types::Nullable<diesel::sql_types::Bool>,
                >,
            > {
                match namespace {
                    Some(ns) => Box::new(periodic_tasks::namespace.eq(ns.to_owned())),
                    None => Box::new(periodic_tasks::namespace.is_null().nullable()),
                }
            }

            /// Run one UPDATE-else-INSERT attempt, retrying it once on a
            /// unique violation.
            ///
            /// A concurrent registration that lands between the UPDATE that
            /// touched nothing and the INSERT wins the identity index and
            /// fails this one. Retrying turns it into the UPDATE it would have
            /// been; there is no third attempt to make, because the row exists
            /// from the winner's commit onward.
            fn with_registration_retry<F>(attempt: F) -> Result<()>
            where
                F: Fn() -> Result<()>,
            {
                match attempt() {
                    Err(QueueError::Storage(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::UniqueViolation,
                        _,
                    ))) => attempt(),
                    result => result,
                }
            }

            /// Register or update the schedule named by `(namespace, name)`.
            pub fn register_periodic(&self, task: &NewPeriodicTask) -> Result<()> {
                Self::with_registration_retry(|| self.register_periodic_once(task))
            }

            /// One UPDATE-else-INSERT attempt, in a single write transaction.
            fn register_periodic_once(&self, task: &NewPeriodicTask) -> Result<()> {
                let namespace = task.namespace.as_deref();
                let row = NewPeriodicTaskRow {
                    name: &task.name,
                    task_name: &task.task_name,
                    cron_expr: &task.cron_expr,
                    args: task.args.as_deref(),
                    kwargs: task.kwargs.as_deref(),
                    queue: &task.queue,
                    enabled: task.enabled,
                    next_run: task.next_run,
                    timezone: task.timezone.as_deref(),
                    namespace,
                };

                self.write_transaction(|conn| {
                    // `AsChangeset` skips the primary key and carries no
                    // `last_run`, so this rewrites the schedule and leaves the
                    // row's identity and its firing history alone.
                    let updated = diesel::update(
                        periodic_tasks::table
                            .filter(Self::in_periodic_namespace(namespace))
                            .filter(periodic_tasks::name.eq(&task.name)),
                    )
                    .set(&row)
                    .execute(conn)?;

                    if updated == 0 {
                        diesel::insert_into(periodic_tasks::table)
                            .values(&row)
                            .execute(conn)?;
                    }
                    Ok(())
                })
            }

            /// Whether the stored schedule differs from the one `task`
            /// declares — the condition that decides whether the stored
            /// `next_run` still means anything.
            ///
            /// Built in Rust rather than as a symmetric SQL comparison because
            /// `timezone` is nullable and Diesel has no portable
            /// `IS DISTINCT FROM`: `timezone <> 'UTC'` is NULL, not true, on a
            /// row that stores no timezone. With the declared value known
            /// here, each case is an ordinary predicate.
            fn periodic_schedule_changed(
                task: &NewPeriodicTask,
            ) -> Box<
                dyn diesel::expression::BoxableExpression<
                    periodic_tasks::table,
                    $backend,
                    SqlType = diesel::sql_types::Nullable<diesel::sql_types::Bool>,
                >,
            > {
                let cron_changed = periodic_tasks::cron_expr.ne(task.cron_expr.clone());
                match task.timezone.clone() {
                    Some(tz) => Box::new(
                        cron_changed
                            .or(periodic_tasks::timezone.is_null())
                            .or(periodic_tasks::timezone.ne(tz))
                            .nullable(),
                    ),
                    None => Box::new(
                        cron_changed
                            .or(periodic_tasks::timezone.is_not_null())
                            .nullable(),
                    ),
                }
            }

            /// Write a declared schedule, leaving `enabled`, `last_run` and —
            /// unless the schedule itself changed — `next_run` alone.
            pub fn declare_periodic(&self, task: &NewPeriodicTask) -> Result<()> {
                Self::with_registration_retry(|| self.declare_periodic_once(task))
            }

            /// One conditional-UPDATE-else-INSERT attempt.
            ///
            /// The UPDATE is a single statement on purpose. Reading the row
            /// first and deciding in Rust would be a lost update on Postgres,
            /// whose `write_transaction` is a plain READ COMMITTED transaction:
            /// the SELECT takes no row lock, so a concurrent
            /// `update_periodic_schedule` still commits between the two. Here
            /// the row lock the UPDATE takes is the fence, and every SET
            /// expression is evaluated against the pre-update row, so the
            /// `CASE` sees the stored schedule rather than the one it is
            /// writing.
            fn declare_periodic_once(&self, task: &NewPeriodicTask) -> Result<()> {
                let namespace = task.namespace.as_deref();

                self.write_transaction(|conn| {
                    // `enabled` and `last_run` are absent from the SET list:
                    // one is the operator's, the other the scheduler's, and a
                    // declaration owns neither.
                    let updated = diesel::update(
                        periodic_tasks::table
                            .filter(Self::in_periodic_namespace(namespace))
                            .filter(periodic_tasks::name.eq(&task.name)),
                    )
                    .set((
                        periodic_tasks::task_name.eq(&task.task_name),
                        periodic_tasks::cron_expr.eq(&task.cron_expr),
                        periodic_tasks::args.eq(task.args.as_deref()),
                        periodic_tasks::kwargs.eq(task.kwargs.as_deref()),
                        periodic_tasks::queue.eq(&task.queue),
                        periodic_tasks::timezone.eq(task.timezone.as_deref()),
                        // `i64` is `AsExpression` for both `BigInt` and
                        // `Nullable<BigInt>`, so the branch type is named
                        // rather than inferred.
                        periodic_tasks::next_run.eq(diesel::dsl::case_when::<
                            _,
                            _,
                            diesel::sql_types::BigInt,
                        >(
                            Self::periodic_schedule_changed(task),
                            task.next_run,
                        )
                        .otherwise(periodic_tasks::next_run)),
                    ))
                    .execute(conn)?;

                    if updated == 0 {
                        // No row yet, so there is nothing to preserve and the
                        // declaration is written whole.
                        diesel::insert_into(periodic_tasks::table)
                            .values(&NewPeriodicTaskRow {
                                name: &task.name,
                                task_name: &task.task_name,
                                cron_expr: &task.cron_expr,
                                args: task.args.as_deref(),
                                kwargs: task.kwargs.as_deref(),
                                queue: &task.queue,
                                enabled: task.enabled,
                                next_run: task.next_run,
                                timezone: task.timezone.as_deref(),
                                namespace,
                            })
                            .execute(conn)?;
                    }
                    Ok(())
                })
            }

            /// Enabled schedules due at `now`. `namespace` is a filter, not an
            /// address: `None` reads every namespace, because a scheduler
            /// running unscoped fires every tenant's schedules.
            pub fn get_due_periodic(
                &self,
                now: i64,
                namespace: Option<&str>,
            ) -> Result<Vec<PeriodicTask>> {
                let mut conn = self.conn()?;

                let mut query = periodic_tasks::table
                    .filter(periodic_tasks::enabled.eq(true))
                    .filter(periodic_tasks::next_run.le(now))
                    .into_boxed();
                if namespace.is_some() {
                    query = query.filter(Self::in_periodic_namespace(namespace));
                }

                let rows = query
                    .select(PeriodicTaskRow::as_select())
                    .load::<PeriodicTaskRow>(&mut conn)?;

                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Advance a schedule after it fires.
            pub fn update_periodic_schedule(
                &self,
                name: &str,
                last_run: i64,
                next_run: i64,
                namespace: Option<&str>,
            ) -> Result<()> {
                let mut conn = self.conn()?;

                diesel::update(
                    periodic_tasks::table
                        .filter(Self::in_periodic_namespace(namespace))
                        .filter(periodic_tasks::name.eq(name)),
                )
                .set((
                    periodic_tasks::last_run.eq(last_run),
                    periodic_tasks::next_run.eq(next_run),
                ))
                .execute(&mut conn)?;

                Ok(())
            }

            /// Every schedule registered in `namespace`, enabled or paused.
            pub fn list_periodic(&self, namespace: Option<&str>) -> Result<Vec<PeriodicTask>> {
                let mut conn = self.conn()?;

                let rows = periodic_tasks::table
                    .filter(Self::in_periodic_namespace(namespace))
                    .select(PeriodicTaskRow::as_select())
                    .load::<PeriodicTaskRow>(&mut conn)?;

                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Remove a schedule. False when `namespace` had none by that name,
            /// including when another namespace does.
            pub fn delete_periodic(&self, name: &str, namespace: Option<&str>) -> Result<bool> {
                let mut conn = self.conn()?;

                let affected = diesel::delete(
                    periodic_tasks::table
                        .filter(Self::in_periodic_namespace(namespace))
                        .filter(periodic_tasks::name.eq(name)),
                )
                .execute(&mut conn)?;

                Ok(affected > 0)
            }

            /// Pause (false) or resume (true) a schedule. False when
            /// `namespace` had none by that name.
            pub fn set_periodic_enabled(
                &self,
                name: &str,
                enabled: bool,
                namespace: Option<&str>,
            ) -> Result<bool> {
                let mut conn = self.conn()?;

                let affected = diesel::update(
                    periodic_tasks::table
                        .filter(Self::in_periodic_namespace(namespace))
                        .filter(periodic_tasks::name.eq(name)),
                )
                .set(periodic_tasks::enabled.eq(enabled))
                .execute(&mut conn)?;

                Ok(affected > 0)
            }
        }
    };
}

pub(crate) use impl_diesel_periodic_ops;
