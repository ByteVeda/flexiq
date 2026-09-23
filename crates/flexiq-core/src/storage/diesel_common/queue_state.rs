/// Generates queue pause state for Diesel-backed backends.
///
/// One implementation for both. The two used to differ only in their upsert —
/// SQLite `REPLACE INTO`, Postgres `ON CONFLICT (queue_name)` — and both
/// targeted the `queue_name` primary key `m0020` removed. Identity is now an
/// index over expressions, which Diesel cannot name as a conflict target, so a
/// pause is an UPDATE followed by an INSERT when it touched nothing.
///
/// `namespace` follows the periodic rule: `None` is the default namespace, one
/// namespace of its own, never "every namespace". A scheduler reads only its
/// own namespace's pauses, which is the rule `dequeue` already follows.
macro_rules! impl_diesel_queue_state_ops {
    ($storage_type:ty, $backend:ty) => {
        impl $storage_type {
            /// Restrict a `queue_state` query to one namespace.
            fn in_queue_state_namespace(
                namespace: Option<&str>,
            ) -> Box<
                dyn diesel::expression::BoxableExpression<
                    queue_state::table,
                    $backend,
                    SqlType = diesel::sql_types::Nullable<diesel::sql_types::Bool>,
                >,
            > {
                match namespace {
                    Some(ns) => Box::new(queue_state::namespace.eq(ns.to_owned())),
                    None => Box::new(queue_state::namespace.is_null().nullable()),
                }
            }

            /// Pause a queue so no new jobs are dispatched from it.
            ///
            /// Retried once on a unique violation: a concurrent pause that
            /// lands between the UPDATE that touched nothing and the INSERT
            /// wins the identity index, and the retry is then the UPDATE.
            pub fn pause_queue(&self, queue_name: &str, namespace: Option<&str>) -> Result<()> {
                match self.pause_queue_once(queue_name, namespace) {
                    Err(QueueError::Storage(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::UniqueViolation,
                        _,
                    ))) => self.pause_queue_once(queue_name, namespace),
                    result => result,
                }
            }

            fn pause_queue_once(&self, queue_name: &str, namespace: Option<&str>) -> Result<()> {
                let now = now_millis();
                self.write_transaction(|conn| {
                    let updated = diesel::update(
                        queue_state::table
                            .filter(Self::in_queue_state_namespace(namespace))
                            .filter(queue_state::queue_name.eq(queue_name)),
                    )
                    .set((queue_state::paused.eq(true), queue_state::paused_at.eq(now)))
                    .execute(conn)?;

                    if updated == 0 {
                        diesel::insert_into(queue_state::table)
                            .values(&NewQueueStateRow {
                                queue_name,
                                paused: true,
                                paused_at: Some(now),
                                namespace,
                            })
                            .execute(conn)?;
                    }
                    Ok(())
                })
            }

            /// Resume a paused queue.
            pub fn resume_queue(&self, queue_name: &str, namespace: Option<&str>) -> Result<()> {
                let mut conn = self.conn()?;
                diesel::delete(
                    queue_state::table
                        .filter(Self::in_queue_state_namespace(namespace))
                        .filter(queue_state::queue_name.eq(queue_name)),
                )
                .execute(&mut conn)?;
                Ok(())
            }

            /// Names of the namespace's paused queues.
            pub fn list_paused_queues(&self, namespace: Option<&str>) -> Result<Vec<String>> {
                let mut conn = self.conn()?;
                let names: Vec<String> = queue_state::table
                    .filter(Self::in_queue_state_namespace(namespace))
                    .filter(queue_state::paused.eq(true))
                    .select(queue_state::queue_name)
                    .load(&mut conn)?;
                Ok(names)
            }
        }
    };
}

pub(crate) use impl_diesel_queue_state_ops;
