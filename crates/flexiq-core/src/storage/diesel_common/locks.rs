/// Generates shared lock operation methods for Diesel-backed storage backends.
///
/// `acquire_lock` and `claim_execution` differ between SQLite and Postgres
/// (different locking/upsert strategies), so they remain in backend-specific files.
macro_rules! impl_diesel_lock_ops {
    ($storage_type:ty) => {
        impl $storage_type {
            /// Release a lock. Returns true if the lock was held by this owner and released.
            pub fn release_lock(&self, lock_name: &str, owner_id: &str) -> Result<bool> {
                let mut conn = self.conn()?;

                let affected = diesel::delete(
                    distributed_locks::table
                        .filter(distributed_locks::lock_name.eq(lock_name))
                        .filter(distributed_locks::owner_id.eq(owner_id)),
                )
                .execute(&mut conn)?;

                Ok(affected > 0)
            }

            /// Extend a lock's TTL. Returns true if the lock was held by this owner and extended.
            pub fn extend_lock(
                &self,
                lock_name: &str,
                owner_id: &str,
                ttl_ms: i64,
            ) -> Result<bool> {
                let mut conn = self.conn()?;
                let now = now_millis();

                let affected = diesel::update(
                    distributed_locks::table
                        .filter(distributed_locks::lock_name.eq(lock_name))
                        .filter(distributed_locks::owner_id.eq(owner_id)),
                )
                .set(distributed_locks::expires_at.eq(now + ttl_ms))
                .execute(&mut conn)?;

                Ok(affected > 0)
            }

            /// Get info about a lock.
            pub fn get_lock_info(
                &self,
                lock_name: &str,
            ) -> Result<Option<$crate::storage::records::LockInfo>> {
                let mut conn = self.conn()?;

                let row = distributed_locks::table
                    .find(lock_name)
                    .select(LockInfoRow::as_select())
                    .first::<LockInfoRow>(&mut conn)
                    .optional()?;

                Ok(row.map(Into::into))
            }

            /// Remove expired locks. Returns count removed.
            pub fn reap_expired_locks(&self, now: i64) -> Result<u64> {
                let mut conn = self.conn()?;

                let affected = diesel::delete(
                    distributed_locks::table.filter(distributed_locks::expires_at.le(now)),
                )
                .execute(&mut conn)?;

                Ok(affected as u64)
            }

            /// Remove the execution claim for a completed job.
            ///
            /// `execution_claims` has no namespace column, so the scope comes
            /// from the claimed job. A claim on a job in another namespace is
            /// left in place — releasing it would hand that tenant's job back
            /// to this one's poller. Resolved before the connection is taken:
            /// a single-connection pool would deadlock on the second.
            pub fn complete_execution(&self, job_id: &str, namespace: Option<&str>) -> Result<()> {
                if namespace.is_some() && self.get_job(job_id, namespace)?.is_none() {
                    return Ok(());
                }

                let mut conn = self.conn()?;

                diesel::delete(execution_claims::table.filter(execution_claims::job_id.eq(job_id)))
                    .execute(&mut conn)?;

                Ok(())
            }

            /// Atomically transfer an existing claim from `expected_owner` to
            /// `new_owner`. The `job_id` PK plus the `worker_id = expected_owner`
            /// filter serialize concurrent rescuers: the first UPDATE rewrites the
            /// owner, every other rescuer's filter no longer matches → 0 rows.
            /// `claim_execution` is INSERT-only and cannot reclaim, so this is a
            /// distinct primitive.
            ///
            /// The transfer mints a **new epoch**, so the rescued job's next
            /// dispatch is a different claim: the owner alone would leave the
            /// rescuer able to authorize a result the dead owner's executor is
            /// still on its way to sending. Returns it, because the rescuer
            /// records that dispatch and needs the identity it was made under.
            pub fn reclaim_execution(
                &self,
                job_id: &str,
                expected_owner: &str,
                new_owner: &str,
            ) -> Result<Option<i64>> {
                let mut conn = self.conn()?;
                let now = now_millis();
                let epoch = $crate::lease::mint_claim_epoch();

                let affected = diesel::update(
                    execution_claims::table
                        .filter(execution_claims::job_id.eq(job_id))
                        .filter(execution_claims::worker_id.eq(expected_owner)),
                )
                .set((
                    execution_claims::worker_id.eq(new_owner),
                    execution_claims::claimed_at.eq(now),
                    execution_claims::epoch.eq(Some(epoch)),
                ))
                .execute(&mut conn)?;

                Ok((affected > 0).then_some(epoch))
            }

            /// Purge old execution claims. Returns count removed.
            pub fn purge_execution_claims(&self, older_than_ms: i64) -> Result<u64> {
                let mut conn = self.conn()?;

                let affected = diesel::delete(
                    execution_claims::table.filter(execution_claims::claimed_at.lt(older_than_ms)),
                )
                .execute(&mut conn)?;

                Ok(affected as u64)
            }
        }
    };
}

pub(crate) use impl_diesel_lock_ops;
