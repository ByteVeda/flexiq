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
            ///
            /// A claim still awaiting a settle is **kept regardless of age**.
            /// The sweep exists to stop abandoned rows accumulating, and its
            /// cutoff is an hour; a dispatch a push target accepted may
            /// legitimately outlive that, and dropping its row would take the
            /// epoch with it. An absent epoch is not a mismatch
            /// (`epochs_agree`), so the fence would then authorize the one
            /// thing it exists to refuse: a stale answer arriving after the
            /// attempt was retried elsewhere.
            ///
            /// The marker's own deadline bounds the reprieve, so this cannot
            /// leak rows: once it passes, the row is as collectable as any
            /// other.
            pub fn purge_execution_claims(&self, older_than_ms: i64) -> Result<u64> {
                let mut conn = self.conn()?;
                let now = now_millis();

                let affected = diesel::delete(
                    execution_claims::table
                        .filter(execution_claims::claimed_at.lt(older_than_ms))
                        .filter(
                            execution_claims::settle_deadline_ms.is_null().or(
                                execution_claims::settle_deadline_ms
                                    .assume_not_null()
                                    .lt(now),
                            ),
                        ),
                )
                .execute(&mut conn)?;

                Ok(affected as u64)
            }

            /// Record that a dispatch was accepted out of band, and how long
            /// the scheduler will wait for its outcome.
            ///
            /// Fenced on `(owner, attempt, epoch)` in the same transaction as
            /// the write, and **re-asserting**: the claim this marker lives on
            /// may already have been swept by age, and putting it back under
            /// the caller's own epoch is what gives the marker a row and the
            /// later settle something to match against.
            ///
            /// Monotonic. A deadline that only moves forward keeps the `jobs`
            /// predicate `reap_stale_jobs` selects on a correct superset of
            /// this one, and makes a retransmitted accept a no-op rather than
            /// a shortening.
            pub fn await_settle(
                &self,
                job_id: &str,
                owner: &str,
                attempt: i32,
                epoch: Option<i64>,
                deadline_ms: i64,
                namespace: Option<&str>,
            ) -> Result<Option<i64>> {
                self.write_transaction(|conn| {
                    match Self::resolve_attempt_fence(
                        conn, job_id, owner, attempt, epoch, namespace, true,
                    ) {
                        Ok(_) => {}
                        // The attempt was superseded. A superseded attempt must
                        // not be able to buy itself more time.
                        Err($crate::error::QueueError::ClaimLost(_)) => return Ok(None),
                        Err(other) => return Err(other),
                    }

                    let held: Option<Option<i64>> = execution_claims::table
                        .find(job_id)
                        .select(execution_claims::settle_deadline_ms)
                        .first(conn)
                        .optional()?;
                    // `None` means the fence re-asserted nothing to write on,
                    // which cannot happen after a re-asserting resolve — but
                    // reading it as "no deadline stored" is the answer that
                    // stays true if that ever changes.
                    let deadline = held
                        .flatten()
                        .map_or(deadline_ms, |held| held.max(deadline_ms));

                    diesel::update(execution_claims::table.find(job_id))
                        .set(execution_claims::settle_deadline_ms.eq(Some(deadline)))
                        .execute(conn)?;

                    Ok(Some(deadline))
                })
            }

            /// Consume the settle marker, if the claimant is entitled to it.
            ///
            /// The test and the removal are one statement, which is the whole
            /// point: three callers race for this and exactly one may win. A
            /// `WHERE` that matched but a delete that ran separately would let
            /// two of them read `Granted`.
            pub fn claim_settle(
                &self,
                job_id: &str,
                claimant: $crate::storage::records::SettleClaimant,
                namespace: Option<&str>,
            ) -> Result<$crate::storage::records::SettleGrant> {
                use $crate::storage::records::{SettleClaimant, SettleGrant};

                self.write_transaction(|conn| {
                    // The namespace guard is on the job, not the claim: the
                    // claim table carries no namespace, and a scheduler must
                    // never settle another tenant's job under its own.
                    if let Some(scope) = namespace {
                        let job_namespace: Option<Option<String>> = jobs::table
                            .find(job_id)
                            .select(jobs::namespace)
                            .first(conn)
                            .optional()?;
                        if job_namespace.flatten().as_deref() != Some(scope) {
                            return Ok(SettleGrant::Refused);
                        }
                    }

                    // Clearing the marker, **not** deleting the claim. The
                    // claim carries the epoch, and the fence that runs moments
                    // later when the result is applied needs it: a settle that
                    // took the row with it would leave `authorize_attempt`
                    // comparing against an absence, which agrees with
                    // everything. Removing a claim is `complete_execution`'s
                    // job, on the terminal path.
                    let mut consume = diesel::update(
                        execution_claims::table
                            .find(job_id)
                            .filter(execution_claims::settle_deadline_ms.is_not_null()),
                    )
                    .into_boxed();

                    consume = match claimant {
                        // Strict: the epoch must be present and equal. This is
                        // `lease_authorizes`, expressed in SQL — an absent
                        // epoch proves nothing, so it must not match.
                        SettleClaimant::Lease(epoch) => {
                            consume.filter(execution_claims::epoch.eq(Some(epoch)))
                        }
                        // The scheduler may only collect a marker whose
                        // deadline has actually passed, evaluated here rather
                        // than by the caller — an extension that committed a
                        // millisecond ago has to win.
                        SettleClaimant::Expired { now } => consume.filter(
                            execution_claims::settle_deadline_ms
                                .assume_not_null()
                                .le(now),
                        ),
                        // No further filter: giving up is unconditional. The
                        // `settle_deadline_ms IS NOT NULL` guard above is what
                        // still makes it single-use.
                        SettleClaimant::Abandoned => consume,
                    };

                    let affected = consume
                        .set(execution_claims::settle_deadline_ms.eq(None::<i64>))
                        .execute(conn)?;
                    Ok(if affected > 0 {
                        SettleGrant::Granted
                    } else {
                        SettleGrant::Refused
                    })
                })
            }
        }
    };
}

pub(crate) use impl_diesel_lock_ops;
