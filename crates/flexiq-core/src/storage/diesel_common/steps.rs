/// Generates the durable-step operations shared by the Diesel-backed backends.
///
/// SQLite and Postgres differ only in how a write transaction is opened, and
/// `write_transaction` already hides that, so every statement here is identical
/// on both. The two unique indexes from `m0013_job_steps` do the heavy lifting:
/// the checks below produce the good error, and the constraints are what hold
/// when two writers race past them.
macro_rules! impl_diesel_step_ops {
    ($storage_type:ty, $conn_type:ty) => {
        impl $storage_type {
            /// Resolve the `(owner, attempt)` fence, inside the caller's
            /// transaction, and return the job's namespace so a row can
            /// denormalise it.
            ///
            /// The four cases, in the order they are checked:
            ///
            /// | Claim row | Job row | Outcome |
            /// |---|---|---|
            /// | names `owner` | `Running`, `retry_count == attempt` | proceed |
            /// | absent | `Running`, `retry_count == attempt` | proceed, re-asserting the claim if `reassert` |
            /// | names another worker | — | `ClaimLost` |
            /// | names `owner` under another epoch | — | `ClaimLost` |
            /// | absent | gone, not `Running`, or a different attempt | `ClaimLost` |
            ///
            /// The epoch is what separates two claims one owner won at one
            /// attempt, which `requeue_stuck` produces every time an operator
            /// unsticks a job an executor is still running. It is compared only
            /// when both sides have one: a claim row written before the column
            /// existed carries `None`, and so does a caller that was never
            /// handed a lease, and neither is evidence of anything.
            ///
            /// An absent claim is not a lost one. `purge_execution_claims`
            /// sweeps by age, so a job that legitimately runs longer than the
            /// cutoff finds its own claim gone while it is still the only thing
            /// executing; calling that `ClaimLost` would abandon a live attempt
            /// and leave the job `Running` with no owner.
            ///
            /// `reassert` is what separates a write from a check. A step write
            /// puts the claim back, because the writes after it need one to
            /// fence against, and the insert is safe on the `job_id` primary
            /// key — at most one racing writer wins, and the `Running` and
            /// `retry_count` guards keep it from resurrecting a claim on a job
            /// that has moved on. A read-only check must not: it would leave a
            /// claim its caller never asked for, and take a write transaction
            /// on the result path to do it.
            ///
            /// A re-asserting resolution also takes the job row under
            /// `FOR UPDATE` where the backend has it. Without that, a concurrent
            /// `retry` or terminal archive can commit between the read and the
            /// insert, and the step lands in a sequence that has already moved
            /// on — or under a job that no longer exists. SQLite needs no lock:
            /// `write_transaction` is `BEGIN IMMEDIATE`, which serializes
            /// writers outright.
            fn resolve_step_fence(
                conn: &mut $conn_type,
                job_id: &str,
                owner: &str,
                attempt: i32,
                epoch: Option<i64>,
                namespace: Option<&str>,
                reassert: bool,
            ) -> Result<Option<String>> {
                let job = if reassert {
                    Self::lock_job_for_step_fence(conn, job_id)?
                } else {
                    jobs::table
                        .find(job_id)
                        .select((jobs::status, jobs::retry_count, jobs::namespace))
                        .first(conn)
                        .optional()?
                };

                let Some((status, retry_count, job_namespace)) = job else {
                    return Err(QueueError::ClaimLost(job_id.to_string()));
                };
                let visible = namespace.is_none_or(|scope| job_namespace.as_deref() == Some(scope));
                if !visible || status != JobStatus::Running as i32 || retry_count != attempt {
                    return Err(QueueError::ClaimLost(job_id.to_string()));
                }

                let claim: Option<(String, Option<i64>)> = execution_claims::table
                    .find(job_id)
                    .select((execution_claims::worker_id, execution_claims::epoch))
                    .first(conn)
                    .optional()?;

                match claim {
                    Some((holder, held_epoch))
                        if holder == owner && !$crate::lease::epochs_agree(held_epoch, epoch) =>
                    {
                        return Err(QueueError::ClaimLost(job_id.to_string()));
                    }
                    Some((holder, _)) if holder == owner => {}
                    Some(_) => return Err(QueueError::ClaimLost(job_id.to_string())),
                    None if reassert => {
                        // Re-assert under the caller's own epoch, not a fresh
                        // one: this puts back the claim the age sweep removed
                        // from a still-running attempt, so it has to come back
                        // as the same claim the lease already names.
                        diesel::insert_into(execution_claims::table)
                            .values(&NewExecutionClaimRow {
                                job_id,
                                worker_id: owner,
                                claimed_at: now_millis(),
                                epoch,
                            })
                            .execute(conn)?;
                    }
                    None => {}
                }

                Ok(job_namespace)
            }

            /// How many steps a job has committed, and how many encoded bytes
            /// they add up to. Two aggregates over the `job_id` index rather
            /// than a scan of every row: a job may commit a thousand steps, and
            /// loading them all on each commit would make one job quadratic.
            fn step_totals(
                conn: &mut $conn_type,
                job_id: &str,
            ) -> diesel::result::QueryResult<(i64, i64)> {
                let committed: i64 = job_steps::table
                    .filter(job_steps::job_id.eq(job_id))
                    .count()
                    .get_result(conn)?;
                let bytes: Option<i64> = job_steps::table
                    .filter(job_steps::job_id.eq(job_id))
                    .select(diesel::dsl::sum(job_steps::result_len))
                    .first(conn)?;
                Ok((committed, bytes.unwrap_or(0)))
            }

            /// Reject a commit that cannot legally take `seq`: an explicit key
            /// already spent at another position, or a gap in the sequence.
            ///
            /// The key lookup rides `UNIQUE(job_id, step_key)`, and the gap
            /// check leans on the sequence being gapless by construction, so
            /// the count *is* the next free position.
            fn check_step_position(
                conn: &mut $conn_type,
                job_id: &str,
                seq: i32,
                step_key: &str,
                committed: i64,
            ) -> Result<()> {
                let taken_at: Option<i32> = job_steps::table
                    .filter(job_steps::job_id.eq(job_id))
                    .filter(job_steps::step_key.eq(step_key))
                    .select(job_steps::seq)
                    .first(conn)
                    .optional()?;
                if let Some(taken_at) = taken_at {
                    return Err(QueueError::StepDiverged {
                        job_id: job_id.to_string(),
                        seq,
                        expected: "an unused step key".to_string(),
                        found: format!("'{step_key}', already committed at position {taken_at}"),
                    });
                }
                if i64::from(seq) != committed {
                    return Err(QueueError::StepDiverged {
                        job_id: job_id.to_string(),
                        seq,
                        expected: format!("position {committed}"),
                        found: format!("position {seq}"),
                    });
                }
                Ok(())
            }

            /// Refuse a commit that would take the job over `max_steps`.
            fn check_step_count(
                committed: i64,
                step_key: &str,
                limits: &$crate::step::StepLimits,
            ) -> Result<()> {
                if committed + 1 > limits.max_steps as i64 {
                    return Err(QueueError::StepLimitExceeded {
                        step_key: step_key.to_string(),
                        limit: "step count".to_string(),
                        actual: committed as u64 + 1,
                        allowed: limits.max_steps as u64,
                    });
                }
                Ok(())
            }

            /// Insert one step row inside the caller's transaction.
            fn insert_step_row(
                conn: &mut $conn_type,
                step: &NewJobStep<'_>,
                job_namespace: Option<&str>,
                wake_at: Option<i64>,
            ) -> diesel::result::QueryResult<usize> {
                let payload = step.result.unwrap_or(&[]);
                let id = uuid::Uuid::now_v7().to_string();
                diesel::insert_into(job_steps::table)
                    .values(&NewJobStepRow {
                        id: &id,
                        job_id: step.job_id,
                        namespace: job_namespace,
                        step_key: step.step_key,
                        seq: step.seq,
                        kind: step.kind.as_str(),
                        result: step.result,
                        result_len: payload.len() as i32,
                        wake_at,
                        created_at: now_millis(),
                    })
                    .execute(conn)
            }

            /// Whether this backend implements the step store.
            pub fn supports_steps(&self) -> bool {
                true
            }

            /// Every committed step for a job, ordered by `seq`.
            pub fn get_job_steps(
                &self,
                job_id: &str,
                namespace: Option<&str>,
            ) -> Result<Vec<$crate::storage::records::JobStep>> {
                let mut conn = self.conn()?;
                let mut query = job_steps::table
                    .filter(job_steps::job_id.eq(job_id))
                    .into_boxed();
                if let Some(ns) = namespace {
                    query = query.filter(job_steps::namespace.eq(ns));
                }
                let rows: Vec<JobStepRow> = query
                    .order(job_steps::seq.asc())
                    .select(JobStepRow::as_select())
                    .load(&mut conn)?;
                Ok(rows.into_iter().map(Into::into).collect())
            }

            /// Commit one step, fenced on the execution claim.
            pub fn record_step_result(
                &self,
                step: &NewJobStep<'_>,
                owner: &str,
                attempt: i32,
                epoch: Option<i64>,
                limits: &$crate::step::StepLimits,
                namespace: Option<&str>,
            ) -> Result<$crate::storage::records::StepCommit> {
                use $crate::storage::records::StepCommit;

                // Clamped, not trusted: a configurable cap a caller can raise
                // without bound is not a cap.
                let limits = limits.clamped();
                let payload = step.result.unwrap_or(&[]);
                // Measured on the encoded bytes — post serializer, post codec —
                // because that is what is stored. Gzip shrinks them and AES-GCM
                // grows them, so this is the only number worth reporting.
                if payload.len() > limits.max_step_bytes {
                    return Err(QueueError::StepLimitExceeded {
                        step_key: step.step_key.to_string(),
                        limit: "step bytes".to_string(),
                        actual: payload.len() as u64,
                        allowed: limits.max_step_bytes as u64,
                    });
                }

                self.write_transaction(|conn| {
                    let job_namespace = Self::resolve_step_fence(
                        conn,
                        step.job_id,
                        owner,
                        attempt,
                        epoch,
                        namespace,
                        true,
                    )?;

                    // A retransmission of a commit that already landed is a
                    // success, not a conflict — the executor channel can
                    // legitimately deliver the same frame twice.
                    let stored: Option<(String, String, Option<Vec<u8>>)> = job_steps::table
                        .filter(job_steps::job_id.eq(step.job_id))
                        .filter(job_steps::seq.eq(step.seq))
                        .select((job_steps::step_key, job_steps::kind, job_steps::result))
                        .first(conn)
                        .optional()?;
                    if let Some((stored_key, stored_kind, stored_result)) = stored {
                        if stored_key != step.step_key || stored_kind != step.kind.as_str() {
                            return Err(QueueError::StepDiverged {
                                job_id: step.job_id.to_string(),
                                seq: step.seq,
                                expected: format!("a {stored_kind} step '{stored_key}'"),
                                found: format!("a {} step '{}'", step.kind.as_str(), step.step_key),
                            });
                        }
                        if stored_result.as_deref().unwrap_or(&[]) == payload {
                            return Ok(StepCommit::AlreadyCommitted);
                        }
                        return Err(QueueError::StepDiverged {
                            job_id: step.job_id.to_string(),
                            seq: step.seq,
                            expected: format!("the stored result of '{stored_key}'"),
                            found: "a different result for the same step".to_string(),
                        });
                    }

                    let (committed, stored_bytes) = Self::step_totals(conn, step.job_id)?;
                    Self::check_step_position(
                        conn,
                        step.job_id,
                        step.seq,
                        step.step_key,
                        committed,
                    )?;
                    Self::check_step_count(committed, step.step_key, &limits)?;

                    let total = stored_bytes + payload.len() as i64;
                    if total > limits.max_total_bytes as i64 {
                        return Err(QueueError::StepLimitExceeded {
                            step_key: step.step_key.to_string(),
                            limit: "total bytes".to_string(),
                            actual: total as u64,
                            allowed: limits.max_total_bytes as u64,
                        });
                    }

                    Self::insert_step_row(conn, step, job_namespace.as_deref(), None)?;
                    Ok(StepCommit::Committed)
                })
            }

            /// End the attempt in a sleep: commit the row, release the claim,
            /// and reschedule the job — one transaction.
            #[allow(clippy::too_many_arguments)]
            pub fn sleep_job(
                &self,
                step: &NewJobStep<'_>,
                owner: &str,
                attempt: i32,
                epoch: Option<i64>,
                wake_at: i64,
                limits: &$crate::step::StepLimits,
                namespace: Option<&str>,
            ) -> Result<$crate::storage::records::SleepOutcome> {
                use $crate::storage::records::{SleepOutcome, StepKind};

                let limits = limits.clamped();
                self.write_transaction(|conn| {
                    let job_namespace = Self::resolve_step_fence(
                        conn,
                        step.job_id,
                        owner,
                        attempt,
                        epoch,
                        namespace,
                        true,
                    )?;

                    let stored: Option<(String, String, Option<i64>)> = job_steps::table
                        .filter(job_steps::job_id.eq(step.job_id))
                        .filter(job_steps::seq.eq(step.seq))
                        .select((job_steps::step_key, job_steps::kind, job_steps::wake_at))
                        .first(conn)
                        .optional()?;

                    // The first commit fixes the deadline and a replay never
                    // moves it. A binding that recomputed `now + 1h` on every
                    // replay would push the deadline an hour further out each
                    // time the job crashed into it — a sleep that outlives the
                    // job, produced by the recovery path itself.
                    let outcome = match stored {
                        Some((stored_key, stored_kind, stored_wake)) => {
                            // `kind` is part of the match: a `run` row carries
                            // no deadline, so reading one as a sleep would
                            // reschedule the job to nothing.
                            if stored_key != step.step_key
                                || stored_kind != StepKind::Sleep.as_str()
                            {
                                return Err(QueueError::StepDiverged {
                                    job_id: step.job_id.to_string(),
                                    seq: step.seq,
                                    expected: format!("a {stored_kind} step '{stored_key}'"),
                                    found: format!("a sleep step '{}'", step.step_key),
                                });
                            }
                            let deadline = stored_wake.ok_or_else(|| QueueError::StepDiverged {
                                job_id: step.job_id.to_string(),
                                seq: step.seq,
                                expected: "a sleep step with a deadline".to_string(),
                                found: format!("'{stored_key}' with none"),
                            })?;
                            SleepOutcome::AlreadySleeping { wake_at: deadline }
                        }
                        None => {
                            let (committed, _) = Self::step_totals(conn, step.job_id)?;
                            Self::check_step_position(
                                conn,
                                step.job_id,
                                step.seq,
                                step.step_key,
                                committed,
                            )?;
                            Self::check_step_count(committed, step.step_key, &limits)?;
                            Self::insert_step_row(
                                conn,
                                step,
                                job_namespace.as_deref(),
                                Some(wake_at),
                            )?;
                            SleepOutcome::Slept { wake_at }
                        }
                    };

                    // Releasing the claim and rescheduling share the row's
                    // transaction: split apart, a crash in between leaves the
                    // job `Running` with an unreached deadline, and the stale
                    // reaper hands it to another worker while its own timeout
                    // clock is still going.
                    diesel::delete(
                        execution_claims::table.filter(execution_claims::job_id.eq(step.job_id)),
                    )
                    .execute(conn)?;

                    let affected = diesel::update(jobs::table)
                        .filter(jobs::id.eq(step.job_id))
                        .filter(jobs::status.eq(JobStatus::Running as i32))
                        .set((
                            jobs::status.eq(JobStatus::Pending as i32),
                            jobs::scheduled_at.eq(outcome.wake_at()),
                            jobs::started_at.eq(None::<i64>),
                            jobs::completed_at.eq(None::<i64>),
                            jobs::error.eq(None::<String>),
                        ))
                        .execute(conn)?;
                    if affected == 0 {
                        return Err(QueueError::ClaimLost(step.job_id.to_string()));
                    }

                    Ok(outcome)
                })
            }

            /// Whether a result carrying `(owner, attempt, epoch)` still speaks
            /// for this job. The step fence, read rather than written — no
            /// claim is re-asserted and no write transaction is taken, because
            /// this runs once per result on the drain path.
            pub fn authorize_attempt(
                &self,
                job_id: &str,
                owner: &str,
                attempt: i32,
                epoch: Option<i64>,
                namespace: Option<&str>,
            ) -> Result<$crate::storage::records::AttemptFence> {
                use $crate::storage::records::AttemptFence;

                let mut conn = self.conn()?;
                match Self::resolve_step_fence(
                    &mut conn, job_id, owner, attempt, epoch, namespace, false,
                ) {
                    Ok(_) => Ok(AttemptFence::Authorized),
                    // The job moved on, or another worker holds it. Not an
                    // error: the result is dropped, and the attempt that is
                    // actually running finishes the job.
                    Err(QueueError::ClaimLost(_)) => Ok(AttemptFence::Superseded),
                    Err(other) => Err(other),
                }
            }

            /// Drop every step row for a job. The explicit admin entry point —
            /// the terminal paths delete inline instead.
            pub fn delete_job_steps(&self, job_id: &str, namespace: Option<&str>) -> Result<u64> {
                let mut conn = self.conn()?;
                let removed = match namespace {
                    Some(ns) => diesel::delete(
                        job_steps::table
                            .filter(job_steps::job_id.eq(job_id))
                            .filter(job_steps::namespace.eq(ns)),
                    )
                    .execute(&mut conn)?,
                    None => diesel::delete(job_steps::table.filter(job_steps::job_id.eq(job_id)))
                        .execute(&mut conn)?,
                };
                Ok(removed as u64)
            }
        }
    };
}

pub(crate) use impl_diesel_step_ops;
