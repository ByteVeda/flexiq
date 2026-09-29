//! Audit trail (#840) on Redis.
//!
//! Each record is a JSON string at `audit:rec:<id>`, indexed by three ZSETs
//! scored by `at_ms` — the whole namespace, per token, per target — the same
//! three questions the SQL table's indexes answer. A listing walks the most
//! selective index and re-checks every record against the namespace and the
//! full filter, so an index key that two odd names happen to share can cost a
//! longer walk but never a wrong row.

use redis::Commands;

use super::{map_err, zset_keyset_page, RedisStorage, SCAN_BATCH};
use crate::error::Result;
use crate::storage::records::{AuditFilter, AuditRecord};

impl RedisStorage {
    fn audit_record_key(&self, id: &str) -> String {
        self.key(&["audit", "rec", id])
    }

    fn audit_all_key(&self, namespace: &str) -> String {
        self.key(&["audit", "all", namespace])
    }

    fn audit_token_key(&self, namespace: &str, token_id: &str) -> String {
        self.key(&["audit", "token", namespace, token_id])
    }

    fn audit_target_key(&self, namespace: &str, kind: &str, target: &str) -> String {
        self.key(&["audit", "target", namespace, kind, target])
    }

    /// The narrowest index that still contains every record `filter` can match.
    fn audit_index_for(&self, namespace: &str, filter: &AuditFilter) -> String {
        match (&filter.target_kind, &filter.target, &filter.token_id) {
            (Some(kind), Some(target), _) => self.audit_target_key(namespace, kind, target),
            (_, _, Some(token_id)) => self.audit_token_key(namespace, token_id),
            _ => self.audit_all_key(namespace),
        }
    }

    /// Append audit records. `SET NX` keeps an existing id's record untouched.
    pub fn append_audit(&self, records: &[AuditRecord]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn()?;
        let pipe = &mut redis::pipe();
        pipe.atomic();
        for record in records {
            let json = serde_json::to_string(record)?;
            let score = record.at_ms as f64;
            pipe.set_nx(self.audit_record_key(&record.id), json)
                .ignore();
            pipe.zadd(self.audit_all_key(&record.namespace), &record.id, score)
                .ignore();
            pipe.zadd(
                self.audit_token_key(&record.namespace, &record.token_id),
                &record.id,
                score,
            )
            .ignore();
            if let (Some(kind), Some(target)) = (&record.target_kind, &record.target) {
                pipe.zadd(
                    self.audit_target_key(&record.namespace, kind, target),
                    &record.id,
                    score,
                )
                .ignore();
            }
        }
        pipe.query::<()>(&mut conn).map_err(map_err)?;
        Ok(())
    }

    /// One namespace's audit records, `(at_ms, id)` descending with a
    /// `(at_ms, id) < after` bound.
    pub fn list_audit_after(
        &self,
        namespace: &str,
        filter: &AuditFilter,
        limit: i64,
        after: Option<(i64, &str)>,
    ) -> Result<Vec<AuditRecord>> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let index = self.audit_index_for(namespace, filter);

        // `until` is an upper bound the walk can start from: `(until, "")`
        // admits exactly `at_ms < until`, since no id sorts below "".
        let mut cursor: Option<(i64, String)> = match (after, filter.until_ms) {
            (Some((at, _)), Some(until)) if until <= at => Some((until, String::new())),
            (Some((at, id)), _) => Some((at, id.to_string())),
            (None, Some(until)) => Some((until, String::new())),
            (None, None) => None,
        };

        let mut results = Vec::with_capacity(limit as usize);
        loop {
            let borrowed = cursor.as_ref().map(|(at, id)| (*at, id.as_str()));
            let ids = zset_keyset_page(&mut conn, &index, borrowed, SCAN_BATCH as i64)?;
            if ids.is_empty() {
                return Ok(results);
            }

            for id in &ids {
                let data: Option<String> = conn.get(self.audit_record_key(id)).map_err(map_err)?;
                let record = data
                    .as_deref()
                    .map(serde_json::from_str::<AuditRecord>)
                    .transpose()?;
                // The record's time is its score; only an index member whose
                // record is gone needs the ZSET asked.
                let at = match &record {
                    Some(r) => Some(r.at_ms),
                    None => conn
                        .zscore::<_, _, Option<f64>>(&index, id)
                        .map_err(map_err)?
                        .map(|s| s as i64),
                };
                // Advance from every member examined, kept or not, so a page
                // that filters out entirely still moves the walk.
                if let Some(at) = at {
                    if filter.since_ms.is_some_and(|since| at < since) {
                        return Ok(results);
                    }
                    cursor = Some((at, id.clone()));
                }

                let Some(record) = record else { continue };
                if record.namespace != namespace || !filter.matches(&record) {
                    continue;
                }
                results.push(record);
                if results.len() as i64 == limit {
                    return Ok(results);
                }
            }

            if (ids.len() as isize) < SCAN_BATCH {
                return Ok(results);
            }
        }
    }

    /// Delete one namespace's audit records older than the cutoff, draining
    /// the `-inf..cutoff` window in bounded batches.
    pub fn purge_audit(&self, namespace: &str, older_than_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let all_key = self.audit_all_key(namespace);
        let mut total = 0u64;

        loop {
            let ids: Vec<String> = conn
                .zrangebyscore_limit(&all_key, "-inf", format!("({older_than_ms}"), 0, SCAN_BATCH)
                .map_err(map_err)?;
            if ids.is_empty() {
                break;
            }

            let pipe = &mut redis::pipe();
            for id in &ids {
                let record_key = self.audit_record_key(id);
                // Read first: the secondary indexes are named by the record.
                let data: Option<String> = conn.get(&record_key).map_err(map_err)?;
                let record = match data.as_deref().map(serde_json::from_str::<AuditRecord>) {
                    Some(Ok(record)) => Some(record),
                    // Still deleted: one unreadable record must not pin the
                    // window. Its index entries are orphaned, which a listing
                    // already tolerates.
                    Some(Err(e)) => {
                        log::warn!("audit purge: unreadable record {id}: {e}");
                        None
                    }
                    None => None,
                };
                if let Some(record) = record {
                    pipe.zrem(self.audit_token_key(namespace, &record.token_id), id)
                        .ignore();
                    if let (Some(kind), Some(target)) = (&record.target_kind, &record.target) {
                        pipe.zrem(self.audit_target_key(namespace, kind, target), id)
                            .ignore();
                    }
                }
                pipe.del(&record_key).ignore();
                pipe.zrem(&all_key, id).ignore();
            }
            pipe.query::<()>(&mut conn).map_err(map_err)?;

            total += ids.len() as u64;
            if (ids.len() as isize) < SCAN_BATCH {
                break;
            }
        }

        Ok(total)
    }
}
