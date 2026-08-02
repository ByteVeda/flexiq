use redis::Commands;

use super::{map_err, strip_list_blobs, RedisStorage};
use crate::error::Result;
use crate::job::{Job, JobStatus};

impl RedisStorage {
    /// Move `Complete`/`Dead`/`Cancelled` jobs completed before `cutoff_ms`
    /// (Unix milliseconds) out of every live index and into the archive.
    /// Returns the count archived. `Failed` jobs never appear here — `fail()`
    /// archives them immediately.
    pub fn archive_old_jobs(&self, cutoff_ms: i64) -> Result<u64> {
        let mut conn = self.conn()?;
        let mut count = 0u64;

        // Sourcing statuses from the enum guarantees that any future reorder
        // or insertion in `JobStatus` doesn't silently change which buckets
        // get archived.
        for status in [JobStatus::Complete, JobStatus::Dead, JobStatus::Cancelled] {
            let status_key = self.key(&["jobs", "status", &(status as i32).to_string()]);
            let job_ids: Vec<String> = conn.smembers(&status_key).map_err(map_err)?;

            for id in &job_ids {
                if let Some(job) = self.load_job(&mut conn, id)? {
                    if let Some(completed_at) = job.completed_at {
                        if completed_at < cutoff_ms {
                            // Move the job out of every live index and into the
                            // archive (including the per-status archive set used
                            // by stats and list_jobs).
                            let old_status = job.status;
                            self.archive_job_immediately(&mut conn, &job, old_status)?;
                            count += 1;
                        }
                    }
                }
            }
        }

        Ok(count)
    }

    /// Archived jobs, newest first, paginated. Rows are blob-free.
    /// `namespace` of `None` returns every namespace, matching `list_jobs`.
    pub fn list_archived(
        &self,
        limit: i64,
        offset: i64,
        namespace: Option<&str>,
    ) -> Result<Vec<Job>> {
        // Scoped: namespace is not indexed, so the set is walked newest-first
        // and paginated after the filter, like `list_dead`.
        if let Some(scope) = namespace {
            return self.list_archived_matching(limit, offset, scope);
        }

        let mut conn = self.conn()?;
        let archived_all = self.key(&["archived", "all"]);

        let ids: Vec<String> = conn
            .zrevrangebyscore_limit(
                &archived_all,
                "+inf",
                "-inf",
                offset.max(0) as isize,
                limit.max(0) as isize,
            )
            .map_err(map_err)?;

        self.load_archived_by_ids(&mut conn, &ids)
    }

    /// Walk the archive newest-first keeping one namespace's rows, then page.
    fn list_archived_matching(&self, limit: i64, offset: i64, namespace: &str) -> Result<Vec<Job>> {
        if limit <= 0 {
            return Ok(Vec::new());
        }
        let mut conn = self.conn()?;
        let archived_all = self.key(&["archived", "all"]);
        let offset = offset.max(0) as usize;
        let limit = limit as usize;

        let ids: Vec<String> = conn.zrevrange(&archived_all, 0, -1).map_err(map_err)?;
        let mut kept = Vec::new();
        for id in ids {
            let archived_key = self.key(&["archived", &id]);
            let data: Option<String> = conn.get(&archived_key).map_err(map_err)?;
            let Some(d) = data else { continue };
            let mut job: Job = serde_json::from_str(&d)?;
            if job.namespace.as_deref() != Some(namespace) {
                continue;
            }
            strip_list_blobs(&mut job);
            kept.push(job);
            // Stop once this page can be served. Saturating: both are public
            // i64 inputs.
            if kept.len() >= offset.saturating_add(limit) {
                break;
            }
        }

        Ok(kept.into_iter().skip(offset).take(limit).collect())
    }

    /// Keyset-paginated `list_archived`, ordered by `(completed_at, id)`
    /// descending. `archived:all` is scored by `completed_at`, so the cursor
    /// maps straight onto the ZSET keyset.
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
        let archived_all = self.key(&["archived", "all"]);

        let mut results: Vec<Job> = Vec::with_capacity(limit as usize);
        let mut cursor: Option<(i64, String)> =
            after.map(|(completed_at, id)| (completed_at, id.to_string()));

        loop {
            let borrowed = cursor.as_ref().map(|(at, id)| (*at, id.as_str()));
            let ids = super::zset_keyset_page(&mut conn, &archived_all, borrowed, limit)?;
            if ids.is_empty() {
                break;
            }

            // Advanced from the last row *examined*, not the last one kept: a
            // page that filters out entirely still has to move the cursor, or
            // the walk would re-read it forever.
            let mut examined = None;
            for id in &ids {
                let archived_key = self.key(&["archived", id]);
                let data: Option<String> = conn.get(&archived_key).map_err(map_err)?;
                let Some(d) = data else { continue };
                let mut job: Job = serde_json::from_str(&d)?;
                examined = Some((job.completed_at.unwrap_or(job.created_at), job.id.clone()));
                if namespace.is_some_and(|scope| job.namespace.as_deref() != Some(scope)) {
                    continue;
                }
                strip_list_blobs(&mut job);
                results.push(job);
                if results.len() as i64 == limit {
                    return Ok(results);
                }
            }

            // A whole page can be members whose blob is already gone —
            // `archived:all` still indexes them, so nothing above set
            // `examined`. Advance from the ZSET member itself rather than
            // ending the walk with matching rows still below the page.
            if examined.is_none() {
                if let Some(last) = ids.last() {
                    let score: Option<f64> = conn.zscore(&archived_all, last).map_err(map_err)?;
                    examined = score.map(|completed_at| (completed_at as i64, last.clone()));
                }
            }

            // Unscoped pages are already exactly the answer; only a filtered
            // walk needs to look past this page.
            if namespace.is_none() || (ids.len() as i64) < limit || examined.is_none() {
                break;
            }
            cursor = examined;
        }

        Ok(results)
    }

    /// Load the given archived-job ids into blob-free [`Job`]s, preserving order.
    fn load_archived_by_ids(
        &self,
        conn: &mut redis::Connection,
        ids: &[String],
    ) -> Result<Vec<Job>> {
        let mut jobs = Vec::with_capacity(ids.len());
        for id in ids {
            let archived_key = self.key(&["archived", id]);
            let data: Option<String> = conn.get(&archived_key).map_err(map_err)?;
            if let Some(d) = data {
                let mut job: Job = serde_json::from_str(&d)?;
                strip_list_blobs(&mut job);
                jobs.push(job);
            }
        }
        Ok(jobs)
    }
}
