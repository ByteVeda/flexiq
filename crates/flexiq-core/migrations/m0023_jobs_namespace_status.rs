//! Index live jobs by `(namespace, status)` (`0023_jobs_namespace_status`).
//!
//! A namespace quota (#841) counts one tenant's pending jobs on every admitted
//! enqueue and its running jobs on every dispatch. `idx_jobs_namespace` is
//! partial (`WHERE namespace IS NOT NULL`), so the default namespace — the
//! common case — has no index to count through, and neither index narrows by
//! status. A plain composite covers both: SQLite and Postgres b-trees index
//! NULL, and both plan `namespace IS NULL` through it.
//!
//! Idempotent: `IF NOT EXISTS`.

use sea_query::{Alias, Index};

use crate::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0023JobsNamespaceStatus;

impl Migration for M0023JobsNamespaceStatus {
    fn version(&self) -> &'static str {
        "0023_jobs_namespace_status"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let index = Index::create()
            .if_not_exists()
            .name("idx_jobs_namespace_status")
            .table(Alias::new("jobs"))
            .col(Alias::new("namespace"))
            .col(Alias::new("status"))
            .to_owned();
        vec![ddl(b, &index)]
    }
}
