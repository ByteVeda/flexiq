//! Index the archive and the DLQ for the per-namespace row trims
//! (`0024_namespace_row_trim`).
//!
//! `max_archived_rows` / `max_dead_rows` (#841) count one namespace's rows and
//! delete its oldest first. Without a composite, both walk the whole table: the
//! archive has no namespace index at all, and `idx_dead_letter_namespace` does
//! not order by `failed_at`. `(namespace, <time>, id)` serves the count, the
//! oldest-first scan and its id tie-break from one index. NULL — the default
//! namespace — is indexed like any other value on both backends.
//!
//! Idempotent: `IF NOT EXISTS`.

use sea_query::{Alias, Index};

use crate::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0024NamespaceRowTrim;

impl Migration for M0024NamespaceRowTrim {
    fn version(&self) -> &'static str {
        "0024_namespace_row_trim"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let archived = Index::create()
            .if_not_exists()
            .name("idx_archived_jobs_ns_completed")
            .table(Alias::new("archived_jobs"))
            .col(Alias::new("namespace"))
            .col(Alias::new("completed_at"))
            .col(Alias::new("id"))
            .to_owned();
        let dead = Index::create()
            .if_not_exists()
            .name("idx_dead_letter_ns_failed")
            .table(Alias::new("dead_letter"))
            .col(Alias::new("namespace"))
            .col(Alias::new("failed_at"))
            .col(Alias::new("id"))
            .to_owned();
        vec![ddl(b, &archived), ddl(b, &dead)]
    }
}
