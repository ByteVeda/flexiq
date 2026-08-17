//! Scaling indexes (`0002_scaling_indexes`).
//!
//! The indexes deferred from the Tier-0 scaling work (formerly "S05"): a
//! `created_at` index on `jobs` for keyset/time-ordered listing, and a
//! `namespace` index on `dead_letter` to match the one already on
//! `archived_jobs`. Clean one-shot DDL — this runs exactly once per database
//! after the recorded baseline.

use sea_query::{Alias, Index};

use crate::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0002ScalingIndexes;

impl Migration for M0002ScalingIndexes {
    fn version(&self) -> &'static str {
        "0002_scaling_indexes"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        vec![
            ddl(
                b,
                &Index::create()
                    .if_not_exists()
                    .name("idx_jobs_created_at")
                    .table(Alias::new("jobs"))
                    .col(Alias::new("created_at"))
                    .to_owned(),
            ),
            ddl(
                b,
                &Index::create()
                    .if_not_exists()
                    .name("idx_dead_letter_namespace")
                    .table(Alias::new("dead_letter"))
                    .col(Alias::new("namespace"))
                    .to_owned(),
            ),
        ]
    }
}
