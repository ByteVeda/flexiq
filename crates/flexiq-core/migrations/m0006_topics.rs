//! First-class topic registry (`0006_topics`).
//!
//! S28 log topics only retain a message when a `mode='log'` subscription
//! already exists at publish time (the late-join boundary). Declaring a topic
//! here lets a log publish be retained even with zero subscribers, and gives it
//! a per-topic `retention_ms` window. Idempotent: `.if_not_exists()` on the
//! table (a name-PK table needs no separate index).

use sea_query::{Alias, ColumnDef, Table};

use crate::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0006Topics;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0006Topics {
    fn version(&self) -> &'static str {
        "0006_topics"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let topics = Table::create()
            .table(t("topics"))
            .if_not_exists()
            .col(col("name").text().not_null().primary_key())
            .col(col("mode").text().not_null().default("log"))
            .col(col("retention_ms").big_integer())
            .col(col("created_at").big_integer().not_null())
            .to_owned();

        vec![ddl(b, &topics)]
    }
}
