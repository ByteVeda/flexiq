//! Log-topic message store (`0005_topic_messages`).
//!
//! S28 adds an opt-in log+cursor pub/sub mode. A publish to a log topic writes
//! exactly one `topic_messages` row (vs one `jobs` row per subscriber in
//! fan-out mode), and each log subscription tracks a `cursor` over the log. This
//! migration creates the message store and adds `mode`/`cursor` to the existing
//! `topic_subscriptions` registry. Idempotent: `.if_not_exists()` on the table
//! and index, `add_column` (swallowed duplicate) for the two columns.

use sea_query::{Alias, ColumnDef, Index, Table};

use crate::storage::migrate::{add_column, ddl, Backend, Migration, Stmt};

pub struct M0005TopicMessages;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0005TopicMessages {
    fn version(&self) -> &'static str {
        "0005_topic_messages"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        let messages = Table::create()
            .table(t("topic_messages"))
            .if_not_exists()
            .col(col("id").text().not_null().primary_key())
            .col(col("topic").text().not_null())
            .col(col("payload").blob().not_null())
            .col(col("metadata").text())
            .col(col("notes").text())
            .col(col("created_at").big_integer().not_null())
            .col(col("expires_at").big_integer())
            .to_owned();

        // Keyset read per topic: `WHERE topic = ? AND id > ? ORDER BY id`.
        let by_topic_id = Index::create()
            .if_not_exists()
            .name("idx_topic_messages_topic_id")
            .table(t("topic_messages"))
            .col(t("topic"))
            .col(t("id"))
            .to_owned();

        // Bounded retention sweep by expiry.
        let by_expiry = Index::create()
            .if_not_exists()
            .name("idx_topic_messages_expires_at")
            .table(t("topic_messages"))
            .col(t("expires_at"))
            .to_owned();

        vec![
            ddl(b, &messages),
            ddl(b, &by_topic_id),
            ddl(b, &by_expiry),
            add_column(
                b,
                "topic_subscriptions",
                col("mode").text().not_null().default("fanout"),
            ),
            add_column(b, "topic_subscriptions", col("cursor").text()),
        ]
    }
}
