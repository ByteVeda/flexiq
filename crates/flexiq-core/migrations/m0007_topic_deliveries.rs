//! Per-message ack for log topics (`0007_topic_deliveries`).
//!
//! S28 log subscriptions consume with a single high-water-mark cursor, so one
//! poisoned message blocks the whole subscription. This adds an opt-in
//! per-message mode: instead of the cursor read, a consumer *leases* each
//! message with a visibility timeout and acks or nacks it individually. It's a
//! consumption choice (which read methods you call), not a registration flag —
//! delivery state per `(subscription, message)` lives in `topic_deliveries`, and
//! an un-acked lease that expires is redelivered. Idempotent:
//! `.if_not_exists()` on the table and index.

use sea_query::{Alias, ColumnDef, Index, Table};

use crate::storage::migrate::{ddl, Backend, Migration, Stmt};

pub struct M0007TopicDeliveries;

fn col(name: &str) -> ColumnDef {
    ColumnDef::new(Alias::new(name))
}

fn t(name: &str) -> Alias {
    Alias::new(name)
}

impl Migration for M0007TopicDeliveries {
    fn version(&self) -> &'static str {
        "0007_topic_deliveries"
    }

    fn up(&self, b: Backend) -> Vec<Stmt> {
        // Per-(subscription, message) delivery state. `lease_expires_at` bounds
        // an in-flight lease (0 = available for redelivery, e.g. after a nack);
        // `acked` ends the delivery. `attempts` counts (re)deliveries.
        let deliveries = Table::create()
            .table(t("topic_deliveries"))
            .if_not_exists()
            .col(col("topic").text().not_null())
            .col(col("subscription_name").text().not_null())
            .col(col("message_id").text().not_null())
            .col(col("acked").boolean().not_null().default(false))
            .col(col("attempts").integer().not_null().default(0))
            .col(col("lease_expires_at").big_integer().not_null().default(0))
            .col(col("delivered_at").big_integer().not_null().default(0))
            .primary_key(
                Index::create()
                    .col(t("topic"))
                    .col(t("subscription_name"))
                    .col(t("message_id")),
            )
            .to_owned();

        // The lease read's anti-join filters by (topic, subscription, availability).
        let by_sub_lease = Index::create()
            .if_not_exists()
            .name("idx_topic_deliveries_sub_lease")
            .table(t("topic_deliveries"))
            .col(t("topic"))
            .col(t("subscription_name"))
            .col(t("lease_expires_at"))
            .to_owned();

        vec![ddl(b, &deliveries), ddl(b, &by_sub_lease)]
    }
}
