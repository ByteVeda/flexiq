//! Shared Diesel-based implementation for SQLite + PostgreSQL workflow stores.
//!
//! `impl_workflow_diesel_ops!($storage_type, $conn_type, $prep_sql)` generates
//! a complete `impl WorkflowStorage for $storage_type` block. The macro
//! contract: the storage type must expose an `inner: T` field where `T` has a
//! `conn() -> Result<PooledConnection<...>>` method that derefs to a mutable
//! `$conn_type`. SQLite and Postgres both satisfy this via their core storage
//! handles.
//!
//! Diesel's `sql_query` does **not** rewrite `?` placeholders per backend — it
//! requires `?` for SQLite and `$N` for Postgres. The `$prep_sql` macro
//! argument names a function that converts the canonical (`?`-based) SQL into
//! the dialect the connection expects. `sql_as_is` is a no-op for SQLite;
//! `pg_rewrite` substitutes `?` → `$1`, `$2`, … for Postgres.
//!
//! Schema differences (`BLOB` vs `BYTEA`, `INTEGER` vs `BIGINT` for
//! timestamps) are handled by the bound `diesel::sql_types::*` types and the
//! per-backend migration files — the query bodies stay unified.

use diesel::prelude::*;
use diesel::sql_types::Text;

use taskito_core::error::{QueueError, Result};

use crate::{
    StepMetadata, WorkflowDefinition, WorkflowNode, WorkflowNodeStatus, WorkflowRun, WorkflowState,
};

/// Pass-through SQL prep for SQLite — `?` placeholders are native.
#[inline]
pub(crate) fn sql_as_is(sql: &str) -> String {
    sql.to_string()
}

/// Rewrite `?` placeholders to `$N` for Postgres in the order they appear.
///
/// Diesel's `sql_query` API requires dialect-correct placeholders. Postgres
/// uses `$1`, `$2`, …; SQLite uses `?`. Doing the rewrite once at query time
/// (a handful of µs for the short SQL bodies used here) lets the workflow
/// crate keep a single canonical SQL string per query.
#[cfg(feature = "postgres")]
pub(crate) fn pg_rewrite(sql: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(sql.len() + 8);
    let mut n: usize = 1;
    for c in sql.chars() {
        if c == '?' {
            // `write!` to String is infallible — `.unwrap()` is safe.
            write!(out, "${n}").unwrap();
            n += 1;
        } else {
            out.push(c);
        }
    }
    out
}

#[derive(QueryableByName)]
pub(crate) struct DefinitionRow {
    #[diesel(sql_type = Text)]
    pub id: String,
    #[diesel(sql_type = Text)]
    pub name: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    pub version: i32,
    #[diesel(sql_type = diesel::sql_types::Binary)]
    pub dag_data: Vec<u8>,
    #[diesel(sql_type = Text)]
    pub step_metadata: String,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub created_at: i64,
}

/// SQL predicate confining a `workflow_runs` row to the store's namespace.
///
/// Both placeholders bind the same `Option<&str>`. Binding it twice rather
/// than switching the SQL on it keeps one canonical query per method — the
/// bind list is positional, so a conditional clause would fork every listing
/// in the macro below. An unscoped store binds `NULL`, the first half of the
/// `OR` holds, and the predicate degenerates to `TRUE`.
pub(crate) const RUN_IN_NAMESPACE: &str = "(CAST(? AS TEXT) IS NULL OR namespace = ?)";

/// [`RUN_IN_NAMESPACE`] for the queries that alias `workflow_runs` as `r`.
pub(crate) const R_IN_NAMESPACE: &str = "(CAST(? AS TEXT) IS NULL OR r.namespace = ?)";

/// Turn a node setter's affected-row count into a result.
///
/// A setter that touches no row has nothing to set: the node was never created,
/// or the run is outside this store's namespace. Both answer `NodeNotFound`, so
/// a scoped caller cannot tell them apart — and the shells' bind-then-cancel
/// guards, which only fire on `Err`, stop leaving jobs running untracked.
pub(crate) fn require_touched(
    affected: usize,
    run_id: &str,
    node_name: &str,
) -> taskito_core::error::Result<()> {
    if affected == 0 {
        return Err(crate::WorkflowError::NodeNotFound {
            run_id: run_id.to_string(),
            node_name: node_name.to_string(),
        }
        .into());
    }
    Ok(())
}

/// Single-column `COUNT(*)` result, used by the run-visibility guard.
#[derive(QueryableByName)]
pub(crate) struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub n: i64,
}

#[derive(QueryableByName)]
pub(crate) struct RunRow {
    #[diesel(sql_type = Text)]
    pub id: String,
    #[diesel(sql_type = Text)]
    pub definition_id: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub params: Option<String>,
    #[diesel(sql_type = Text)]
    pub state: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    pub started_at: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    pub completed_at: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub error: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub parent_run_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub parent_node_name: Option<String>,
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pub created_at: i64,
}

#[derive(QueryableByName)]
pub(crate) struct NodeRow {
    #[diesel(sql_type = Text)]
    pub id: String,
    #[diesel(sql_type = Text)]
    pub run_id: String,
    #[diesel(sql_type = Text)]
    pub node_name: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub job_id: Option<String>,
    #[diesel(sql_type = Text)]
    pub status: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub result_hash: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Integer>)]
    pub fan_out_count: Option<i32>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub fan_in_data: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    pub started_at: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    pub completed_at: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub error: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub compensation_job_id: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    pub compensation_started_at: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::BigInt>)]
    pub compensation_completed_at: Option<i64>,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    pub compensation_error: Option<String>,
}

pub(crate) fn definition_from_row(row: DefinitionRow) -> Result<WorkflowDefinition> {
    let step_metadata: std::collections::HashMap<String, StepMetadata> =
        serde_json::from_str(&row.step_metadata).map_err(|e| {
            QueueError::Serialization(format!("failed to deserialize step_metadata: {e}"))
        })?;
    Ok(WorkflowDefinition {
        id: row.id,
        name: row.name,
        version: row.version,
        dag_data: row.dag_data,
        step_metadata,
        created_at: row.created_at,
    })
}

pub(crate) fn run_from_row(row: RunRow) -> WorkflowRun {
    WorkflowRun {
        id: row.id,
        definition_id: row.definition_id,
        params: row.params,
        state: WorkflowState::from_str_val(&row.state).unwrap_or(WorkflowState::Pending),
        started_at: row.started_at,
        completed_at: row.completed_at,
        error: row.error,
        parent_run_id: row.parent_run_id,
        parent_node_name: row.parent_node_name,
        created_at: row.created_at,
    }
}

pub(crate) fn node_from_row(row: NodeRow) -> WorkflowNode {
    WorkflowNode {
        id: row.id,
        run_id: row.run_id,
        node_name: row.node_name,
        job_id: row.job_id,
        status: WorkflowNodeStatus::from_str_val(&row.status)
            .unwrap_or(WorkflowNodeStatus::Pending),
        result_hash: row.result_hash,
        fan_out_count: row.fan_out_count,
        fan_in_data: row.fan_in_data,
        started_at: row.started_at,
        completed_at: row.completed_at,
        error: row.error,
        compensation_job_id: row.compensation_job_id,
        compensation_started_at: row.compensation_started_at,
        compensation_completed_at: row.compensation_completed_at,
        compensation_error: row.compensation_error,
    }
}

/// Generate the full `impl WorkflowStorage for $storage_type` block.
///
/// `$prep_sql` names a function that converts canonical (`?`-based) SQL into
/// the dialect the connection expects. `$crate::diesel_common::sql_as_is`
/// works for SQLite; `$crate::diesel_common::pg_rewrite` swaps `?` → `$N` for
/// Postgres.
macro_rules! impl_workflow_diesel_ops {
    ($storage_type:ty, $conn_type:ty, $prep_sql:path) => {
        impl $storage_type {
            /// Whether `run_id` is outside this store's namespace, in which
            /// case the caller must be answered as if the run did not exist.
            ///
            /// `workflow_nodes` carries no namespace of its own — a node is
            /// reachable only through its run — so every node operation gates
            /// on this rather than filtering itself. An unscoped store
            /// addresses every namespace and skips the query entirely.
            fn run_out_of_scope(&self, run_id: &str) -> ::taskito_core::error::Result<bool> {
                let Some(ns) = self.namespace.as_deref() else {
                    return Ok(false);
                };
                let mut conn = self.inner.conn()?;
                let rows: Vec<$crate::diesel_common::CountRow> = ::diesel::sql_query(
                    &$prep_sql(
                        "SELECT COUNT(*) AS n FROM workflow_runs WHERE id = ? AND namespace = ?",
                    ),
                )
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(ns)
                .load(&mut *conn)?;

                Ok(rows.first().map(|row| row.n).unwrap_or(0) == 0)
            }
        }

        impl $crate::storage::WorkflowStorage for $storage_type {
            fn create_workflow_definition(
                &self,
                def: &$crate::WorkflowDefinition,
            ) -> ::taskito_core::error::Result<()> {
                let mut conn = self.inner.conn()?;
                let meta_json = ::serde_json::to_string(&def.step_metadata).map_err(|e| {
                    ::taskito_core::error::QueueError::Serialization(format!(
                        "failed to serialize step_metadata: {e}"
                    ))
                })?;

                ::diesel::sql_query(
                    &$prep_sql("INSERT INTO workflow_definitions (id, name, version, dag_data, step_metadata, created_at)
                     VALUES (?, ?, ?, ?, ?, ?)"),
                )
                .bind::<::diesel::sql_types::Text, _>(&def.id)
                .bind::<::diesel::sql_types::Text, _>(&def.name)
                .bind::<::diesel::sql_types::Integer, _>(def.version)
                .bind::<::diesel::sql_types::Binary, _>(&def.dag_data)
                .bind::<::diesel::sql_types::Text, _>(&meta_json)
                .bind::<::diesel::sql_types::BigInt, _>(def.created_at)
                .execute(&mut *conn)?;

                Ok(())
            }

            fn get_workflow_definition(
                &self,
                name: &str,
                version: Option<i32>,
            ) -> ::taskito_core::error::Result<Option<$crate::WorkflowDefinition>> {
                let mut conn = self.inner.conn()?;

                let rows: Vec<$crate::diesel_common::DefinitionRow> = if let Some(v) = version {
                    ::diesel::sql_query(
                        &$prep_sql("SELECT id, name, version, dag_data, step_metadata, created_at
                         FROM workflow_definitions WHERE name = ? AND version = ?"),
                    )
                    .bind::<::diesel::sql_types::Text, _>(name)
                    .bind::<::diesel::sql_types::Integer, _>(v)
                    .load(&mut *conn)?
                } else {
                    ::diesel::sql_query(
                        &$prep_sql("SELECT id, name, version, dag_data, step_metadata, created_at
                         FROM workflow_definitions WHERE name = ? ORDER BY version DESC LIMIT 1"),
                    )
                    .bind::<::diesel::sql_types::Text, _>(name)
                    .load(&mut *conn)?
                };

                rows.into_iter()
                    .next()
                    .map($crate::diesel_common::definition_from_row)
                    .transpose()
            }

            fn get_workflow_definition_by_id(
                &self,
                id: &str,
            ) -> ::taskito_core::error::Result<Option<$crate::WorkflowDefinition>> {
                let mut conn = self.inner.conn()?;
                let rows: Vec<$crate::diesel_common::DefinitionRow> = ::diesel::sql_query(
                    &$prep_sql("SELECT id, name, version, dag_data, step_metadata, created_at
                     FROM workflow_definitions WHERE id = ?"),
                )
                .bind::<::diesel::sql_types::Text, _>(id)
                .load(&mut *conn)?;

                rows.into_iter()
                    .next()
                    .map($crate::diesel_common::definition_from_row)
                    .transpose()
            }

            fn create_workflow_run(
                &self,
                run: &$crate::WorkflowRun,
            ) -> ::taskito_core::error::Result<()> {
                // A sub-workflow may only hang off a parent this store can
                // see. Without the check a scoped caller indexes a child under
                // another tenant's run.
                if let Some(parent) = run.parent_run_id.as_deref() {
                    if self.run_out_of_scope(parent)? {
                        return Err(::taskito_core::error::QueueError::Other(format!(
                            "parent workflow run not found: {parent}"
                        )));
                    }
                }
                let mut conn = self.inner.conn()?;
                // The namespace is the store's, not the caller's: a run must
                // land in the tenant whose queue created it, and nothing
                // outside this crate should be able to choose otherwise.
                ::diesel::sql_query(
                    &$prep_sql("INSERT INTO workflow_runs
                        (id, definition_id, params, state, started_at, completed_at, error,
                         parent_run_id, parent_node_name, created_at, namespace)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
                )
                .bind::<::diesel::sql_types::Text, _>(&run.id)
                .bind::<::diesel::sql_types::Text, _>(&run.definition_id)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&run.params)
                .bind::<::diesel::sql_types::Text, _>(run.state.as_str())
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(run.started_at)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(run.completed_at)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&run.error)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&run.parent_run_id)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&run.parent_node_name)
                .bind::<::diesel::sql_types::BigInt, _>(run.created_at)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .execute(&mut *conn)?;

                Ok(())
            }

            fn get_workflow_run(
                &self,
                run_id: &str,
            ) -> ::taskito_core::error::Result<Option<$crate::WorkflowRun>> {
                let mut conn = self.inner.conn()?;
                let rows: Vec<$crate::diesel_common::RunRow> = ::diesel::sql_query(
                    &$prep_sql(&format!("SELECT id, definition_id, params, state, started_at, completed_at, error,
                            parent_run_id, parent_node_name, created_at
                     FROM workflow_runs WHERE id = ? AND {}", $crate::diesel_common::RUN_IN_NAMESPACE)),
                )
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .load(&mut *conn)?;

                Ok(rows.into_iter().next().map($crate::diesel_common::run_from_row))
            }

            fn update_workflow_run_state(
                &self,
                run_id: &str,
                state: $crate::WorkflowState,
                error: Option<&str>,
            ) -> ::taskito_core::error::Result<()> {
                let mut conn = self.inner.conn()?;
                ::diesel::sql_query(
                    &$prep_sql(&format!(
                        "UPDATE workflow_runs SET state = ?, error = ? WHERE id = ? AND {}",
                        $crate::diesel_common::RUN_IN_NAMESPACE
                    )),
                )
                .bind::<::diesel::sql_types::Text, _>(state.as_str())
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(error)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .execute(&mut *conn)?;
                Ok(())
            }

            fn set_workflow_run_started(
                &self,
                run_id: &str,
                started_at: i64,
            ) -> ::taskito_core::error::Result<()> {
                let mut conn = self.inner.conn()?;
                ::diesel::sql_query(
                    &$prep_sql(&format!(
                        "UPDATE workflow_runs SET state = 'running', started_at = ? WHERE id = ? AND {}",
                        $crate::diesel_common::RUN_IN_NAMESPACE
                    )),
                )
                .bind::<::diesel::sql_types::BigInt, _>(started_at)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .execute(&mut *conn)?;
                Ok(())
            }

            fn set_workflow_run_completed(
                &self,
                run_id: &str,
                completed_at: i64,
            ) -> ::taskito_core::error::Result<()> {
                let mut conn = self.inner.conn()?;
                ::diesel::sql_query(
                    &$prep_sql(&format!(
                        "UPDATE workflow_runs SET completed_at = ? WHERE id = ? AND {}",
                        $crate::diesel_common::RUN_IN_NAMESPACE
                    )),
                )
                .bind::<::diesel::sql_types::BigInt, _>(completed_at)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .execute(&mut *conn)?;
                Ok(())
            }

            fn list_workflow_runs(
                &self,
                definition_name: Option<&str>,
                state: Option<$crate::WorkflowState>,
                limit: i64,
                offset: i64,
            ) -> ::taskito_core::error::Result<Vec<$crate::WorkflowRun>> {
                let mut conn = self.inner.conn()?;

                let rows: Vec<$crate::diesel_common::RunRow> = match (definition_name, state) {
                    (Some(name), Some(st)) => ::diesel::sql_query(
                        &$prep_sql(&format!("SELECT r.id, r.definition_id, r.params, r.state, r.started_at,
                                r.completed_at, r.error, r.parent_run_id, r.parent_node_name,
                                r.created_at
                         FROM workflow_runs r
                         JOIN workflow_definitions d ON r.definition_id = d.id
                         WHERE d.name = ? AND r.state = ? AND {}
                         ORDER BY r.created_at DESC LIMIT ? OFFSET ?", $crate::diesel_common::R_IN_NAMESPACE)),
                    )
                    .bind::<::diesel::sql_types::Text, _>(name)
                    .bind::<::diesel::sql_types::Text, _>(st.as_str())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::BigInt, _>(limit)
                    .bind::<::diesel::sql_types::BigInt, _>(offset)
                    .load(&mut *conn)?,
                    (Some(name), None) => ::diesel::sql_query(
                        &$prep_sql(&format!("SELECT r.id, r.definition_id, r.params, r.state, r.started_at,
                                r.completed_at, r.error, r.parent_run_id, r.parent_node_name,
                                r.created_at
                         FROM workflow_runs r
                         JOIN workflow_definitions d ON r.definition_id = d.id
                         WHERE d.name = ? AND {}
                         ORDER BY r.created_at DESC LIMIT ? OFFSET ?", $crate::diesel_common::R_IN_NAMESPACE)),
                    )
                    .bind::<::diesel::sql_types::Text, _>(name)
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::BigInt, _>(limit)
                    .bind::<::diesel::sql_types::BigInt, _>(offset)
                    .load(&mut *conn)?,
                    (None, Some(st)) => ::diesel::sql_query(
                        &$prep_sql(&format!("SELECT id, definition_id, params, state, started_at, completed_at,
                                error, parent_run_id, parent_node_name, created_at
                         FROM workflow_runs WHERE state = ? AND {}
                         ORDER BY created_at DESC LIMIT ? OFFSET ?", $crate::diesel_common::RUN_IN_NAMESPACE)),
                    )
                    .bind::<::diesel::sql_types::Text, _>(st.as_str())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::BigInt, _>(limit)
                    .bind::<::diesel::sql_types::BigInt, _>(offset)
                    .load(&mut *conn)?,
                    (None, None) => ::diesel::sql_query(
                        &$prep_sql(&format!("SELECT id, definition_id, params, state, started_at, completed_at,
                                error, parent_run_id, parent_node_name, created_at
                         FROM workflow_runs WHERE {}
                         ORDER BY created_at DESC LIMIT ? OFFSET ?", $crate::diesel_common::RUN_IN_NAMESPACE)),
                    )
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::BigInt, _>(limit)
                    .bind::<::diesel::sql_types::BigInt, _>(offset)
                    .load(&mut *conn)?,
                };

                Ok(rows
                    .into_iter()
                    .map($crate::diesel_common::run_from_row)
                    .collect())
            }

            fn list_workflow_runs_after(
                &self,
                definition_name: Option<&str>,
                state: Option<$crate::WorkflowState>,
                limit: i64,
                after: Option<(i64, &str)>,
            ) -> ::taskito_core::error::Result<Vec<$crate::WorkflowRun>> {
                let mut conn = self.inner.conn()?;

                // No cursor → sentinel `(i64::MAX, "")`: `created_at < MAX` holds
                // for every real timestamp, so the keyset clause matches all rows
                // and the first page is returned. Keeps one SQL per filter combo.
                let (cursor_created_at, cursor_id) = after.unwrap_or((i64::MAX, ""));

                let rows: Vec<$crate::diesel_common::RunRow> = match (definition_name, state) {
                    (Some(name), Some(st)) => ::diesel::sql_query(
                        &$prep_sql(&format!("SELECT r.id, r.definition_id, r.params, r.state, r.started_at,
                                r.completed_at, r.error, r.parent_run_id, r.parent_node_name,
                                r.created_at
                         FROM workflow_runs r
                         JOIN workflow_definitions d ON r.definition_id = d.id
                         WHERE d.name = ? AND r.state = ? AND {}
                           AND (r.created_at < ? OR (r.created_at = ? AND r.id < ?))
                         ORDER BY r.created_at DESC, r.id DESC LIMIT ?", $crate::diesel_common::R_IN_NAMESPACE)),
                    )
                    .bind::<::diesel::sql_types::Text, _>(name)
                    .bind::<::diesel::sql_types::Text, _>(st.as_str())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::BigInt, _>(cursor_created_at)
                    .bind::<::diesel::sql_types::BigInt, _>(cursor_created_at)
                    .bind::<::diesel::sql_types::Text, _>(cursor_id)
                    .bind::<::diesel::sql_types::BigInt, _>(limit)
                    .load(&mut *conn)?,
                    (Some(name), None) => ::diesel::sql_query(
                        &$prep_sql(&format!("SELECT r.id, r.definition_id, r.params, r.state, r.started_at,
                                r.completed_at, r.error, r.parent_run_id, r.parent_node_name,
                                r.created_at
                         FROM workflow_runs r
                         JOIN workflow_definitions d ON r.definition_id = d.id
                         WHERE d.name = ? AND {}
                           AND (r.created_at < ? OR (r.created_at = ? AND r.id < ?))
                         ORDER BY r.created_at DESC, r.id DESC LIMIT ?", $crate::diesel_common::R_IN_NAMESPACE)),
                    )
                    .bind::<::diesel::sql_types::Text, _>(name)
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::BigInt, _>(cursor_created_at)
                    .bind::<::diesel::sql_types::BigInt, _>(cursor_created_at)
                    .bind::<::diesel::sql_types::Text, _>(cursor_id)
                    .bind::<::diesel::sql_types::BigInt, _>(limit)
                    .load(&mut *conn)?,
                    (None, Some(st)) => ::diesel::sql_query(
                        &$prep_sql(&format!("SELECT id, definition_id, params, state, started_at, completed_at,
                                error, parent_run_id, parent_node_name, created_at
                         FROM workflow_runs WHERE state = ? AND {}
                           AND (created_at < ? OR (created_at = ? AND id < ?))
                         ORDER BY created_at DESC, id DESC LIMIT ?", $crate::diesel_common::RUN_IN_NAMESPACE)),
                    )
                    .bind::<::diesel::sql_types::Text, _>(st.as_str())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::BigInt, _>(cursor_created_at)
                    .bind::<::diesel::sql_types::BigInt, _>(cursor_created_at)
                    .bind::<::diesel::sql_types::Text, _>(cursor_id)
                    .bind::<::diesel::sql_types::BigInt, _>(limit)
                    .load(&mut *conn)?,
                    (None, None) => ::diesel::sql_query(
                        &$prep_sql(&format!("SELECT id, definition_id, params, state, started_at, completed_at,
                                error, parent_run_id, parent_node_name, created_at
                         FROM workflow_runs
                         WHERE {} AND (created_at < ? OR (created_at = ? AND id < ?))
                         ORDER BY created_at DESC, id DESC LIMIT ?", $crate::diesel_common::RUN_IN_NAMESPACE)),
                    )
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                    .bind::<::diesel::sql_types::BigInt, _>(cursor_created_at)
                    .bind::<::diesel::sql_types::BigInt, _>(cursor_created_at)
                    .bind::<::diesel::sql_types::Text, _>(cursor_id)
                    .bind::<::diesel::sql_types::BigInt, _>(limit)
                    .load(&mut *conn)?,
                };

                Ok(rows
                    .into_iter()
                    .map($crate::diesel_common::run_from_row)
                    .collect())
            }

            fn create_workflow_node(
                &self,
                node: &$crate::WorkflowNode,
            ) -> ::taskito_core::error::Result<()> {
                // A create against a run this store cannot see would silently
                // drop the node, so it reports the run missing instead.
                if self.run_out_of_scope(&node.run_id)? {
                    return Err($crate::WorkflowError::RunNotFound(node.run_id.clone()).into());
                }
                let mut conn = self.inner.conn()?;
                ::diesel::sql_query(
                    &$prep_sql("INSERT INTO workflow_nodes
                        (id, run_id, node_name, job_id, status, result_hash,
                         fan_out_count, fan_in_data, started_at, completed_at, error,
                         compensation_job_id, compensation_started_at,
                         compensation_completed_at, compensation_error)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
                )
                .bind::<::diesel::sql_types::Text, _>(&node.id)
                .bind::<::diesel::sql_types::Text, _>(&node.run_id)
                .bind::<::diesel::sql_types::Text, _>(&node.node_name)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.job_id)
                .bind::<::diesel::sql_types::Text, _>(node.status.as_str())
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.result_hash)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Integer>, _>(node.fan_out_count)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.fan_in_data)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(node.started_at)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(node.completed_at)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.error)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.compensation_job_id)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(node.compensation_started_at)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(node.compensation_completed_at)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.compensation_error)
                .execute(&mut *conn)?;
                Ok(())
            }

            fn create_workflow_nodes_batch(
                &self,
                nodes: &[$crate::WorkflowNode],
            ) -> ::taskito_core::error::Result<()> {
                // Checked per distinct run, not just the first node: a mixed
                // batch would otherwise smuggle a foreign node in behind an
                // in-scope one, in the same transaction.
                let mut checked = ::std::collections::HashSet::new();
                for node in nodes {
                    if checked.insert(node.run_id.as_str())
                        && self.run_out_of_scope(&node.run_id)?
                    {
                        return Err(
                            $crate::WorkflowError::RunNotFound(node.run_id.clone()).into()
                        );
                    }
                }
                use ::diesel::connection::Connection;
                let mut conn = self.inner.conn()?;
                conn.transaction::<_, ::taskito_core::error::QueueError, _>(|conn| {
                    for node in nodes {
                        ::diesel::sql_query(
                            &$prep_sql("INSERT INTO workflow_nodes
                                (id, run_id, node_name, job_id, status, result_hash,
                                 fan_out_count, fan_in_data, started_at, completed_at, error,
                                 compensation_job_id, compensation_started_at,
                                 compensation_completed_at, compensation_error)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
                        )
                        .bind::<::diesel::sql_types::Text, _>(&node.id)
                        .bind::<::diesel::sql_types::Text, _>(&node.run_id)
                        .bind::<::diesel::sql_types::Text, _>(&node.node_name)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.job_id)
                        .bind::<::diesel::sql_types::Text, _>(node.status.as_str())
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.result_hash)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Integer>, _>(node.fan_out_count)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.fan_in_data)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(node.started_at)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(node.completed_at)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.error)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.compensation_job_id)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(node.compensation_started_at)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::BigInt>, _>(node.compensation_completed_at)
                        .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(&node.compensation_error)
                        .execute(conn)?;
                    }
                    Ok(())
                })
            }

            fn get_workflow_node(
                &self,
                run_id: &str,
                node_name: &str,
            ) -> ::taskito_core::error::Result<Option<$crate::WorkflowNode>> {
                if self.run_out_of_scope(run_id)? {
                    return Ok(None);
                }
                let mut conn = self.inner.conn()?;
                let rows: Vec<$crate::diesel_common::NodeRow> = ::diesel::sql_query(
                    &$prep_sql("SELECT id, run_id, node_name, job_id, status, result_hash,
                            fan_out_count, fan_in_data, started_at, completed_at, error,
                            compensation_job_id, compensation_started_at,
                            compensation_completed_at, compensation_error
                     FROM workflow_nodes WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .load(&mut *conn)?;

                Ok(rows
                    .into_iter()
                    .next()
                    .map($crate::diesel_common::node_from_row))
            }

            fn get_workflow_nodes(
                &self,
                run_id: &str,
            ) -> ::taskito_core::error::Result<Vec<$crate::WorkflowNode>> {
                if self.run_out_of_scope(run_id)? {
                    return Ok(Vec::new());
                }
                let mut conn = self.inner.conn()?;
                let rows: Vec<$crate::diesel_common::NodeRow> = ::diesel::sql_query(
                    &$prep_sql("SELECT id, run_id, node_name, job_id, status, result_hash,
                            fan_out_count, fan_in_data, started_at, completed_at, error,
                            compensation_job_id, compensation_started_at,
                            compensation_completed_at, compensation_error
                     FROM workflow_nodes WHERE run_id = ?"),
                )
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .load(&mut *conn)?;

                Ok(rows
                    .into_iter()
                    .map($crate::diesel_common::node_from_row)
                    .collect())
            }

            fn update_workflow_node_status(
                &self,
                run_id: &str,
                node_name: &str,
                status: $crate::WorkflowNodeStatus,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes SET status = ? WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::Text, _>(status.as_str())
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn set_workflow_node_job(
                &self,
                run_id: &str,
                node_name: &str,
                job_id: &str,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes SET job_id = ? WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::Text, _>(job_id)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn set_workflow_node_started(
                &self,
                run_id: &str,
                node_name: &str,
                started_at: i64,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes SET status = 'running', started_at = ?
                     WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::BigInt, _>(started_at)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn set_workflow_node_completed(
                &self,
                run_id: &str,
                node_name: &str,
                completed_at: i64,
                result_hash: Option<&str>,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes SET status = 'completed', completed_at = ?, result_hash = ?
                     WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::BigInt, _>(completed_at)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(result_hash)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn set_workflow_node_error(
                &self,
                run_id: &str,
                node_name: &str,
                error: &str,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes SET status = 'failed', error = ?
                     WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::Text, _>(error)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn set_workflow_node_fan_out_count(
                &self,
                run_id: &str,
                node_name: &str,
                count: i32,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes SET fan_out_count = ?, status = 'running'
                     WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::Integer, _>(count)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn set_workflow_node_running(
                &self,
                run_id: &str,
                node_name: &str,
                started_at: i64,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes SET status = 'running', started_at = ?
                     WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::BigInt, _>(started_at)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn finalize_fan_out_parent(
                &self,
                run_id: &str,
                node_name: &str,
                succeeded: bool,
                error: Option<&str>,
                completed_at: i64,
            ) -> ::taskito_core::error::Result<bool> {
                if self.run_out_of_scope(run_id)? {
                    return Ok(false);
                }
                let mut conn = self.inner.conn()?;
                let affected = if succeeded {
                    ::diesel::sql_query(
                        &$prep_sql("UPDATE workflow_nodes
                         SET status = 'completed', completed_at = ?
                         WHERE run_id = ? AND node_name = ?
                           AND status NOT IN ('completed', 'failed', 'skipped', 'cache_hit')"),
                    )
                    .bind::<::diesel::sql_types::BigInt, _>(completed_at)
                    .bind::<::diesel::sql_types::Text, _>(run_id)
                    .bind::<::diesel::sql_types::Text, _>(node_name)
                    .execute(&mut *conn)?
                } else {
                    ::diesel::sql_query(
                        &$prep_sql("UPDATE workflow_nodes
                         SET status = 'failed', error = ?
                         WHERE run_id = ? AND node_name = ?
                           AND status NOT IN ('completed', 'failed', 'skipped', 'cache_hit')"),
                    )
                    .bind::<::diesel::sql_types::Text, _>(error.unwrap_or("fan-out child failed"))
                    .bind::<::diesel::sql_types::Text, _>(run_id)
                    .bind::<::diesel::sql_types::Text, _>(node_name)
                    .execute(&mut *conn)?
                };
                Ok(affected > 0)
            }

            fn get_workflow_nodes_by_prefix(
                &self,
                run_id: &str,
                prefix: &str,
            ) -> ::taskito_core::error::Result<Vec<$crate::WorkflowNode>> {
                if self.run_out_of_scope(run_id)? {
                    return Ok(Vec::new());
                }
                let mut conn = self.inner.conn()?;
                let pattern = format!("{prefix}%");
                let rows: Vec<$crate::diesel_common::NodeRow> = ::diesel::sql_query(
                    &$prep_sql("SELECT id, run_id, node_name, job_id, status, result_hash,
                            fan_out_count, fan_in_data, started_at, completed_at, error,
                            compensation_job_id, compensation_started_at,
                            compensation_completed_at, compensation_error
                     FROM workflow_nodes WHERE run_id = ? AND node_name LIKE ?"),
                )
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(&pattern)
                .load(&mut *conn)?;

                Ok(rows
                    .into_iter()
                    .map($crate::diesel_common::node_from_row)
                    .collect())
            }

            fn get_ready_workflow_nodes(
                &self,
                run_id: &str,
                dag_json: &str,
            ) -> ::taskito_core::error::Result<Vec<$crate::WorkflowNode>> {
                let all_nodes =
                    <Self as $crate::storage::WorkflowStorage>::get_workflow_nodes(self, run_id)?;
                $crate::common::compute_ready_nodes(all_nodes, dag_json)
            }

            fn get_child_workflow_runs(
                &self,
                parent_run_id: &str,
            ) -> ::taskito_core::error::Result<Vec<$crate::WorkflowRun>> {
                let mut conn = self.inner.conn()?;
                let rows: Vec<$crate::diesel_common::RunRow> = ::diesel::sql_query(
                    &$prep_sql(&format!("SELECT id, definition_id, params, state, started_at, completed_at,
                            error, parent_run_id, parent_node_name, created_at
                     FROM workflow_runs WHERE parent_run_id = ? AND {}",
                        $crate::diesel_common::RUN_IN_NAMESPACE
                    )),
                )
                .bind::<::diesel::sql_types::Text, _>(parent_run_id)
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .bind::<::diesel::sql_types::Nullable<::diesel::sql_types::Text>, _>(self.namespace.as_deref())
                .load(&mut *conn)?;

                Ok(rows
                    .into_iter()
                    .map($crate::diesel_common::run_from_row)
                    .collect())
            }

            fn set_workflow_node_compensation_job(
                &self,
                run_id: &str,
                node_name: &str,
                compensation_job_id: &str,
                started_at: i64,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes
                       SET status = 'compensating',
                           compensation_job_id = ?,
                           compensation_started_at = ?
                     WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::Text, _>(compensation_job_id)
                .bind::<::diesel::sql_types::BigInt, _>(started_at)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn set_workflow_node_compensated(
                &self,
                run_id: &str,
                node_name: &str,
                completed_at: i64,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes
                       SET status = 'compensated',
                           compensation_completed_at = ?
                     WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::BigInt, _>(completed_at)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }

            fn set_workflow_node_compensation_failed(
                &self,
                run_id: &str,
                node_name: &str,
                error: &str,
                completed_at: i64,
            ) -> ::taskito_core::error::Result<()> {
                if self.run_out_of_scope(run_id)? {
                    return $crate::diesel_common::require_touched(0, run_id, node_name);
                }
                let mut conn = self.inner.conn()?;
                let affected = ::diesel::sql_query(
                    &$prep_sql("UPDATE workflow_nodes
                       SET status = 'compensation_failed',
                           compensation_completed_at = ?,
                           compensation_error = ?
                     WHERE run_id = ? AND node_name = ?"),
                )
                .bind::<::diesel::sql_types::BigInt, _>(completed_at)
                .bind::<::diesel::sql_types::Text, _>(error)
                .bind::<::diesel::sql_types::Text, _>(run_id)
                .bind::<::diesel::sql_types::Text, _>(node_name)
                .execute(&mut *conn)?;
                $crate::diesel_common::require_touched(affected, run_id, node_name)
            }
        }
    };
}

pub(crate) use impl_workflow_diesel_ops;
