//! Read-only inspection entry points for `NativeQueue`.

use std::collections::{HashMap, HashSet};

use jni::objects::{JClass, JString};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;
use serde::Serialize;
use taskito_core::job::Job;
use taskito_core::Storage;

use super::borrow_queue;
use crate::convert::{
    status_code, to_json, CircuitBreakerView, JobErrorView, JobFilter, JobView, MetricView,
    PageView, StatsView, WorkerView,
};
use crate::error::BindingError;
use crate::ffi::{guard, new_string, read_optional_string, read_string};

/// One `dependency -> dependent` edge in a job DAG.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DagEdgeView {
    from: String,
    to: String,
}

/// The dependency DAG reachable from a job: full job rows plus edges.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JobDagView<'a> {
    nodes: Vec<JobView<'a>>,
    edges: Vec<DagEdgeView>,
}

const DEFAULT_LIMIT: i64 = 50;

/// `String stats(long handle)` — job counts by status across all queues.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_stats<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        new_string(
            env,
            to_json(&StatsView::from(
                queue.storage.stats(queue.namespace.as_deref())?,
            ))?,
        )
    })
}

/// `String statsByQueue(long handle, String queue)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_statsByQueue<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    queue_name: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &queue_name)?;
        let stats = queue
            .storage
            .stats_by_queue(&name, queue.namespace.as_deref())?;
        new_string(env, to_json(&StatsView::from(stats))?)
    })
}

/// `long countPendingByQueue(long handle, String queue)` — the lean primitive
/// behind the `maxPending` admission cap.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_countPendingByQueue<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    queue_name: JString<'local>,
) -> jlong {
    guard(&mut env, 0, |env| {
        let queue = unsafe { borrow_queue(handle) };
        let name = read_string(env, &queue_name)?;
        Ok(queue.storage.count_pending_by_queue(&name)?)
    })
}

/// `String statsAllQueues(long handle)` — a JSON map of queue name to counts.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_statsAllQueues<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let all = queue.storage.stats_all_queues(queue.namespace.as_deref())?;
        let mapped: HashMap<String, StatsView> = all
            .into_iter()
            .map(|(k, v)| (k, StatsView::from(v)))
            .collect();
        new_string(env, to_json(&mapped)?)
    })
}

/// `String listJobs(long handle, String filterJson)` — a JSON array of jobs.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listJobs<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    filter_json: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let raw = read_string(env, &filter_json)?;
        let filter: JobFilter = crate::convert::parse_json(&raw, "job filter")?;
        // Reject an unknown status so a typo fails loudly instead of matching all.
        let status = match filter.status.as_deref() {
            Some(s) => Some(
                status_code(s).ok_or_else(|| BindingError::new(format!("unknown status '{s}'")))?,
            ),
            None => None,
        };
        let limit = filter.limit.unwrap_or(DEFAULT_LIMIT).max(0);
        let offset = filter.offset.unwrap_or(0).max(0);
        let jobs = queue.storage.list_jobs(
            status,
            filter.queue.as_deref(),
            filter.task.as_deref(),
            limit,
            offset,
            queue.namespace.as_deref(),
        )?;
        let views: Vec<JobView> = jobs.iter().map(JobView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String listJobsAfter(long handle, String filterJson, String afterOrNull)` —
/// keyset-paginated `listJobs`, ordered by created time. Returns a `Page`; its
/// `nextCursor` is absent on the last page.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listJobsAfter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    filter_json: JString<'local>,
    after: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let raw = read_string(env, &filter_json)?;
        let filter: JobFilter = crate::convert::parse_json(&raw, "job filter")?;
        // Reject an unknown status so a typo fails loudly instead of matching all.
        let status = match filter.status.as_deref() {
            Some(s) => Some(
                status_code(s).ok_or_else(|| BindingError::new(format!("unknown status '{s}'")))?,
            ),
            None => None,
        };
        let limit = filter.limit.unwrap_or(DEFAULT_LIMIT).max(0);
        // A malformed cursor is a bad request, not a reason to silently restart
        // from the first page.
        let after = read_optional_string(env, &after)?;
        let cursor = after
            .as_deref()
            .map(taskito_core::storage::cursor::decode_cursor)
            .transpose()?;
        let jobs = queue.storage.list_jobs_after(
            status,
            filter.queue.as_deref(),
            filter.task.as_deref(),
            limit,
            cursor,
            queue.namespace.as_deref(),
        )?;
        let next_cursor =
            taskito_core::storage::cursor::next_cursor(&jobs, limit, |j| (j.created_at, &j.id));
        let page = PageView {
            items: jobs.iter().map(JobView::from).collect(),
            next_cursor,
        };
        new_string(env, to_json(&page)?)
    })
}

/// `String listArchivedAfter(long handle, long limit, String afterOrNull)` —
/// keyset-paginated `listArchived`, ordered by completed time.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listArchivedAfter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    limit: jlong,
    after: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let limit = limit.max(0);
        let after = read_optional_string(env, &after)?;
        let cursor = after
            .as_deref()
            .map(taskito_core::storage::cursor::decode_cursor)
            .transpose()?;
        let jobs = queue.storage.list_archived_after(limit, cursor)?;
        // Archived rows always carry `completed_at`; the fallback keeps the key
        // non-null for a row written before it was set.
        let next_cursor = taskito_core::storage::cursor::next_cursor(&jobs, limit, |j| {
            (j.completed_at.unwrap_or(j.created_at), &j.id)
        });
        let page = PageView {
            items: jobs.iter().map(JobView::from).collect(),
            next_cursor,
        };
        new_string(env, to_json(&page)?)
    })
}

/// `String jobErrors(long handle, String jobId)` — one entry per failed attempt.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_jobErrors<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let id = read_string(env, &job_id)?;
        let errors = queue.storage.get_job_errors(&id)?;
        let views: Vec<JobErrorView> = errors.iter().map(JobErrorView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String metrics(long handle, String taskNameOrNull, long sinceMs)`.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_metrics<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    task_name: JString<'local>,
    since_ms: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let task = read_optional_string(env, &task_name)?;
        let metrics =
            queue
                .storage
                .get_metrics(task.as_deref(), since_ms, queue.namespace.as_deref())?;
        let views: Vec<MetricView> = metrics.iter().map(MetricView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String listWorkers(long handle)` — registered workers.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listWorkers<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let workers = queue.storage.list_workers()?;
        let views: Vec<WorkerView> = workers.iter().map(WorkerView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String listCircuitBreakers(long handle)` — every configured task's breaker state.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_listCircuitBreakers<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let breakers = queue.storage.list_circuit_breakers()?;
        let views: Vec<CircuitBreakerView> =
            breakers.iter().map(CircuitBreakerView::from).collect();
        new_string(env, to_json(&views)?)
    })
}

/// `String jobDag(long handle, String jobId)` — the dependency DAG reachable
/// from a job as a JSON `{nodes, edges}` object. Walks dependencies and
/// dependents in both directions, deduping nodes and edges.
#[no_mangle]
pub extern "system" fn Java_org_byteveda_taskito_internal_NativeQueue_jobDag<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    job_id: JString<'local>,
) -> jstring {
    guard(&mut env, std::ptr::null_mut(), |env| {
        let queue = unsafe { borrow_queue(handle) };
        let start = read_string(env, &job_id)?;

        let mut visited: HashSet<String> = HashSet::new();
        // Both endpoints of an edge report it; dedupe by (from, to).
        let mut seen_edges: HashSet<(String, String)> = HashSet::new();
        // Keep the jobs alive so the borrowing `JobView`s can be built at the end.
        let mut jobs: Vec<Job> = Vec::new();
        let mut edges: Vec<DagEdgeView> = Vec::new();
        let mut pending = vec![start];
        while let Some(current) = pending.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            let Some(job) = queue.storage.get_job(&current)? else {
                continue;
            };
            jobs.push(job);
            for dep_id in queue.storage.get_dependencies(&current)? {
                if seen_edges.insert((dep_id.clone(), current.clone())) {
                    edges.push(DagEdgeView {
                        from: dep_id.clone(),
                        to: current.clone(),
                    });
                }
                pending.push(dep_id);
            }
            for dep_id in queue.storage.get_dependents(&current)? {
                if seen_edges.insert((current.clone(), dep_id.clone())) {
                    edges.push(DagEdgeView {
                        from: current.clone(),
                        to: dep_id.clone(),
                    });
                }
                pending.push(dep_id);
            }
        }
        let nodes: Vec<JobView> = jobs.iter().map(JobView::from).collect();
        new_string(env, to_json(&JobDagView { nodes, edges })?)
    })
}
