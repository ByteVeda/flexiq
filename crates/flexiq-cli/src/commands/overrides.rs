//! `fq overrides` — task and queue overrides.
//!
//! Two properties an operator must know, both printed in `--help`: a set
//! *replaces* the override (an omitted flag is cleared, not kept), and a
//! worker reads overrides when it starts, so a change reaches the next worker
//! start and no running worker.

use anyhow::Result;

use super::{emit, refused};
use crate::cli::{
    OverridesCommand, QueueNameArgs, SetQueueOverrideArgs, SetTaskOverrideArgs, TaskNameArgs,
};
use crate::connect::AdminClient;
use crate::output;
use crate::output::admin::{
    empty_json, list_overrides_json, override_rows, queue_override_envelope_json,
    queue_override_row, task_override_envelope_json, task_override_row, OVERRIDE_COLUMNS,
};
use crate::pb::admin as pb;
use crate::safe::escape;
use crate::time::span;

/// Dispatch the five verbs.
pub async fn run(client: &mut AdminClient, command: &OverridesCommand, json: bool) -> Result<()> {
    match command {
        OverridesCommand::List => list(client, json).await,
        OverridesCommand::SetTask(args) => set_task(client, args, json).await,
        OverridesCommand::ClearTask(args) => clear_task(client, args, json).await,
        OverridesCommand::SetQueue(args) => set_queue(client, args, json).await,
        OverridesCommand::ClearQueue(args) => clear_queue(client, args, json).await,
    }
}

/// `fq overrides list`: tasks then queues, each by name.
async fn list(client: &mut AdminClient, json: bool) -> Result<()> {
    let response = client
        .list_overrides(pb::ListOverridesRequest {})
        .await
        .map_err(refused)?
        .into_inner();
    emit(
        json,
        || list_overrides_json(&response),
        || output::table(&OVERRIDE_COLUMNS, &override_rows(&response)),
    )
}

/// The task override request. Each omitted flag leaves its field unset, and
/// on this RPC unset means "no longer overridden".
pub fn set_task_request(args: &SetTaskOverrideArgs) -> Result<pb::SetTaskOverrideRequest> {
    Ok(pb::SetTaskOverrideRequest {
        task_name: args.task.clone(),
        task_override: Some(pb::TaskOverride {
            rate_limit: args.rate_limit.clone(),
            max_concurrent: args.max_concurrent,
            max_retries: args.max_retries,
            retry_backoff: span(args.retry_backoff_ms, "--retry-backoff-ms")?,
            timeout: span(args.timeout_ms, "--timeout-ms")?,
            priority: args.priority,
            paused: args.paused,
            // Output only; the server ignores it on input.
            update_time: None,
        }),
    })
}

/// `fq overrides set-task`: prints the override as stored.
async fn set_task(client: &mut AdminClient, args: &SetTaskOverrideArgs, json: bool) -> Result<()> {
    let response = client
        .set_task_override(set_task_request(args)?)
        .await
        .map_err(refused)?
        .into_inner();
    let stored = response.task_override.as_ref();
    emit(
        json,
        || task_override_envelope_json(stored),
        || {
            let rows = stored
                .map(|value| vec![task_override_row(&args.task, value)])
                .unwrap_or_default();
            output::table(&OVERRIDE_COLUMNS, &rows)
        },
    )
}

/// The task override removal request.
pub fn clear_task_request(args: &TaskNameArgs) -> pb::ClearTaskOverrideRequest {
    pb::ClearTaskOverrideRequest {
        task_name: args.task.clone(),
    }
}

/// `fq overrides clear-task`.
async fn clear_task(client: &mut AdminClient, args: &TaskNameArgs, json: bool) -> Result<()> {
    client
        .clear_task_override(clear_task_request(args))
        .await
        .map_err(refused)?;
    emit(json, empty_json, || {
        format!("cleared task override {}\n", escape(&args.task))
    })
}

/// The queue override request. An omitted flag is cleared, as for a task.
pub fn set_queue_request(args: &SetQueueOverrideArgs) -> pb::SetQueueOverrideRequest {
    pb::SetQueueOverrideRequest {
        queue: args.queue.clone(),
        queue_override: Some(pb::QueueOverride {
            rate_limit: args.rate_limit.clone(),
            max_concurrent: args.max_concurrent,
            update_time: None,
        }),
    }
}

/// `fq overrides set-queue`: prints the override as stored.
async fn set_queue(
    client: &mut AdminClient,
    args: &SetQueueOverrideArgs,
    json: bool,
) -> Result<()> {
    let response = client
        .set_queue_override(set_queue_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    let stored = response.queue_override.as_ref();
    emit(
        json,
        || queue_override_envelope_json(stored),
        || {
            let rows = stored
                .map(|value| vec![queue_override_row(&args.queue, value)])
                .unwrap_or_default();
            output::table(&OVERRIDE_COLUMNS, &rows)
        },
    )
}

/// The queue override removal request.
pub fn clear_queue_request(args: &QueueNameArgs) -> pb::ClearQueueOverrideRequest {
    pb::ClearQueueOverrideRequest {
        queue: args.queue.clone(),
    }
}

/// `fq overrides clear-queue`.
async fn clear_queue(client: &mut AdminClient, args: &QueueNameArgs, json: bool) -> Result<()> {
    client
        .clear_queue_override(clear_queue_request(args))
        .await
        .map_err(refused)?;
    emit(json, empty_json, || {
        format!("cleared queue override {}\n", escape(&args.queue))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_args() -> SetTaskOverrideArgs {
        SetTaskOverrideArgs {
            task: "send".into(),
            rate_limit: None,
            max_concurrent: None,
            max_retries: None,
            retry_backoff_ms: None,
            timeout_ms: None,
            priority: None,
            paused: None,
        }
    }

    #[test]
    fn every_task_flag_reaches_the_wire() {
        let args = SetTaskOverrideArgs {
            rate_limit: Some("100/m".into()),
            max_concurrent: Some(2),
            max_retries: Some(3),
            retry_backoff_ms: Some(1_500),
            timeout_ms: Some(30_000),
            priority: Some(5),
            paused: Some(false),
            ..task_args()
        };
        let request = set_task_request(&args).expect("builds");
        assert_eq!(request.task_name, "send");
        let value = request.task_override.expect("always sent");
        assert_eq!(value.rate_limit.as_deref(), Some("100/m"));
        assert_eq!(value.max_concurrent, Some(2));
        assert_eq!(value.max_retries, Some(3));
        let backoff = value.retry_backoff.expect("set");
        assert_eq!((backoff.seconds, backoff.nanos), (1, 500_000_000));
        assert_eq!(value.timeout.expect("set").seconds, 30);
        assert_eq!(value.priority, Some(5));
        assert_eq!(value.paused, Some(false));
        assert!(value.update_time.is_none());
    }

    /// A replace, not a merge: every omitted flag is an unset field, which the
    /// server reads as "not overridden".
    #[test]
    fn an_omitted_task_flag_is_an_unset_field() {
        let value = set_task_request(&task_args())
            .expect("builds")
            .task_override
            .expect("always sent");
        assert_eq!(value, pb::TaskOverride::default());
    }

    #[test]
    fn a_duration_no_wire_type_holds_is_refused_by_flag_name() {
        let args = SetTaskOverrideArgs {
            timeout_ms: Some(i64::MAX),
            ..task_args()
        };
        let error = set_task_request(&args).expect_err("too long");
        assert!(error.to_string().contains("--timeout-ms"), "{error}");
        let args = SetTaskOverrideArgs {
            retry_backoff_ms: Some(i64::MIN),
            ..task_args()
        };
        let error = set_task_request(&args).expect_err("too long");
        assert!(error.to_string().contains("--retry-backoff-ms"), "{error}");
    }

    #[test]
    fn queue_flags_reach_the_wire_and_omitted_ones_stay_unset() {
        let request = set_queue_request(&SetQueueOverrideArgs {
            queue: "mail".into(),
            rate_limit: Some("10/s".into()),
            max_concurrent: None,
        });
        assert_eq!(request.queue, "mail");
        let value = request.queue_override.expect("always sent");
        assert_eq!(value.rate_limit.as_deref(), Some("10/s"));
        assert!(value.max_concurrent.is_none());
    }

    #[test]
    fn clear_takes_the_name() {
        assert_eq!(
            clear_task_request(&TaskNameArgs {
                task: "send".into()
            })
            .task_name,
            "send"
        );
        assert_eq!(
            clear_queue_request(&QueueNameArgs {
                queue: "mail".into()
            })
            .queue,
            "mail"
        );
    }
}
