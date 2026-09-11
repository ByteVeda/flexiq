//! `fq enqueue` — submit one job.

use anyhow::{anyhow, Result};
use prost_types::{Duration as ProtoDuration, Timestamp};

use crate::cli::EnqueueArgs;
use crate::connect::Client;
use crate::{args, error, output, pb};

/// Milliseconds in a second.
const MILLIS_PER_SECOND: i64 = 1_000;

/// Nanoseconds in a millisecond.
const NANOS_PER_MILLI: i32 = 1_000_000;

/// Submit the job and print what came back.
pub async fn run(client: &mut Client, cli_args: &EnqueueArgs, json: bool) -> Result<()> {
    let request = request(cli_args, chrono::Utc::now().timestamp_millis())?;
    let response = client
        .enqueue(request)
        .await
        .map_err(|status| anyhow!("{}", error::describe(&status)))?
        .into_inner();

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&output::enqueue_json(&response))?
        );
    } else {
        let rows = response
            .job
            .as_ref()
            .map(|job| vec![output::job_row(job)])
            .unwrap_or_default();
        print!("{}", output::table(&output::JOB_COLUMNS, &rows));
        if response.deduplicated {
            println!("deduplicated: an active job already held this unique key");
        }
    }
    Ok(())
}

/// The whole request, body included.
pub fn request(cli_args: &EnqueueArgs, now_ms: i64) -> Result<pb::EnqueueRequest> {
    let body = args::structured(&cli_args.args, &cli_args.kwargs)?;
    Ok(pb::EnqueueRequest {
        task_name: cli_args.task.clone(),
        // The `structured` arm, always: the server encodes the CBOR envelope
        // with the one in-tree encoder, so this binary carries no second one.
        body: Some(pb::enqueue_request::Body::Structured(body)),
        options: Some(options(cli_args, now_ms)?),
    })
}

/// The producer-settable knobs.
///
/// `now_ms` is passed in rather than read here because two of the flags are
/// relative and the wire's fields are absolute; a caller that supplies the
/// clock is a function a test can pin.
pub fn options(cli_args: &EnqueueArgs, now_ms: i64) -> Result<pb::EnqueueOptions> {
    Ok(pb::EnqueueOptions {
        // Empty means "the server's default queue", which is what an omitted
        // flag means too.
        queue: cli_args.queue.clone().unwrap_or_default(),
        priority: cli_args.priority.unwrap_or_default(),
        max_retries: cli_args.max_retries.unwrap_or_default(),
        scheduled_at: instant_after(now_ms, cli_args.delay_ms, "--delay-ms")?,
        timeout: cli_args.timeout_ms.map(duration),
        unique_key: cli_args.unique_key.clone(),
        metadata: cli_args.metadata.clone(),
        notes: cli_args.notes.clone(),
        depends_on: cli_args.depends_on.clone(),
        expires_at: instant_after(now_ms, cli_args.expires_in_ms, "--expires-in-ms")?,
        result_ttl: cli_args.result_ttl_ms.map(duration),
        // Debounce is a three-field message with its own invariants, not a
        // flag; a CLI that offered half of it would be worse than one that
        // offers none.
        debounce: None,
    })
}

/// `now_ms + offset` as an absolute instant, refusing an offset that does not
/// fit.
///
/// An unchecked `+` here panics under `overflow-checks` and wraps without them
/// — and a wrapped offset is the worse outcome, because it schedules the job at
/// an instant in the distant past rather than failing.
fn instant_after(now_ms: i64, offset_ms: Option<i64>, flag: &str) -> Result<Option<Timestamp>> {
    offset_ms
        .map(|offset| {
            now_ms
                .checked_add(offset)
                .map(timestamp)
                .ok_or_else(|| anyhow!("`{flag} {offset}` is too far from now to be an instant"))
        })
        .transpose()
}

/// Unix milliseconds as a `Timestamp`.
///
/// Euclidean division so a pre-epoch instant keeps a non-negative `nanos`,
/// which is the only spelling a `Timestamp` may carry.
fn timestamp(millis: i64) -> Timestamp {
    Timestamp {
        seconds: millis.div_euclid(MILLIS_PER_SECOND),
        nanos: millis.rem_euclid(MILLIS_PER_SECOND) as i32 * NANOS_PER_MILLI,
    }
}

/// A span in milliseconds as a `Duration`.
fn duration(millis: i64) -> ProtoDuration {
    ProtoDuration {
        seconds: millis / MILLIS_PER_SECOND,
        nanos: (millis % MILLIS_PER_SECOND) as i32 * NANOS_PER_MILLI,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> EnqueueArgs {
        EnqueueArgs {
            task: "send_email".into(),
            args: vec!["a@b.c".into()],
            kwargs: vec!["retries=2".into()],
            queue: Some("mail".into()),
            priority: Some(5),
            max_retries: Some(3),
            delay_ms: Some(60_000),
            timeout_ms: Some(30_000),
            expires_in_ms: None,
            result_ttl_ms: Some(3_600_000),
            unique_key: Some("k".into()),
            metadata: Some(r#"{"a":1}"#.into()),
            notes: None,
            depends_on: vec!["job-0".into()],
        }
    }

    /// `--delay-ms` is relative and `scheduled_at` is absolute, so the
    /// conversion needs the clock — passed in so the test can pin it.
    #[test]
    fn a_delay_becomes_an_absolute_instant() {
        let options = options(&sample(), 1_757_500_000_000).expect("builds");
        let scheduled = options.scheduled_at.expect("a delay sets one");
        assert_eq!(scheduled.seconds, 1_757_500_060);
        assert_eq!(scheduled.nanos, 0);
    }

    #[test]
    fn no_delay_leaves_the_instant_unset() {
        let mut input = sample();
        input.delay_ms = None;
        assert!(options(&input, 1_757_500_000_000)
            .expect("builds")
            .scheduled_at
            .is_none());
    }

    #[test]
    fn every_option_reaches_the_wire() {
        let options = options(&sample(), 0).expect("builds");
        assert_eq!(options.queue, "mail");
        assert_eq!(options.priority, 5);
        assert_eq!(options.max_retries, 3);
        assert_eq!(options.timeout.expect("set").seconds, 30);
        assert_eq!(options.result_ttl.expect("set").seconds, 3_600);
        assert_eq!(options.unique_key.as_deref(), Some("k"));
        assert_eq!(options.metadata.as_deref(), Some(r#"{"a":1}"#));
        assert_eq!(options.depends_on, vec!["job-0".to_string()]);
        assert!(options.expires_at.is_none());
        assert!(options.notes.is_none());
        assert!(options.debounce.is_none());
    }

    /// An omitted queue is the empty string, which the door reads as "the
    /// default" — not as a queue literally named "".
    #[test]
    fn an_omitted_queue_is_the_empty_default() {
        let mut input = sample();
        input.queue = None;
        assert_eq!(options(&input, 0).expect("builds").queue, "");
    }

    /// An unchecked `+` would wrap here and schedule the job in the distant
    /// past, which is worse than refusing the flag.
    #[test]
    fn an_offset_that_does_not_fit_is_refused_by_flag_name() {
        let mut input = sample();
        input.delay_ms = Some(i64::MAX);
        let error = options(&input, 1_757_500_000_000).expect_err("overflows");
        assert!(error.to_string().contains("--delay-ms"), "{error}");

        let mut input = sample();
        input.delay_ms = None;
        input.expires_in_ms = Some(i64::MAX);
        let error = options(&input, 1_757_500_000_000).expect_err("overflows");
        assert!(error.to_string().contains("--expires-in-ms"), "{error}");
    }

    #[test]
    fn the_body_is_the_structured_arm() {
        let request = request(&sample(), 0).expect("builds");
        assert_eq!(request.task_name, "send_email");
        let Some(pb::enqueue_request::Body::Structured(body)) = request.body else {
            panic!("fq sends the structured arm");
        };
        assert_eq!(body.args.len(), 1);
        assert!(body.kwargs.contains_key("retries"));
    }

    #[test]
    fn a_sub_second_timeout_keeps_its_nanos() {
        let mut input = sample();
        input.timeout_ms = Some(1_500);
        let timeout = options(&input, 0).expect("builds").timeout.expect("set");
        assert_eq!(timeout.seconds, 1);
        assert_eq!(timeout.nanos, 500_000_000);
    }
}
