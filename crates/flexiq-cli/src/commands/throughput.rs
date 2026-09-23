//! `fq throughput` — jobs finished per queue over a recent window.
//!
//! The wire carries counts, not rates: a rate implies a smoothing choice that
//! belongs to the reader. The table divides by the window the server echoes,
//! which is the one it applied; `--json` prints the message as sent.

use anyhow::Result;

use super::{emit, refused};
use crate::cli::ThroughputArgs;
use crate::connect::AdminClient;
use crate::output::admin::{
    throughput_header, throughput_json, throughput_rows, THROUGHPUT_COLUMNS,
};
use crate::time::span;
use crate::{output, pb};

/// The request. An omitted window stays unset, which the server reads as its
/// default of five minutes.
pub fn request(args: &ThroughputArgs) -> Result<pb::admin::GetThroughputRequest> {
    Ok(pb::admin::GetThroughputRequest {
        window: span(args.window_ms, "--window-ms")?,
    })
}

/// Fetch the counts and print them with their rates.
pub async fn run(client: &mut AdminClient, args: &ThroughputArgs, json: bool) -> Result<()> {
    let response = client
        .get_throughput(request(args)?)
        .await
        .map_err(refused)?
        .into_inner();
    emit(
        json,
        || throughput_json(&response),
        || {
            format!(
                "{}\n{}",
                throughput_header(&response),
                output::table(&THROUGHPUT_COLUMNS, &throughput_rows(&response))
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_omitted_window_is_the_servers_default() {
        assert!(request(&ThroughputArgs { window_ms: None })
            .expect("builds")
            .window
            .is_none());
    }

    #[test]
    fn a_window_reaches_the_wire_as_a_duration() {
        let window = request(&ThroughputArgs {
            window_ms: Some(90_500),
        })
        .expect("builds")
        .window
        .expect("set");
        assert_eq!(window.seconds, 90);
        assert_eq!(window.nanos, 500_000_000);
    }

    /// The server refuses a window above a day, but only one a `Duration` can
    /// carry reaches it; past that the door would saturate.
    #[test]
    fn a_window_no_duration_holds_is_refused_by_flag_name() {
        let error = request(&ThroughputArgs {
            window_ms: Some(i64::MAX),
        })
        .expect_err("too long");
        assert!(error.to_string().contains("--window-ms"), "{error}");
    }
}
