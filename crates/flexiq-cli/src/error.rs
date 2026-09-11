//! What a refusal reads like.
//!
//! Every non-OK response from the door carries a `google.rpc.ErrorInfo` whose
//! `reason` comes from a closed list, and sometimes a `RetryInfo`. Printing the
//! code alone throws that away: `PermissionDenied: scope denied` is not
//! actionable, and `PermissionDenied: scope denied (SCOPE_DENIED, scope=produce)`
//! is.

use tonic::{Code, Status};
use tonic_types::StatusExt as _;

use crate::connect::TOKEN_VAR;
use crate::safe::escape;

/// Anything that went wrong that was not a usage error.
pub const EXIT_FAILURE: i32 = 1;

/// A usage error. clap exits with this itself; named here so the two agree.
pub const EXIT_USAGE: i32 = 2;

/// A status as an operator should read it.
///
/// Every string here came off the wire, so every string here is escaped. The
/// message and the `ErrorInfo` fields are whatever the peer sent, and this text
/// goes to a terminal — an escape byte in a status message would otherwise
/// repaint it, which is the one thing a failure path must not do.
pub fn describe(status: &Status) -> String {
    let mut text = format!("{:?}: {}", status.code(), escape(status.message()));

    let details = status.get_error_details();
    if let Some(info) = details.error_info() {
        // Metadata is a HashMap, so a stable order has to be imposed here or
        // two runs of the same failure print differently.
        let mut pairs: Vec<_> = info
            .metadata
            .iter()
            .map(|(key, value)| format!("{}={}", escape(key), escape(value)))
            .collect();
        pairs.sort();
        pairs.insert(0, escape(&info.reason));
        text.push_str(&format!(" ({})", pairs.join(", ")));
    }
    if let Some(delay) = details.retry_info().and_then(|info| info.retry_delay) {
        text.push_str(&format!("\n  retry after {:.3}s", delay.as_secs_f64()));
    }

    // One status covers a missing header, an unknown token, a revoked one, an
    // expired one and one minted for another namespace — deliberately, so the
    // door is not an existence oracle. The operator's first question is which
    // of those it is, and the variable is where they go to find out.
    if status.code() == Code::Unauthenticated {
        text.push_str(&format!(
            "\n  the server accepted no credential. Check {TOKEN_VAR}: it may be unset, \
             revoked, expired, or minted for another namespace."
        ));
    }
    text
}

#[cfg(test)]
mod tests {
    use tonic_types::ErrorDetails;

    use super::*;

    #[test]
    fn a_scope_denial_names_the_reason_and_the_scope() {
        let mut details = ErrorDetails::new();
        details.set_error_info(
            "SCOPE_DENIED",
            "flexiq",
            [("scope".to_string(), "produce".to_string())],
        );
        let status = Status::with_error_details(Code::PermissionDenied, "scope denied", details);
        let text = describe(&status);
        assert!(text.contains("PermissionDenied"), "{text}");
        assert!(text.contains("SCOPE_DENIED"), "{text}");
        assert!(text.contains("scope=produce"), "{text}");
    }

    /// Metadata arrives in a HashMap, so without a sort the same failure prints
    /// differently between runs.
    #[test]
    fn metadata_prints_in_a_stable_order() {
        let mut details = ErrorDetails::new();
        details.set_error_info(
            "QUEUE_FULL",
            "flexiq",
            [
                ("z".to_string(), "1".to_string()),
                ("a".to_string(), "2".to_string()),
            ],
        );
        let status = Status::with_error_details(Code::ResourceExhausted, "full", details);
        let text = describe(&status);
        assert!(text.contains("(QUEUE_FULL, a=2, z=1)"), "{text}");
    }

    #[test]
    fn an_unauthenticated_status_names_the_token_variable() {
        let text = describe(&Status::new(Code::Unauthenticated, "unauthenticated"));
        assert!(text.contains(TOKEN_VAR), "{text}");
    }

    #[test]
    fn a_plain_status_still_prints_its_code_and_message() {
        let text = describe(&Status::new(Code::NotFound, "no such job"));
        assert!(text.contains("NotFound"), "{text}");
        assert!(text.contains("no such job"), "{text}");
        assert!(!text.contains('('), "{text}");
    }

    /// The message and every `ErrorInfo` field are the peer's, and this text
    /// goes to a terminal. A plaintext peer — or a compromised one — must not
    /// be able to repaint it through a failure path.
    #[test]
    fn a_control_character_from_the_peer_is_spelled_out() {
        let mut details = ErrorDetails::new();
        details.set_error_info(
            "EVIL\u{1b}[2K",
            "flexiq",
            [("k\u{1b}y".to_string(), "v\nalue".to_string())],
        );
        let status = Status::with_error_details(Code::Internal, "boom\u{1b}[2Kcleared", details);
        let text = describe(&status);
        assert!(!text.contains('\u{1b}'), "{text:?}");
        assert!(text.contains("boom\\u{1b}[2Kcleared"), "{text:?}");
        assert!(text.contains("EVIL\\u{1b}[2K"), "{text:?}");
        assert!(text.contains("k\\u{1b}y=v\\nalue"), "{text:?}");
        // The hint line is the only newline this may carry.
        assert_eq!(text.lines().count(), 1);
    }

    #[test]
    fn a_retry_delay_is_printed() {
        let mut details = ErrorDetails::new();
        details.set_retry_info(Some(std::time::Duration::from_millis(1_500)));
        let status = Status::with_error_details(Code::Unavailable, "busy", details);
        assert!(describe(&status).contains("retry after 1.500s"));
    }
}
