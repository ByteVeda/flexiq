//! Escaping for values that arrived over the wire and end up on a terminal.
//!
//! Everything `fq` prints about a job — its task name, a queue, an error
//! message, the reason on an `ErrorInfo` — is a string the server sent, and the
//! door validates almost none of it. An escape byte in any of them repaints the
//! terminal of whoever ran the command, and a newline forges a line. Both are
//! spelled out instead.
//!
//! Same rule as `flexiq-server`'s `log_safe::escape`, which exists for the same
//! reason one layer down.

/// Render `value` as a single printable fragment.
///
/// No length cap, unlike the server's: what this escapes is read by a person
/// looking for the whole value, and a truncated job id makes the line useless.
pub fn escape(value: &str) -> String {
    if !value.chars().any(char::is_control) {
        return value.to_string();
    }
    value
        .chars()
        .flat_map(|character| {
            if character.is_control() {
                character.escape_debug().collect::<Vec<_>>()
            } else {
                vec![character]
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_clean_value_is_unchanged() {
        assert_eq!(escape("send_email"), "send_email");
        // Non-ASCII is not a control character and must survive intact.
        assert_eq!(escape("café — ok"), "café — ok");
    }

    #[test]
    fn control_characters_are_spelled_out() {
        assert_eq!(escape("two\nlines"), "two\\nlines");
        assert_eq!(escape("evil\u{1b}[2K"), "evil\\u{1b}[2K");
        assert_eq!(escape("tab\there"), "tab\\there");
    }
}
