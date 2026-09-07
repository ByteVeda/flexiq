//! Escaping for values that reach the log straight off a request.

/// How much of an untrusted value is worth keeping in a log line.
const MAX_LOGGED_CHARS: usize = 200;

/// Render `value` as a single printable log fragment.
///
/// A newline inside a logged request parameter lets whoever supplied it append
/// what reads as a second log entry, and an escape byte lets them repaint the
/// terminal of whoever tails the log. Both are spelled out here instead. The
/// length cap keeps one oversized parameter from burying the lines around it.
///
/// Returns an owned `String` unconditionally rather than a `Cow`: every caller
/// is a `log::` macro on a path that has already failed, so the allocation is
/// never on a hot path and the signature stays obvious at the call site.
pub fn escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    let mut truncated = false;

    for (index, character) in value.chars().enumerate() {
        if index == MAX_LOGGED_CHARS {
            truncated = true;
            break;
        }
        match character {
            '\\' => escaped.push_str("\\\\"),
            character if character.is_control() => escaped.extend(character.escape_debug()),
            character => escaped.push(character),
        }
    }

    if truncated {
        escaped.push('…');
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_text_is_left_alone() {
        assert_eq!(escape("acme"), "acme");
        assert_eq!(escape("access_denied"), "access_denied");
        // Non-ASCII is printable, and mangling it would make a real provider's
        // error message unreadable for no gain.
        assert_eq!(escape("accès refusé"), "accès refusé");
    }

    #[test]
    fn a_forged_second_entry_stays_on_one_line() {
        let forged = escape("denied\nWARN  [flexiq] admin login succeeded");

        assert!(!forged.contains('\n'), "must not break the line");
        assert!(forged.contains("\\n"), "must show what was sent");
    }

    #[test]
    fn carriage_returns_and_escape_bytes_are_spelled_out() {
        assert_eq!(escape("a\rb"), "a\\rb");
        assert_eq!(escape("a\tb"), "a\\tb");
        assert_eq!(escape("\u{1b}[2J"), "\\u{1b}[2J");
    }

    #[test]
    fn a_backslash_cannot_forge_an_escape_that_was_never_sent() {
        // Without this arm, a literal `\` followed by `n` would read back as a
        // newline the caller never sent.
        assert_eq!(escape("a\\nb"), "a\\\\nb");
    }

    #[test]
    fn an_oversized_value_is_cut_and_marked() {
        let escaped = escape(&"x".repeat(MAX_LOGGED_CHARS + 50));

        assert_eq!(escaped.chars().count(), MAX_LOGGED_CHARS + 1);
        assert!(escaped.ends_with('…'));
    }

    #[test]
    fn a_value_at_the_cap_is_not_marked() {
        let escaped = escape(&"x".repeat(MAX_LOGGED_CHARS));

        assert_eq!(escaped.chars().count(), MAX_LOGGED_CHARS);
        assert!(!escaped.ends_with('…'));
    }
}
