//! Reading a duration written the way a person writes one.

use syn::{Error, Lit, Result};

/// Milliseconds from `"30s"`, `"500ms"`, `"5m"`, `"2h"`, `"1d"`, or a bare
/// integer already in milliseconds.
///
/// A bare integer is accepted because every duration core stores is named
/// `*_ms`, so a caller who has one in hand should not have to stringify it.
pub fn millis(lit: &Lit) -> Result<i64> {
    match lit {
        Lit::Int(value) => value.base10_parse::<i64>(),
        Lit::Str(value) => parse_text(&value.value())
            .ok_or_else(|| Error::new(lit.span(), UNPARSEABLE.replace("{}", &value.value()))),
        other => Err(Error::new(other.span(), UNPARSEABLE.replace("{}", "that"))),
    }
}

/// The one message a bad duration produces, so a caller sees the accepted
/// spellings rather than a parser's complaint.
const UNPARSEABLE: &str =
    "`{}` is not a duration. Write it as 500ms, 30s, 5m, 2h, 1d, or a bare integer of milliseconds";

/// `<number><unit>` into milliseconds.
fn parse_text(text: &str) -> Option<i64> {
    let text = text.trim();
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (count, unit) = text.split_at(split);
    let count: i64 = count.parse().ok()?;

    // `ms` before `m`: a prefix match the other way round reads "5ms" as five
    // minutes, which is a 60,000× error that still runs.
    let scale = match unit.trim() {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    count.checked_mul(scale)
}

#[cfg(test)]
mod tests {
    use super::parse_text;

    #[test]
    fn every_accepted_unit() {
        assert_eq!(parse_text("500ms"), Some(500));
        assert_eq!(parse_text("30s"), Some(30_000));
        assert_eq!(parse_text("5m"), Some(300_000));
        assert_eq!(parse_text("2h"), Some(7_200_000));
        assert_eq!(parse_text("1d"), Some(86_400_000));
    }

    /// The one that would be a 60,000× error rather than a failure.
    #[test]
    fn ms_is_not_read_as_minutes() {
        assert_eq!(parse_text("5ms"), Some(5));
        assert_eq!(parse_text("5m"), Some(300_000));
    }

    #[test]
    fn nonsense_is_refused() {
        assert_eq!(parse_text("30 fortnights"), None);
        assert_eq!(parse_text("s"), None);
        assert_eq!(parse_text(""), None);
        assert_eq!(parse_text("30"), None);
    }

    #[test]
    fn an_overflowing_count_is_refused_rather_than_wrapped() {
        assert_eq!(parse_text("9223372036854775807d"), None);
    }
}
