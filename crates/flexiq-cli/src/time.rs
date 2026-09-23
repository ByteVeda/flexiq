//! Flag values to `google.protobuf.Timestamp` and `Duration`.
//!
//! One rule for every command: a value the wire type cannot carry is refused by
//! the flag's name, never saturated. The door's `millis_from_timestamp` and
//! `millis_from_duration` *saturate*, so an out-of-range value that got past
//! here would be stored as a plausible-looking wrong one — and `--json` would
//! then omit the field, because it does not render.

use anyhow::{anyhow, Result};
use chrono::DateTime;
use prost_types::{Duration as ProtoDuration, Timestamp};

/// Milliseconds in a second.
const MILLIS_PER_SECOND: i64 = 1_000;

/// Nanoseconds in a millisecond.
const NANOS_PER_MILLI: i32 = 1_000_000;

/// `0001-01-01T00:00:00Z`, the earliest instant a `google.protobuf.Timestamp`
/// may carry, in Unix milliseconds.
pub const MIN_TIMESTAMP_MS: i64 = -62_135_596_800_000;

/// `9999-12-31T23:59:59.999Z`, the latest.
pub const MAX_TIMESTAMP_MS: i64 = 253_402_300_799_999;

/// The widest span a `google.protobuf.Duration` may carry, in milliseconds.
///
/// The type is documented as ±315,576,000,000 seconds — roughly ten thousand
/// years, the same span the timestamp range covers.
pub const MAX_DURATION_MS: i64 = 315_576_000_000_000;

/// `now_ms + offset` as an absolute instant, refusing an offset that does not
/// land on one.
///
/// Two limits, and the wider one is not the interesting one. An unchecked `+`
/// panics under `overflow-checks` and wraps without them, and a wrapped offset
/// lands in the distant past rather than failing. But `i64` is far wider than
/// a `Timestamp`, which runs `0001-01-01` to `9999-12-31`, and nothing
/// downstream catches the gap.
pub fn instant_after(now_ms: i64, offset_ms: Option<i64>, flag: &str) -> Result<Option<Timestamp>> {
    offset_ms
        .map(|offset| {
            now_ms
                .checked_add(offset)
                .and_then(in_timestamp_range)
                .map(timestamp)
                .ok_or_else(|| out_of_range(flag, &offset.to_string()))
        })
        .transpose()
}

/// `now_ms - age` as an absolute instant: "older than this".
///
/// A separate function rather than [`instant_after`] with a negated value,
/// because `-i64::MIN` does not exist and negating first would panic on it.
pub fn instant_before(now_ms: i64, age_ms: Option<i64>, flag: &str) -> Result<Option<Timestamp>> {
    age_ms
        .map(|age| {
            now_ms
                .checked_sub(age)
                .and_then(in_timestamp_range)
                .map(timestamp)
                .ok_or_else(|| out_of_range(flag, &age.to_string()))
        })
        .transpose()
}

/// An RFC 3339 instant, refusing one a `Timestamp` cannot carry.
///
/// RFC 3339 allows year `0000`, one year before a `Timestamp` begins, so a
/// value that parses can still be out of range. Sub-millisecond digits are
/// dropped: the door stores milliseconds.
pub fn instant_at(text: Option<&str>, flag: &str) -> Result<Option<Timestamp>> {
    text.map(|text| {
        let parsed = DateTime::parse_from_rfc3339(text).map_err(|error| {
            anyhow!(
                "`{flag} {text}` is not an RFC 3339 instant ({error}), e.g. 2026-09-01T00:00:00Z"
            )
        })?;
        in_timestamp_range(parsed.timestamp_millis())
            .map(timestamp)
            .ok_or_else(|| out_of_range(flag, text))
    })
    .transpose()
}

/// A span in milliseconds as a `Duration`, refusing one the type cannot carry.
///
/// `i64` milliseconds reach roughly 292 million years, a `Duration` about ten
/// thousand.
pub fn span(millis: Option<i64>, flag: &str) -> Result<Option<ProtoDuration>> {
    millis
        .map(|millis| {
            if millis.checked_abs().is_none_or(|abs| abs > MAX_DURATION_MS) {
                return Err(anyhow!(
                    "`{flag} {millis}` is longer than the ±{MAX_DURATION_MS} milliseconds a \
                     duration can express"
                ));
            }
            Ok(duration(millis))
        })
        .transpose()
}

/// A duration's length in seconds, as a float, for a rate.
pub fn seconds_of(value: &ProtoDuration) -> f64 {
    value.seconds as f64 + f64::from(value.nanos) / 1e9
}

/// `millis`, if a `Timestamp` can carry it.
fn in_timestamp_range(millis: i64) -> Option<i64> {
    (MIN_TIMESTAMP_MS..=MAX_TIMESTAMP_MS)
        .contains(&millis)
        .then_some(millis)
}

/// The refusal for an instant outside the range.
fn out_of_range(flag: &str, value: &str) -> anyhow::Error {
    anyhow!(
        "`{flag} {value}` lands outside 0001-01-01 to 9999-12-31, which is the range an \
         instant can express"
    )
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

/// A span in milliseconds as a `Duration`. Truncating division keeps both
/// halves the same sign, which a `Duration` requires.
fn duration(millis: i64) -> ProtoDuration {
    ProtoDuration {
        seconds: millis / MILLIS_PER_SECOND,
        nanos: (millis % MILLIS_PER_SECOND) as i32 * NANOS_PER_MILLI,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: i64 = 1_757_500_000_000;

    #[test]
    fn an_age_is_subtracted_from_now() {
        let at = instant_before(NOW, Some(60_000), "--older-than-ms")
            .expect("in range")
            .expect("set");
        assert_eq!(at.seconds, 1_757_499_940);
    }

    /// `i64::MIN` cannot be negated, so "now minus it" must be a checked
    /// subtraction, not an addition of its negation.
    #[test]
    fn an_age_that_does_not_fit_is_refused_by_flag_name() {
        for age in [i64::MIN, i64::MAX, NOW - MIN_TIMESTAMP_MS + 1] {
            let error = instant_before(NOW, Some(age), "--older-than-ms").expect_err("out");
            assert!(error.to_string().contains("--older-than-ms"), "{error}");
        }
        assert!(instant_before(NOW, Some(NOW - MIN_TIMESTAMP_MS), "--x").is_ok());
    }

    #[test]
    fn an_rfc3339_instant_becomes_a_timestamp() {
        let at = instant_at(Some("2025-09-10T10:26:40.250Z"), "--before")
            .expect("parses")
            .expect("set");
        assert_eq!(at.seconds, 1_757_500_000);
        assert_eq!(at.nanos, 250_000_000);
        let offset = instant_at(Some("2025-09-10T12:26:40+02:00"), "--before")
            .expect("parses")
            .expect("set");
        assert_eq!(offset.seconds, 1_757_500_000);
    }

    #[test]
    fn a_malformed_instant_is_refused_by_flag_name() {
        let error = instant_at(Some("yesterday"), "--before").expect_err("not RFC 3339");
        assert!(error.to_string().contains("--before"), "{error}");
    }

    /// RFC 3339 parses year `0000`; a `Timestamp` starts at year 1.
    #[test]
    fn a_parseable_instant_outside_the_range_is_refused() {
        let error = instant_at(Some("0000-06-01T00:00:00Z"), "--before").expect_err("year 0");
        assert!(error.to_string().contains("0001-01-01"), "{error}");
    }

    #[test]
    fn a_span_of_i64_min_is_refused_rather_than_overflowing() {
        assert!(span(Some(i64::MIN), "--window-ms").is_err());
    }

    #[test]
    fn a_rate_reads_the_nanos_too() {
        let value = ProtoDuration {
            seconds: 1,
            nanos: 500_000_000,
        };
        assert_eq!(seconds_of(&value), 1.5);
    }
}
