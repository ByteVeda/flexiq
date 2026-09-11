//! Shell tokens to [`crate::pb::StructuredArgs`].
//!
//! `fq` sends the `structured` arm of `EnqueueRequest.body`, so the server
//! encodes the CBOR envelope with the one in-tree encoder and this binary
//! carries no second one. Two costs come with that arm and both are documented
//! next to the flags: a protobuf map decodes into a `BTreeMap` server-side, so
//! keyword arguments are encoded in sorted key order whatever order they were
//! typed in; and `google.protobuf.Value` is a double, so integers cap at the
//! exact-integer range. Either can make an `auto:` idempotency key computed
//! from an `fq` enqueue differ from an SDK's for the same logical call, which
//! is what `--unique-key` is for.
//!
//! What arrives here is text, and JSON is the model that fits it: each token is
//! read as JSON and falls back to a string, so `3` is a number, `hello` is a
//! string, and `'"3"'` is the string `3`.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use prost_types::value::Kind;
use prost_types::{ListValue, Struct, Value};

use crate::pb::StructuredArgs;

/// The largest integer `google.protobuf.Value` holds exactly, and the limit the
/// server enforces on this arm.
const MAX_EXACT_INTEGER: i64 = 9_007_199_254_740_991;

/// Read positional and keyword tokens into the wire's structured arm.
pub fn structured(args: &[String], kwargs: &[String]) -> Result<StructuredArgs> {
    let positional = args
        .iter()
        .map(|token| value(token))
        .collect::<Result<Vec<_>>>()?;
    let mut named = BTreeMap::new();
    for entry in kwargs {
        // The first `=` only: a value may contain more, and a name may not.
        let (name, raw) = entry
            .split_once('=')
            .ok_or_else(|| anyhow!("`--kw {entry}` is not name=value"))?;
        if name.is_empty() {
            return Err(anyhow!("`--kw {entry}` has an empty name"));
        }
        named.insert(name.to_string(), value(raw)?);
    }
    Ok(StructuredArgs {
        args: positional,
        kwargs: named,
    })
}

/// One token: JSON if it parses as JSON, a string otherwise.
pub fn value(token: &str) -> Result<Value> {
    match serde_json::from_str::<serde_json::Value>(token) {
        Ok(parsed) => convert(parsed),
        Err(_) => Ok(string(token)),
    }
}

/// A `serde_json::Value` as a `google.protobuf.Value`.
fn convert(input: serde_json::Value) -> Result<Value> {
    let kind = match input {
        serde_json::Value::Null => Kind::NullValue(0),
        serde_json::Value::Bool(flag) => Kind::BoolValue(flag),
        serde_json::Value::Number(number) => Kind::NumberValue(number_of(&number)?),
        serde_json::Value::String(text) => Kind::StringValue(text),
        serde_json::Value::Array(items) => Kind::ListValue(ListValue {
            values: items.into_iter().map(convert).collect::<Result<Vec<_>>>()?,
        }),
        serde_json::Value::Object(entries) => Kind::StructValue(Struct {
            fields: entries
                .into_iter()
                .map(|(key, item)| convert(item).map(|item| (key, item)))
                .collect::<Result<_>>()?,
        }),
    };
    Ok(Value { kind: Some(kind) })
}

/// A JSON number as a double, refusing what the arm cannot carry.
///
/// The door rejects the same values rather than rounding them, so refusing
/// here names the limit; letting it through would surface as an
/// `INVALID_ARGUMENT` with the number already lost.
///
/// The magnitude test is on the resulting `f64`, not on the integer arm alone.
/// `serde_json` reads `9007199254740993` through `as_i64` but `9007199254740993.0`
/// — the same value with a decimal point — through `as_f64`, where it has
/// already rounded to `9007199254740992.0`. Checking only the integer arm would
/// let the second spelling past a guard that claims to catch it.
fn number_of(number: &serde_json::Number) -> Result<f64> {
    let value = number
        .as_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| anyhow!("{number} is not a finite number, which this wire arm requires"))?;
    exact(value)
}

/// A value the `structured` arm carries without rounding it.
///
/// Mirrors `crates/flexiq-server/src/grpc/producer/structured.rs::convert_number`
/// — a fractional value is a double and passes, an integral one past the exact
/// range is refused rather than silently rounded.
fn exact(value: f64) -> Result<f64> {
    if value.fract() == 0.0 && value.abs() > MAX_EXACT_INTEGER as f64 {
        return Err(anyhow!(
            "{value:.0} is past ±{MAX_EXACT_INTEGER}, the largest integer this wire arm holds \
             exactly. Pass it as a string, or enqueue from an SDK."
        ));
    }
    Ok(value)
}

/// A string value.
fn string(text: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(text.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kind(token: &str) -> Kind {
        value(token).expect("a value").kind.expect("a kind")
    }

    #[test]
    fn a_bare_word_is_a_string_and_a_number_is_a_number() {
        assert!(matches!(kind("hello"), Kind::StringValue(text) if text == "hello"));
        assert!(matches!(kind("3"), Kind::NumberValue(number) if number == 3.0));
        assert!(matches!(kind("true"), Kind::BoolValue(true)));
        assert!(matches!(kind("null"), Kind::NullValue(_)));
        assert!(matches!(kind(r#"{"x":1}"#), Kind::StructValue(_)));
        assert!(matches!(kind("[1,2]"), Kind::ListValue(_)));
    }

    /// An address is the commonest argument there is and it must not need
    /// quoting; `@` is not JSON, so the fallback carries it.
    #[test]
    fn an_email_address_stays_a_string() {
        assert!(matches!(kind("a@b.c"), Kind::StringValue(text) if text == "a@b.c"));
    }

    /// `'"3"'` is the JSON spelling of the string, and it is the only escape
    /// offered — a second mechanism for the same thing is a second thing to
    /// document.
    #[test]
    fn a_quoted_number_is_a_string() {
        assert!(matches!(kind(r#""3""#), Kind::StringValue(text) if text == "3"));
    }

    #[test]
    fn a_kwarg_splits_on_the_first_equals() {
        let out = structured(&[], &["q=a=b".to_string()]).expect("parses");
        assert!(matches!(
            out.kwargs["q"].kind.as_ref().expect("a kind"),
            Kind::StringValue(text) if text == "a=b"
        ));
    }

    #[test]
    fn a_kwarg_without_an_equals_is_refused() {
        let error = structured(&[], &["oops".to_string()]).expect_err("no =");
        assert!(error.to_string().contains("name=value"), "{error}");
    }

    #[test]
    fn a_kwarg_with_an_empty_name_is_refused() {
        assert!(structured(&[], &["=1".to_string()]).is_err());
    }

    /// The door refuses the same values rather than rounding them, so refusing
    /// locally names the limit instead of surfacing an INVALID_ARGUMENT.
    #[test]
    fn an_integer_past_the_exact_range_is_refused_locally() {
        let error = value("9007199254740993").expect_err("too large");
        assert!(error.to_string().contains("9007199254740991"), "{error}");
        let error = value("-9007199254740993").expect_err("too small");
        assert!(error.to_string().contains("9007199254740991"), "{error}");
    }

    /// `serde_json` routes the decimal-point spelling through `as_f64`, where
    /// it has already rounded. A guard on the integer arm alone would pass it.
    #[test]
    fn the_float_spelling_of_a_too_large_integer_is_refused_too() {
        let error = value("9007199254740993.0").expect_err("too large");
        assert!(error.to_string().contains("9007199254740991"), "{error}");
        let error = value("1e300").expect_err("far too large");
        assert!(error.to_string().contains("9007199254740991"), "{error}");
    }

    /// A value with a fraction is a double on the wire and the integer guard
    /// must not catch it. "Has a fraction" is `fract() != 0.0`, not "was
    /// written with a point": past 2⁵³ every `f64` is integral, so `1.5e300`
    /// is an integer as far as this rule is concerned — which is exactly what
    /// the server's `convert_number` decides, and why it refuses that one too.
    #[test]
    fn a_value_with_a_fraction_is_not_subject_to_the_integer_limit() {
        assert!(matches!(kind("0.5"), Kind::NumberValue(number) if number == 0.5));
        assert!(matches!(kind("-2.25"), Kind::NumberValue(number) if number == -2.25));
        assert!(value("1.5e300").is_err());
    }

    #[test]
    fn the_boundary_itself_is_accepted() {
        assert!(matches!(
            kind("9007199254740991"),
            Kind::NumberValue(number) if number == 9_007_199_254_740_991.0
        ));
    }

    #[test]
    fn positional_order_is_preserved() {
        let out = structured(&["1".into(), "2".into()], &[]).expect("parses");
        assert_eq!(out.args.len(), 2);
        assert!(matches!(out.args[0].kind, Some(Kind::NumberValue(n)) if n == 1.0));
        assert!(matches!(out.args[1].kind, Some(Kind::NumberValue(n)) if n == 2.0));
    }

    /// Nesting goes through the same conversion, so a limit inside an object is
    /// still a limit.
    #[test]
    fn a_nested_integer_is_checked_too() {
        assert!(value(r#"{"n":9007199254740993}"#).is_err());
    }
}
