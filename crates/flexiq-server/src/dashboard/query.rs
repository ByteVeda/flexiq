//! Query-string parsing with the SDK dashboards' semantics.
//!
//! Repeated keys keep the **first** value (`?limit=1&limit=2` → 1), which is
//! what `urllib.parse.parse_qs` plus `[0]` indexing does on the Python side —
//! axum's `Query<HashMap<..>>` would keep the last.

use std::collections::HashMap;

use axum::extract::{FromRequestParts, OptionalFromRequestParts};
use axum::http::request::Parts;
use std::convert::Infallible;

use crate::dashboard::error::{ApiError, ApiResult};

/// Parsed query parameters.
#[derive(Debug, Default, Clone)]
pub struct Params(HashMap<String, String>);

impl Params {
    /// Parse a raw query string (no leading `?`).
    pub fn parse(raw: &str) -> Self {
        let mut values: HashMap<String, String> = HashMap::new();
        for (key, value) in
            serde_urlencoded::from_str::<Vec<(String, String)>>(raw).unwrap_or_default()
        {
            values.entry(key).or_insert(value);
        }
        Self(values)
    }

    /// A parameter's value, treating an empty value as absent so `?queue=`
    /// means "no filter" rather than "the queue named empty string".
    pub fn get(&self, key: &str) -> Option<&str> {
        self.0
            .get(key)
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }

    /// A non-negative integer parameter, or `default` when absent.
    pub fn int(&self, key: &str, default: i64) -> ApiResult<i64> {
        let Some(raw) = self.get(key) else {
            return Ok(default);
        };
        let parsed: i64 = raw
            .parse()
            .map_err(|_| ApiError::BadRequest(format!("{key} must be an integer")))?;
        if parsed < 0 {
            return Err(ApiError::BadRequest(format!("{key} must be non-negative")));
        }
        Ok(parsed)
    }
}

impl<S: Sync> FromRequestParts<S> for Params {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self::parse(parts.uri.query().unwrap_or_default()))
    }
}

impl<S: Sync> OptionalFromRequestParts<S> for Params {
    type Rejection = Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> Result<Option<Self>, Self::Rejection> {
        <Self as FromRequestParts<S>>::from_request_parts(parts, state)
            .await
            .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_are_percent_and_plus_decoded() {
        let params = Params::parse("task=send+mail&queue=high%20priority");
        assert_eq!(params.get("task"), Some("send mail"));
        assert_eq!(params.get("queue"), Some("high priority"));
    }

    #[test]
    fn a_repeated_key_keeps_the_first_value() {
        let params = Params::parse("limit=1&limit=2");
        assert_eq!(params.int("limit", 20).expect("parses"), 1);
    }

    #[test]
    fn an_empty_value_reads_as_absent() {
        let params = Params::parse("queue=&status=running");
        assert_eq!(params.get("queue"), None);
        assert_eq!(params.get("status"), Some("running"));
    }

    #[test]
    fn integers_are_validated() {
        let params = Params::parse("limit=abc&offset=-1");
        assert!(matches!(
            params.int("limit", 20),
            Err(ApiError::BadRequest(_))
        ));
        assert!(matches!(
            params.int("offset", 0),
            Err(ApiError::BadRequest(_))
        ));
        assert_eq!(params.int("missing", 7).expect("default"), 7);
    }
}
