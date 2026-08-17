//! Opaque keyset-pagination cursor shared by every SDK binding.
//!
//! A cursor encodes the `(sort_key, id)` of the last row of a page — the sort
//! key being the listing's ordering column (`created_at`/`completed_at`/
//! `failed_at`, a non-negative millis timestamp) and `id` the row's UUIDv7 /
//! DLQ id. Neither contains a `:`, so a single split round-trips unambiguously
//! with no base64. Callers must treat the string as opaque and pass it back
//! verbatim; the format may gain a version prefix later.

use crate::error::{QueueError, Result};

/// Encode a `(sort_key, id)` keyset cursor as an opaque string.
pub fn encode_cursor(sort_key: i64, id: &str) -> String {
    format!("{sort_key}:{id}")
}

/// Decode a cursor produced by [`encode_cursor`]. Returns `Other` on malformed
/// input — treat it like any other bad-request error.
pub fn decode_cursor(cursor: &str) -> Result<(i64, &str)> {
    let (key, id) = cursor
        .split_once(':')
        .ok_or_else(|| QueueError::Other(format!("invalid cursor: {cursor}")))?;
    let sort_key: i64 = key
        .parse()
        .map_err(|_| QueueError::Other(format!("invalid cursor: {cursor}")))?;
    // A cursor this crate did not encode: sort keys are non-negative millis and
    // every row has an id. Reject rather than page from a position no row holds.
    if sort_key < 0 || id.is_empty() {
        return Err(QueueError::Other(format!("invalid cursor: {cursor}")));
    }
    Ok((sort_key, id))
}

/// Build the next-page cursor from a returned page, or `None` when the page is
/// the last one — a short page means the listing is exhausted, so callers can
/// loop `while let Some(cursor)`. `key` reads the row's `(sort_key, id)`.
///
/// Lives here rather than in a binding: every SDK pages the same way, and the
/// "fewer rows than limit ⇒ last page" rule is part of the cursor contract, not
/// of any one shell.
pub fn next_cursor<T>(rows: &[T], limit: i64, key: impl Fn(&T) -> (i64, &str)) -> Option<String> {
    if (rows.len() as i64) < limit {
        return None;
    }
    rows.last().map(|row| {
        let (sort_key, id) = key(row);
        encode_cursor(sort_key, id)
    })
}

/// One page of a keyset-paginated listing: the rows plus the cursor for the
/// next page (`None` when this page is the last).
#[derive(Debug, Clone)]
pub struct Page<T> {
    /// Rows in this page.
    pub items: Vec<T>,
    /// Opaque cursor for the next page; `None` when this page is the last.
    pub next_cursor: Option<String>,
}

/// Wrap a `*_after` listing result as a [`Page`], deriving the next-page
/// cursor via [`next_cursor`]. `key` reads a row's `(sort_key, id)`.
pub fn page<T>(items: Vec<T>, limit: i64, key: impl Fn(&T) -> (i64, &str)) -> Page<T> {
    let next = next_cursor(&items, limit, key);
    Page {
        items,
        next_cursor: next,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `(sort_key, id)` rows standing in for a listing's page.
    fn rows(n: usize) -> Vec<(i64, String)> {
        (0..n).map(|i| (i as i64, format!("id-{i}"))).collect()
    }

    #[test]
    fn next_cursor_is_none_on_a_short_page() {
        assert!(next_cursor(&rows(2), 5, |r| (r.0, &r.1)).is_none());
    }

    #[test]
    fn next_cursor_points_at_the_last_row_of_a_full_page() {
        let cursor = next_cursor(&rows(3), 3, |r| (r.0, &r.1)).expect("full page has a cursor");
        assert_eq!(cursor, encode_cursor(2, "id-2"));
    }

    #[test]
    fn next_cursor_is_none_on_an_empty_page() {
        assert!(next_cursor::<(i64, String)>(&[], 5, |r| (r.0, &r.1)).is_none());
    }

    #[test]
    fn round_trips() {
        let s = encode_cursor(1_720_000_000_123, "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b");
        let (key, id) = decode_cursor(&s).unwrap();
        assert_eq!(key, 1_720_000_000_123);
        assert_eq!(id, "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b");
    }

    #[test]
    fn rejects_malformed() {
        assert!(decode_cursor("nocolon").is_err());
        assert!(decode_cursor("notanint:abc").is_err());
        assert!(decode_cursor("-1:abc").is_err());
        assert!(decode_cursor("1:").is_err());
    }
}
