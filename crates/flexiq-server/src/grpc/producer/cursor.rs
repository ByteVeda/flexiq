//! The `page_token` a listing hands back, and reads back.
//!
//! `list_jobs_after` takes a keyset of `(created_at, id)`. That tuple must not
//! be what travels: Redis has no seekable index and applies the keyset in
//! memory over a candidate set, so what a cursor needs to carry is free to
//! change per backend and per release. A tuple on the wire would freeze both.
//!
//! So the token is an opaque string. The encoding here is an implementation
//! detail that a client must not decode, and the one guarantee it makes is that
//! a token this server issued is a token this server can read.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;

use crate::grpc::status::WireError;

/// The keyset a page resumes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    /// The last row's `created_at`, in Unix milliseconds.
    pub created_at: i64,
    /// The last row's id, breaking ties within one millisecond.
    pub id: String,
}

impl Cursor {
    /// The token a client sends back.
    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(format!("{}:{}", self.created_at, self.id))
    }

    /// Read a token this server issued.
    ///
    /// An unreadable one is the client's error and not the server's: it either
    /// came from a different deployment or was edited, and both mean the page
    /// it asks for cannot be produced.
    pub fn decode(token: &str) -> Result<Self, WireError> {
        let refuse = || WireError::invalid_request("page_token is not one this server issued");

        let raw = URL_SAFE_NO_PAD.decode(token).map_err(|_| refuse())?;
        let text = String::from_utf8(raw).map_err(|_| refuse())?;
        // The id may itself contain a colon, so split once from the left: the
        // timestamp is the part that cannot.
        let (created_at, id) = text.split_once(':').ok_or_else(refuse)?;
        let created_at = created_at.parse().map_err(|_| refuse())?;
        if id.is_empty() {
            return Err(refuse());
        }
        Ok(Self {
            created_at,
            id: id.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_token_round_trips() {
        let cursor = Cursor {
            created_at: 1_700_000_000_123,
            id: "0192f3c4-5a6b-7c8d-9e0f-112233445566".to_string(),
        };
        assert_eq!(Cursor::decode(&cursor.encode()).unwrap(), cursor);
    }

    #[test]
    fn an_id_containing_a_colon_survives() {
        let cursor = Cursor {
            created_at: 1,
            id: "a:b:c".to_string(),
        };
        assert_eq!(Cursor::decode(&cursor.encode()).unwrap(), cursor);
    }

    #[test]
    fn a_pre_epoch_cursor_round_trips() {
        let cursor = Cursor {
            created_at: -42,
            id: "x".to_string(),
        };
        assert_eq!(Cursor::decode(&cursor.encode()).unwrap(), cursor);
    }

    #[test]
    fn the_token_reveals_nothing_a_client_should_read() {
        // Not an assertion about secrecy — it is not a secret — but about the
        // shape a client is tempted to parse. Base64 discourages the attempt.
        let token = Cursor {
            created_at: 1,
            id: "abc".to_string(),
        }
        .encode();
        assert!(!token.contains(':'));
    }

    #[test]
    fn a_token_this_server_did_not_issue_is_the_clients_error() {
        for bad in ["", "not base64!!", &URL_SAFE_NO_PAD.encode("no-colon")] {
            let error = Cursor::decode(bad).expect_err("refused");
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
        // Well-formed base64, a colon, and a timestamp that is not a number.
        let error = Cursor::decode(&URL_SAFE_NO_PAD.encode("nan:id")).expect_err("refused");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        // A colon but no id.
        let error = Cursor::decode(&URL_SAFE_NO_PAD.encode("12:")).expect_err("refused");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
