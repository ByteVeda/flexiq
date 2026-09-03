//! One error model, rendered for the door the request arrived at.
//!
//! A gRPC caller gets the `google.rpc.Status` in trailers, as tonic writes it.
//! A JSON caller gets the same value as a body:
//!
//! ```json
//! {"error": {
//!   "code": 429,
//!   "status": "RESOURCE_EXHAUSTED",
//!   "message": "queue `emails` is full",
//!   "details": [{"@type": "type.googleapis.com/google.rpc.ErrorInfo",
//!                "reason": "QUEUE_FULL", "domain": "flexiq.byteveda.org",
//!                "metadata": {"queue": "emails", "pending": "11", "cap": "10"}}]
//! }}
//! ```
//!
//! Nothing is invented here. `code` is the HTTP status, `status` names the
//! `google.rpc.Code`, and `details` carries the same `ErrorInfo` — whose
//! `reason` is the stable identifier a client branches on — that the gRPC
//! trailer carries. The message is for humans and may be reworded in any
//! release.
//!
//! **The HTTP status is a pure function of the `google.rpc.Code`**, with no
//! per-case exceptions. That is why a body over the size cap is 400 and not
//! 413: the cap is `OUT_OF_RANGE` on both doors, which is the code tonic
//! answers for a message over `max_decoding_message_size`, and one mapping with
//! an exception in it is two mappings. A client that wants the finer answer
//! reads `status` and `reason`, which is what they are for.

use axum::response::Response;
use http::{header, HeaderMap, HeaderValue, StatusCode};
use serde_json::{json, Map, Value};
use tonic::{Code, Status};
use tonic_types::{ErrorInfo, RetryInfo, RpcStatusExt as _};

use super::json::wkt::duration_to_json;
use crate::grpc::status::WireError;

/// `499 Client Closed Request` — nginx's, and what every gRPC gateway answers
/// to `CANCELLED`. Not in `http`'s table, because it is not in an RFC.
const CLIENT_CLOSED_REQUEST: u16 = 499;

/// The media type a facade response carries.
const JSON: HeaderValue = HeaderValue::from_static("application/json");

/// Job state changes continuously, so a cached read is a wrong answer with a
/// fast response time. `NO_SIDE_EFFECTS` is what makes a `GET` legal; it is not
/// a cacheability claim.
const NO_STORE: HeaderValue = HeaderValue::from_static("no-store");

/// The HTTP status for one `google.rpc.Code`.
pub fn http_status(code: Code) -> StatusCode {
    match code {
        Code::Ok => StatusCode::OK,
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            StatusCode::BAD_REQUEST
        }
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::AlreadyExists | Code::Aborted => StatusCode::CONFLICT,
        Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        Code::Cancelled => {
            StatusCode::from_u16(CLIENT_CLOSED_REQUEST).unwrap_or(StatusCode::BAD_REQUEST)
        }
        Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        Code::Unknown | Code::Internal | Code::DataLoss => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// One `google.rpc.Status` as the object a JSON client reads.
///
/// Used in two positions: wrapped by [`body`] as a failed response, and
/// unwrapped as the `error` arm of an `EnqueueBatchItemResult`. One shape in
/// both, so a client parses a batch item's failure with the code it already
/// wrote for an RPC's — and a nested item's `code` is the HTTP status that
/// failure would have carried had it arrived on its own.
pub fn status_json(status: &tonic_types::pb::Status) -> Value {
    let code = Code::from_i32(status.code);
    let mut object = Map::new();
    object.insert("code".to_string(), http_status(code).as_u16().into());
    object.insert("status".to_string(), code_name(code).into());
    object.insert("message".to_string(), status.message.clone().into());
    object.insert("details".to_string(), Value::Array(details(status)));
    Value::Object(object)
}

/// The body of a failed response.
pub fn body(status: &tonic_types::pb::Status) -> Value {
    json!({ "error": status_json(status) })
}

/// A failed response, rendered for a JSON client.
pub fn response(status: &Status) -> Response {
    let rendered = body(&rich(status));
    let code = http_status(status.code());
    let bytes = serde_json::to_vec(&rendered).unwrap_or_else(|error| {
        // Serialising a map of strings cannot fail; if it somehow did, an empty
        // body with the right status beats a panic inside the auth layer.
        log::error!("grpc: a facade error body would not serialise: {error}");
        Vec::new()
    });

    let mut response = Response::new(axum::body::Body::from(bytes));
    *response.status_mut() = code;
    set_headers(response.headers_mut());
    response
}

/// A refusal this door raised itself, rendered for a JSON client.
pub fn refuse(error: WireError) -> Response {
    response(&Status::from(error))
}

/// A successful response body.
pub fn ok(value: &Value) -> Response {
    match serde_json::to_vec(value) {
        Ok(bytes) => {
            let mut response = Response::new(axum::body::Body::from(bytes));
            set_headers(response.headers_mut());
            response
        }
        Err(error) => {
            log::error!("grpc: a facade response body would not serialise: {error}");
            refuse(WireError::internal())
        }
    }
}

fn set_headers(headers: &mut HeaderMap) {
    headers.insert(header::CONTENT_TYPE, JSON);
    headers.insert(header::CACHE_CONTROL, NO_STORE);
}

/// The `google.rpc.Status` behind a `tonic::Status`.
///
/// tonic carries it as the encoded detail bytes, which is where the `ErrorInfo`
/// lives; a status built without details still has a code and a message, and
/// those alone are a valid — if less useful — answer.
fn rich(status: &Status) -> tonic_types::pb::Status {
    use prost::Message as _;

    tonic_types::pb::Status::decode(status.details()).unwrap_or(tonic_types::pb::Status {
        code: status.code() as i32,
        message: status.message().to_string(),
        details: Vec::new(),
    })
}

/// The two detail messages this error model produces, in the `@type` form the
/// JSON mapping of `google.protobuf.Any` uses.
fn details(status: &tonic_types::pb::Status) -> Vec<Value> {
    let mut details = Vec::new();
    if let Some(info) = status.get_details_error_info() {
        details.push(json!({
            "@type": ErrorInfo::TYPE_URL,
            "reason": info.reason,
            "domain": info.domain,
            "metadata": info.metadata,
        }));
    }
    if let Some(delay) = status
        .get_details_retry_info()
        .and_then(|info| info.retry_delay)
    {
        let delay = prost_types::Duration {
            seconds: i64::try_from(delay.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(delay.subsec_nanos()).unwrap_or_default(),
        };
        details.push(json!({
            "@type": RetryInfo::TYPE_URL,
            "retryDelay": duration_to_json(&delay),
        }));
    }
    details
}

/// The `google.rpc.Code` enumerator's own name.
///
/// Spelled out rather than derived from `Debug`, because it is a value clients
/// branch on: `Code::InvalidArgument` debug-prints as `InvalidArgument`, and
/// the contract's name for it is `INVALID_ARGUMENT`.
fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::status::reason;
    use flexiq_core::error::QueueError;

    fn rendered(error: WireError) -> Value {
        body(&rich(&Status::from(error)))
    }

    #[test]
    fn a_full_queue_carries_its_numbers_and_a_retry_hint() {
        let value = rendered(WireError::from_queue_error(&QueueError::QueueFull {
            queue: "emails".to_string(),
            pending: 11,
            cap: 10,
        }));
        let error = &value["error"];
        assert_eq!(error["code"], Value::from(429));
        assert_eq!(error["status"], Value::from("RESOURCE_EXHAUSTED"));
        assert_eq!(
            error["details"][0]["@type"],
            Value::from(ErrorInfo::TYPE_URL)
        );
        assert_eq!(
            error["details"][0]["reason"],
            Value::from(reason::QUEUE_FULL)
        );
        assert_eq!(error["details"][0]["domain"], Value::from(reason::DOMAIN));
        assert_eq!(error["details"][0]["metadata"]["cap"], Value::from("10"));
        assert_eq!(
            error["details"][1]["@type"],
            Value::from(RetryInfo::TYPE_URL)
        );
        assert_eq!(error["details"][1]["retryDelay"], Value::from("1s"));
    }

    /// The half of the trade that makes sanitising acceptable is that the
    /// operator's log keeps the cause. What the client gets must not.
    #[test]
    fn storage_detail_never_reaches_a_json_client_either() {
        let value = rendered(WireError::from_queue_error(&QueueError::Storage(
            flexiq_core::diesel::result::Error::DatabaseError(
                flexiq_core::diesel::result::DatabaseErrorKind::ClosedConnection,
                Box::new("host=db.internal user=flexiq".to_string()),
            ),
        )));
        assert_eq!(value["error"]["code"], Value::from(503));
        assert!(!value.to_string().contains("db.internal"));
    }

    #[test]
    fn every_code_maps_to_one_http_status_and_one_name() {
        for code in [
            Code::Ok,
            Code::Cancelled,
            Code::Unknown,
            Code::InvalidArgument,
            Code::DeadlineExceeded,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::ResourceExhausted,
            Code::FailedPrecondition,
            Code::Aborted,
            Code::OutOfRange,
            Code::Unimplemented,
            Code::Internal,
            Code::Unavailable,
            Code::DataLoss,
            Code::Unauthenticated,
        ] {
            let status = http_status(code);
            assert!(
                status.as_u16() >= 200,
                "{code:?} produced a nonsense status"
            );
            assert!(
                code_name(code)
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b == b'_'),
                "{code:?} has a name that is not the contract's spelling"
            );
        }
        assert_eq!(http_status(Code::Unauthenticated), StatusCode::UNAUTHORIZED);
        assert_eq!(http_status(Code::PermissionDenied), StatusCode::FORBIDDEN);
        assert_eq!(http_status(Code::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(
            http_status(Code::Unimplemented),
            StatusCode::NOT_IMPLEMENTED
        );
    }

    /// The cap is one number and one code on both doors; the HTTP status falls
    /// out of the code rather than being chosen for the occasion.
    #[test]
    fn a_body_over_the_cap_answers_the_same_code_grpc_does() {
        let error = WireError::payload_too_large(crate::grpc::limits::PRODUCER_MAX_MESSAGE_BYTES);
        assert_eq!(error.code(), Code::OutOfRange);
        assert_eq!(error.reason(), reason::INVALID_REQUEST);
        assert_eq!(http_status(error.code()), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn a_response_says_json_and_says_not_to_cache_it() {
        let response = refuse(WireError::internal());
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    }
}
