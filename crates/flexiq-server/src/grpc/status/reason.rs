//! The closed list of `ErrorInfo.reason` values, and the metadata keys each one
//! carries.
//!
//! The reason — not the code, and never the message — is the stable identifier
//! a client branches on. Messages are written for humans and may be reworded in
//! any release; these strings may not.
//!
//! It is a Rust module rather than a proto enum because the lint category the
//! contract is held to prefixes every enum value with its enum's name. That
//! would spell `ERROR_REASON_QUEUE_FULL` in the `.proto` while the wire has to
//! read `QUEUE_FULL`, leaving two spellings of one value to be kept in step by
//! hand.

/// The `ErrorInfo.domain` every error this server produces carries.
pub const DOMAIN: &str = "flexiq.byteveda.org";

// ── Reasons ──────────────────────────────────────────────────────────

/// The database or its connection pool did not answer. Retryable with backoff.
pub const STORAGE_UNAVAILABLE: &str = "STORAGE_UNAVAILABLE";
/// A write violated a database constraint. It will violate it again.
pub const STORAGE_CONSTRAINT: &str = "STORAGE_CONSTRAINT";
/// A server-side fault with nothing useful to say to the caller.
pub const INTERNAL: &str = "INTERNAL";
/// No usable credential was presented.
///
/// One reason for every way of failing, and a message that names none of them:
/// telling a missing credential from a wrong one is an oracle for whether a
/// guessed token exists. A client refreshes its credential and retries once.
pub const UNAUTHENTICATED: &str = "UNAUTHENTICATED";
/// The credential is genuine but is not for this package.
///
/// A scope is "may call this package" (D1), so this is the answer to a
/// `produce` credential opening an executor stream. Never retryable: the same
/// credential will be refused again.
pub const SCOPE_DENIED: &str = "SCOPE_DENIED";
/// Bytes the client sent could not be decoded.
pub const MALFORMED_PAYLOAD: &str = "MALFORMED_PAYLOAD";
/// The request itself is not a shape this service accepts — no `body` arm, an
/// unreadable `page_token`, a `Debounce` missing a window.
///
/// This one has no `QueueError` behind it: the request is refused before any
/// storage call, so there is no variant to map. It exists because every error
/// carries a reason, and a request-validation failure is still an error.
pub const INVALID_REQUEST: &str = "INVALID_REQUEST";
/// The path names no RPC this server implements.
///
/// Added for the JSON facade (#718). A gRPC caller reaching a method that does
/// not exist is answered `UNIMPLEMENTED` by the router itself, with no details
/// attached; a JSON caller gets a body, and a body must carry a reason. It is
/// its own reason rather than `INVALID_REQUEST` because the two are different
/// things to a client: one says fix the request, the other says the URL is not
/// a door. Never retryable, and never an indication of what *does* exist.
pub const NO_SUCH_METHOD: &str = "NO_SUCH_METHOD";
/// No such job — or a job in another namespace, which is indistinguishable by
/// design.
pub const JOB_NOT_FOUND: &str = "JOB_NOT_FOUND";
/// A `depends_on` id names nothing this caller may depend on.
pub const DEPENDENCY_NOT_FOUND: &str = "DEPENDENCY_NOT_FOUND";
/// The queue is at its admission cap. Carries `queue`, `pending` and `cap`.
pub const QUEUE_FULL: &str = "QUEUE_FULL";
/// A rate limit rejected the call.
pub const RATE_LIMITED: &str = "RATE_LIMITED";
/// The storage requires a newer build than this process. Carries `speaks` and
/// `required`.
pub const CONTRACT_TOO_OLD: &str = "CONTRACT_TOO_OLD";
/// No executor implements the named task.
pub const TASK_NOT_REGISTERED: &str = "TASK_NOT_REGISTERED";
/// The job exceeded its timeout.
pub const JOB_TIMEOUT: &str = "JOB_TIMEOUT";
/// The execution claim moved to another owner. Never resend.
pub const CLAIM_LOST: &str = "CLAIM_LOST";
/// A durable step replayed differently from the run it is resuming.
pub const STEP_DIVERGED: &str = "STEP_DIVERGED";
/// A step exceeded a size or count limit. Carries `limit`, `actual`, `allowed`.
pub const STEP_LIMIT_EXCEEDED: &str = "STEP_LIMIT_EXCEEDED";
/// A step was refused by something that could see the real error.
pub const STEP_REFUSED: &str = "STEP_REFUSED";
/// The server's own configuration is wrong.
pub const SERVER_MISCONFIGURED: &str = "SERVER_MISCONFIGURED";
/// A lock is held elsewhere. Read again and retry.
pub const LOCK_HELD: &str = "LOCK_HELD";
/// A setting was changed by another writer. Read again and retry.
pub const SETTING_CONFLICT: &str = "SETTING_CONFLICT";
/// Nothing above matched.
pub const UNKNOWN: &str = "UNKNOWN";

// ── Metadata keys ────────────────────────────────────────────────────
//
// Every value is base-10 ASCII: no grouping, no unit suffix, `-` for negative.
// The width and signedness are the Rust field's, not a uniform int64 — a byte
// count that is `u64` in the core must not be narrowed to fit one parser. A
// value that will not parse is a server bug, and a client treats it as absent
// rather than failing the whole response: the code and the reason already
// carry the decision.

/// The queue's name, verbatim. `QUEUE_FULL`.
pub const KEY_QUEUE: &str = "queue";
/// Jobs currently pending, `int64`. `QUEUE_FULL`.
pub const KEY_PENDING: &str = "pending";
/// The admission cap, `int64`. `QUEUE_FULL`.
pub const KEY_CAP: &str = "cap";
/// The contract level this build speaks, `uint32`. `CONTRACT_TOO_OLD`.
pub const KEY_SPEAKS: &str = "speaks";
/// The contract level the storage requires, `uint32`. `CONTRACT_TOO_OLD`.
pub const KEY_REQUIRED: &str = "required";
/// Which limit was exceeded — `step bytes`, `total bytes` or `step count`.
/// `STEP_LIMIT_EXCEEDED`.
pub const KEY_LIMIT: &str = "limit";
/// The measured value, `uint64`, in `limit`'s unit. `STEP_LIMIT_EXCEEDED`.
pub const KEY_ACTUAL: &str = "actual";
/// The permitted value, `uint64`, in `limit`'s unit. `STEP_LIMIT_EXCEEDED`.
pub const KEY_ALLOWED: &str = "allowed";
/// The scope the credential lacks — one of `produce`, `execute`.
/// `SCOPE_DENIED`.
pub const KEY_SCOPE: &str = "scope";
/// 0-based position in an `EnqueueBatch` request, `int32`.
///
/// The one cross-cutting key: it accompanies whatever reason the failing item
/// raised rather than getting a reason of its own, because a client that gets
/// `QUEUE_FULL` on a batch needs both facts at once.
pub const KEY_INDEX: &str = "index";
