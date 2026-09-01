//! Message-size caps, one declaration per proto package.
//!
//! A payload limit and a message limit are different numbers, and conflating
//! them rejects work the local frame protocol already accepts: tonic's
//! `max_decoding_message_size` measures the *serialized message*, so a maximum
//! payload plus its tag, length prefix and sibling fields is always larger than
//! the payload itself. The producer and executor doors therefore carry
//! different caps, and each is written down exactly once — every service
//! registration reads its number from here rather than restating it.

use flexiq_core::worker::protocol::MAX_PAYLOAD_BYTES;

/// One mebibyte, so the two caps below read as the sizes they are.
const MIB: usize = 1024 * 1024;

/// Envelope headroom over the largest payload a door may carry.
const ENVELOPE_HEADROOM_BYTES: usize = 4 * MIB;

/// Cap on a `flexiq.v1` message. gRPC's own default, and the same number the
/// JSON facade caps a request body at, so both producer doors agree about what
/// "too large" means.
pub const PRODUCER_MAX_MESSAGE_BYTES: usize = 4 * MIB;

/// Cap on a `flexiq.executor.v1` message: the largest payload the worker frame
/// protocol allows, plus room for the message that carries it. Setting this
/// *to* [`MAX_PAYLOAD_BYTES`] would reject a maximum-sized payload and make the
/// gRPC transport refuse work the TCP one accepts.
pub const EXECUTOR_MAX_MESSAGE_BYTES: usize = MAX_PAYLOAD_BYTES + ENVELOPE_HEADROOM_BYTES;

// Compile-time, not a test: the whole point of the headroom is that a message
// limit equal to the payload limit rejects the largest legal payload, and an
// edit that erased it should fail the build rather than one test run.
const _: () = assert!(EXECUTOR_MAX_MESSAGE_BYTES > MAX_PAYLOAD_BYTES);
const _: () = assert!(EXECUTOR_MAX_MESSAGE_BYTES == 68 * MIB);
// gRPC's own default, so the two producer doors agree about "too large".
const _: () = assert!(PRODUCER_MAX_MESSAGE_BYTES == 4 * 1024 * 1024);
