//! The seam: one question asked of request metadata, answered by whatever
//! credential scheme the deployment configured.
//!
//! The question is asked of a [`MetadataMap`] and of nothing else. That is the
//! whole reason the wire carries no namespace field (design doc D10): a check
//! that reads a decoded request message is a check written once per RPC, and
//! the first RPC that forgets it is a cross-tenant read. Metadata is available
//! before the router has chosen a handler, so the check happens in one place
//! for every RPC there will ever be.
//!
//! **It is `async` because the answer is in storage.** A token is a row, and
//! every `Storage` call is blocking, so an authenticator runs its lookup on the
//! blocking pool and the layer awaits it. A trait behind `dyn` cannot carry a
//! plain `async fn`, hence the boxed future `#[async_trait]` writes.

use tonic::metadata::MetadataMap;
use tonic::Status;

use super::principal::Principal;

/// Turns a request's metadata into the caller it belongs to.
///
/// Implementations answer only "who is this"; whether that caller may reach the
/// path it asked for is [`super::gate`]'s question, so a new credential scheme
/// cannot accidentally redefine what a scope means.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Identify the caller behind `metadata`, or refuse the request.
    ///
    /// The refusal must not distinguish a missing credential from a wrong one:
    /// telling them apart is an oracle for whether a guessed token exists. The
    /// one refusal that may differ is a failure to *reach* the credential —
    /// a database that is down is not a caller that is wrong, and answering
    /// `UNAUTHENTICATED` to it would send a client to rotate a working token.
    async fn authenticate(&self, metadata: &MetadataMap) -> Result<Principal, Status>;
}
