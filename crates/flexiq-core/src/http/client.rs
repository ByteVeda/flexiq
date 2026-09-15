//! The client every operator-configured push target is dialled through.

use std::sync::Arc;
use std::time::Duration;

use super::egress::EgressPolicy;
use super::resolver::PinnedResolver;
use crate::worker::http_target::HttpTargetError;

/// The client every operator-configured URL is dialled through.
///
/// There is no `Default` and no second constructor: a policy is not optional,
/// and a client that could be built without one is a client someone will
/// build without one.
pub struct DispatchClient {
    client: reqwest::Client,
}

impl DispatchClient {
    /// Build a client that resolves through `policy` and gives up on
    /// connecting after `connect_timeout`.
    pub fn new(
        policy: Arc<EgressPolicy>,
        connect_timeout: Duration,
    ) -> Result<Self, HttpTargetError> {
        let client = reqwest::Client::builder()
            .dns_resolver(Arc::new(PinnedResolver::new(policy)))
            // A 3xx could carry a signed body to a host that never passed the guard.
            .redirect(reqwest::redirect::Policy::none())
            // reqwest retries a protocol NACK twice by default. Those never
            // reached the application, but a second retry loop inside the
            // client is a second retry policy, and this crate already has one.
            .retry(reqwest::retry::never())
            .connect_timeout(connect_timeout)
            // No client-wide timeout: the budget is per request and is
            // derived from the job's own execution timeout, so it cannot live
            // here.
            .build()
            .map_err(|error| HttpTargetError::Client(error.to_string()))?;
        Ok(Self { client })
    }

    /// The underlying client, for the dispatcher that builds requests on it.
    // No caller yet: the dispatcher that builds requests on this client
    // arrives in a later commit. This file's own tests are the only caller
    // until then.
    #[allow(dead_code)]
    pub(crate) fn inner(&self) -> &reqwest::Client {
        &self.client
    }
}

/// Read at most `cap` bytes of a response, reporting whether it was truncated.
///
/// `bytes()` would buffer whatever the endpoint chose to send before any cap
/// applied. Streaming stops as soon as the budget is spent.
// No caller yet: the dispatcher that reads a target's response arrives in a
// later commit. This file's own tests are the only caller until then.
#[allow(dead_code)]
pub(crate) async fn read_bounded(response: reqwest::Response, cap: usize) -> (Vec<u8>, bool) {
    let mut response = response;
    let mut buffered: Vec<u8> = Vec::new();
    let mut truncated = false;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let room = cap.saturating_sub(buffered.len());
                if chunk.len() > room {
                    truncated = true;
                }
                buffered.extend_from_slice(&chunk[..chunk.len().min(room)]);
                if room == 0 {
                    break;
                }
            }
            Ok(None) => break,
            // A read that fails mid-body still leaves whatever arrived useful.
            Err(_) => break,
        }
    }
    (buffered, truncated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::Allowlist;

    #[test]
    fn a_client_builds_from_a_policy() {
        let policy = Arc::new(EgressPolicy::new(
            Allowlist::parse("93.184.216.34").expect("test allowlist parses"),
            false,
        ));

        let client = DispatchClient::new(policy, Duration::from_secs(5));

        assert!(
            client.is_ok(),
            "a policy and a timeout are enough to build a client"
        );
    }
}
