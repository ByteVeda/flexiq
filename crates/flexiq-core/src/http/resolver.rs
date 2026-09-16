//! The `reqwest::dns::Resolve` implementation every dispatch client resolves
//! through.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

use super::egress::{EgressPolicy, EgressRefusal};

/// The resolver every dispatch client resolves through, for a *name*.
///
/// Vetting inside resolution is what closes the rebinding window for a name:
/// the addresses the connector receives for it are the addresses that just
/// passed, so there is no second lookup between the check and the socket.
/// This has nothing to say about an IP-literal host — the connector dials
/// that directly without ever calling a `Resolve` impl — which is why
/// [`EgressPolicy::permits_host`] applies the unconditional refusals itself
/// rather than leaving every case to this resolver.
///
/// **A lookup in flight can outlast a shutdown drain.** `getaddrinfo` runs on
/// the blocking pool, and a blocking task cannot be cancelled once it has
/// started; the worker's runtime is dropped without a shutdown timeout, so
/// dropping it waits for that task. A wedged lookup therefore holds shutdown
/// open past `HttpTargetConfig::shutdown_drain`. It is still bounded, just
/// not here: the platform resolver's own configuration is the ceiling —
/// `resolv.conf`'s `timeout` × `attempts`, across each nameserver listed. A
/// timeout on the resolution future would buy nothing, since the blocking
/// task keeps running behind it; removing the bound needs either an async
/// resolver or a bounded runtime shutdown, and neither is this path's alone
/// to change.
pub(crate) struct PinnedResolver {
    policy: Arc<EgressPolicy>,
}

impl PinnedResolver {
    /// Resolve through `policy`, which every lookup is vetted against.
    pub(crate) fn new(policy: Arc<EgressPolicy>) -> Self {
        Self { policy }
    }
}

impl Resolve for PinnedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let policy = Arc::clone(&self.policy);
        let host = name.as_str().to_string();
        Box::pin(async move {
            // `getaddrinfo` blocks the calling thread; it must not run on an
            // async worker. Port 0 is correct — reqwest overrides it with the
            // URL's port or the scheme default once resolution returns.
            let lookup_host = host.clone();
            let lookup =
                tokio::task::spawn_blocking(move || (lookup_host.as_str(), 0u16).to_socket_addrs())
                    .await
                    .map_err(|join_error| resolve_failure(&host, join_error.to_string()))?;

            let resolved: Vec<SocketAddr> = match lookup {
                Ok(addrs) => addrs.collect(),
                Err(io_error) => return Err(resolve_failure(&host, io_error.to_string())),
            };

            let vetted = policy.vet(&host, resolved).map_err(refusal)?;
            Ok(Box::new(vetted.into_iter()) as Addrs)
        })
    }
}

/// A lookup that never reached the policy: the resolver itself failed, or the
/// blocking task never ran to completion. Distinct from [`refusal`] — this
/// means nobody tried anything, the network or the runtime just did not
/// answer.
fn resolve_failure(host: &str, reason: String) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(EgressRefusal::Resolve {
        host: host.to_string(),
        reason,
    })
}

/// A lookup the policy itself refused.
fn refusal(refusal: EgressRefusal) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(refusal)
}

#[cfg(test)]
mod tests {
    use crate::net::Allowlist;

    use super::*;

    fn policy(entries: &str, allow_loopback: bool) -> Arc<EgressPolicy> {
        Arc::new(EgressPolicy::new(
            Allowlist::parse(entries).expect("test allowlist parses"),
            allow_loopback,
        ))
    }

    /// `Addrs` (`Box<dyn Iterator<..> + Send>`) implements neither `Debug`
    /// nor `Clone`, so `unwrap_err`/`expect_err` cannot be called on the
    /// `Result` `resolve` returns — this unwraps by hand instead.
    fn expect_refusal(
        result: Result<Addrs, Box<dyn std::error::Error + Send + Sync>>,
    ) -> EgressRefusal {
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("expected the lookup to be refused"),
        };
        *error
            .downcast::<EgressRefusal>()
            .expect("a refused lookup carries an EgressRefusal")
    }

    #[tokio::test]
    async fn localhost_is_refused_by_a_policy_that_does_not_allow_it() {
        // A public network that has nothing to do with loopback: no network
        // access needed, `localhost` resolves locally without touching DNS.
        let resolver = PinnedResolver::new(policy("93.184.216.0/24", false));
        let name = "localhost"
            .parse::<Name>()
            .expect("localhost is a valid DNS name");

        let refusal = expect_refusal(resolver.resolve(name).await);

        // `allow_loopback` is false, so loopback is refused unconditionally
        // and deterministically — asserting the specific variant (rather
        // than also accepting `NotAllowed`) is itself a regression test for
        // the taxonomy `EgressPolicy::refusal_for` computes.
        assert!(matches!(refusal, EgressRefusal::NeverRoutable { .. }));
    }

    #[tokio::test]
    async fn localhost_resolves_when_the_policy_permits_it() {
        let resolver = PinnedResolver::new(policy("127.0.0.0/8,::1/128", true));
        let name = "localhost"
            .parse::<Name>()
            .expect("localhost is a valid DNS name");

        let addrs: Vec<SocketAddr> = resolver
            .resolve(name)
            .await
            .expect("a policy naming loopback and allowing it must resolve localhost")
            .collect();

        assert!(!addrs.is_empty());
        for addr in addrs {
            assert!(
                addr.ip().is_loopback(),
                "{addr} must be loopback, localhost resolves to nothing else"
            );
        }
    }

    #[tokio::test]
    async fn a_name_that_does_not_resolve_reports_a_resolve_error() {
        // `.invalid` is reserved by RFC 6761 as guaranteed not to resolve —
        // this still sends a real query (this is not the loopback-only
        // shortcut the other two tests get), but the assertion holds however
        // it fails: NXDOMAIN, a timeout, or any other lookup error all reach
        // this same `EgressRefusal::Resolve` arm. Do not "fix" a slow run in
        // a network-isolated sandbox by weakening this to a lighter check —
        // a slow failure here is still a correct one.
        let resolver = PinnedResolver::new(policy("93.184.216.0/24", false));
        let name = "nothing.invalid"
            .parse::<Name>()
            .expect("a valid DNS name syntactically");

        let refusal = expect_refusal(resolver.resolve(name).await);

        assert!(matches!(refusal, EgressRefusal::Resolve { .. }));
    }
}
