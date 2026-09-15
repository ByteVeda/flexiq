//! The policy that decides where a dispatch may be sent, independent of when
//! or how the decision gets checked (the resolver in `super::resolver` is the
//! "when").

use std::net::{IpAddr, SocketAddr};

use crate::net::{is_loopback_address, is_never_routable, Allowlist};
use crate::worker::http_target::HttpTargetConfig;

/// Where a dispatch may be sent.
///
/// Two rules, and they are not the same rule. The allowlist is the operator's:
/// a deny-by-default set of names and networks they have named. The
/// unconditional refusals are not theirs to overrule — loopback and link-local
/// reach the host and whatever credential endpoint sits on it, and the cloud
/// metadata literals hand out credentials to anything that asks.
#[derive(Debug, Clone)]
pub struct EgressPolicy {
    allow: Allowlist,
    allow_loopback: bool,
}

/// Why a destination was refused. The message is safe to log and safe to store
/// on a job: it names the host and the address, never a credential.
#[derive(Debug, Clone, thiserror::Error)]
pub enum EgressRefusal {
    /// The address is loopback, link-local, a metadata literal, multicast or
    /// broadcast, and the loopback relaxation does not apply.
    #[error("host '{host}' resolves to {address}, which is never a valid destination")]
    NeverRoutable {
        /// The name that was being resolved.
        host: String,
        /// The refused address it resolved to.
        address: IpAddr,
    },
    /// The address is not named by the operator's allowlist.
    #[error("host '{host}' resolves to {address}, which is not on the allowlist")]
    NotAllowed {
        /// The name that was being resolved.
        host: String,
        /// The refused address it resolved to.
        address: IpAddr,
    },
    /// The host name itself is not named by the operator's allowlist.
    #[error("host '{host}' is not on the allowlist")]
    HostNotAllowed {
        /// The name that was refused.
        host: String,
    },
    /// The name could not be resolved at all, including a resolution that
    /// answered with no addresses.
    #[error("could not resolve '{host}': {reason}")]
    Resolve {
        /// The name that was being resolved.
        host: String,
        /// Why resolution failed, from the underlying lookup.
        reason: String,
    },
}

impl EgressPolicy {
    /// `allow_loopback` relaxes exactly one of the unconditional refusals, and
    /// only for an address the allowlist also names.
    pub fn new(allow: Allowlist, allow_loopback: bool) -> Self {
        Self {
            allow,
            allow_loopback,
        }
    }

    /// Build the policy a target's configuration describes.
    pub fn from_target(config: &HttpTargetConfig) -> Self {
        Self::new(config.allow.clone(), config.allow_loopback)
    }

    /// Whether the name alone is permitted, before anything is resolved.
    ///
    /// An IP-literal host (`"127.0.0.1"`, not a domain name) never reaches a
    /// resolver: the connector recognizes it as an address and dials it
    /// directly (verified against `hyper-util`'s connector, which special-cases
    /// exactly this). The unconditional refusals have no other gate to pass
    /// through for a literal, so they are applied here rather than left to
    /// `Allowlist::permits_host`, which — by design, so it stays one rule
    /// instead of two — knows nothing about them.
    pub fn permits_host(&self, host: &str) -> bool {
        // Mirrors the normalization `Allowlist::permits_host` applies before
        // its own `IpAddr` parse: without it, `"127.0.0.1."` would miss this
        // fast path and fall through to the allowlist-only check below,
        // disagreeing with `permits_address("127.0.0.1")` on the same address.
        let normalized = host.strip_suffix('.').unwrap_or(host);
        if let Ok(address) = normalized.parse::<IpAddr>() {
            return self.permits_address(address);
        }
        self.allow.permits_host(host)
    }

    /// Whether this address may be dialled.
    pub fn permits_address(&self, address: IpAddr) -> bool {
        if self.refused_unconditionally(address) {
            return false;
        }
        self.allow.permits_address(address)
    }

    /// Whether `address` is refused by the unconditional rule — loopback,
    /// link-local, the metadata literals, multicast, broadcast — once the
    /// loopback relaxation is applied.
    ///
    /// The one fact both [`Self::permits_address`] and [`Self::vet`] (via
    /// [`Self::refusal_for`]) read, so a change to the rule cannot update the
    /// boolean without updating which variant `vet` reports for it.
    fn refused_unconditionally(&self, address: IpAddr) -> bool {
        is_never_routable(address) && !(self.allow_loopback && is_loopback_address(address))
    }

    /// Why `address` may not be dialled, or `None` when it may.
    fn refusal_for(&self, host: &str, address: IpAddr) -> Option<EgressRefusal> {
        if self.permits_address(address) {
            return None;
        }
        // Which of the two rules refused it matters to the operator: only
        // one of them is theirs to change.
        Some(if self.refused_unconditionally(address) {
            EgressRefusal::NeverRoutable {
                host: host.to_string(),
                address,
            }
        } else {
            EgressRefusal::NotAllowed {
                host: host.to_string(),
                address,
            }
        })
    }

    /// Vet a resolution: every address, or none.
    ///
    /// Filtering to the permitted addresses would let a rebinding attack in
    /// progress succeed on its second A record, so one refused address
    /// refuses the whole resolution.
    pub fn vet(
        &self,
        host: &str,
        resolved: Vec<SocketAddr>,
    ) -> Result<Vec<SocketAddr>, EgressRefusal> {
        if resolved.is_empty() {
            return Err(EgressRefusal::Resolve {
                host: host.to_string(),
                reason: "no addresses".to_string(),
            });
        }

        for socket in &resolved {
            if let Some(refusal) = self.refusal_for(host, socket.ip()) {
                return Err(refusal);
            }
        }

        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(entries: &str) -> Allowlist {
        Allowlist::parse(entries).expect("test allowlist parses")
    }

    fn addr(literal: &str) -> IpAddr {
        literal.parse().expect("test address parses")
    }

    fn sock(literal: &str) -> SocketAddr {
        format!("{literal}:443")
            .parse()
            .expect("test socket address parses")
    }

    #[test]
    fn an_allowlisted_public_address_is_permitted() {
        let policy = EgressPolicy::new(allow("93.184.216.34"), false);
        assert!(policy.permits_address(addr("93.184.216.34")));
    }

    #[test]
    fn from_target_carries_the_configs_allowlist_and_loopback_setting() {
        let mut config = HttpTargetConfig::new(
            "http://api.example.com/hook",
            1,
            allow("api.example.com,127.0.0.0/8"),
        );
        config.allow_loopback = true;

        let policy = EgressPolicy::from_target(&config);

        assert!(policy.permits_host("api.example.com"));
        assert!(policy.permits_address(addr("127.0.0.1")));
    }

    #[test]
    fn permits_host_matches_a_domain_through_the_allowlist() {
        let policy = EgressPolicy::new(allow("api.example.com"), false);
        assert!(policy.permits_host("api.example.com"));
        assert!(!policy.permits_host("evil.example.com"));
    }

    #[test]
    fn permits_host_applies_the_unconditional_refusals_to_an_ip_literal() {
        // An IP-literal host never reaches the resolver, so `permits_host` is
        // the only gate it passes through — it must agree with
        // `permits_address` on the same address rather than deferring to the
        // raw allowlist, which knows nothing about the unconditional rule.
        let policy = EgressPolicy::new(allow("127.0.0.0/8"), false);
        assert!(!policy.permits_host("127.0.0.1"));

        let relaxed = EgressPolicy::new(allow("127.0.0.0/8"), true);
        assert!(relaxed.permits_host("127.0.0.1"));
    }

    #[test]
    fn permits_host_reads_a_trailing_dot_before_parsing_an_ip_literal() {
        let policy = EgressPolicy::new(allow("127.0.0.0/8"), true);
        assert!(policy.permits_host("127.0.0.1."));
    }

    #[test]
    fn loopback_is_refused_even_when_the_allowlist_names_it() {
        // This is the test the whole design turns on: naming loopback on the
        // allowlist is not, on its own, permission to reach it.
        let policy = EgressPolicy::new(allow("127.0.0.0/8"), false);
        assert!(!policy.permits_address(addr("127.0.0.1")));
    }

    #[test]
    fn loopback_is_permitted_only_when_both_the_knob_and_the_list_agree() {
        // The knob alone, with an allowlist that does not name loopback,
        // still refuses: `allow_loopback` widens what may be reached, it does
        // not replace the list.
        let knob_only = EgressPolicy::new(allow("93.184.216.34"), true);
        assert!(!knob_only.permits_address(addr("127.0.0.1")));

        let both = EgressPolicy::new(allow("127.0.0.0/8"), true);
        assert!(both.permits_address(addr("127.0.0.1")));
    }

    #[test]
    fn the_metadata_literals_are_refused_at_every_setting() {
        let policy =
            EgressPolicy::new(allow("169.254.169.254,fd00:ec2::254,100.100.100.200"), true);
        for literal in ["169.254.169.254", "fd00:ec2::254", "100.100.100.200"] {
            assert!(
                !policy.permits_address(addr(literal)),
                "{literal} must be refused however the policy is configured"
            );
        }
    }

    #[test]
    fn ordinary_unconditional_refusals_are_refused_at_every_setting() {
        // Same rule as the metadata literals above, extended to the rest of
        // `is_never_routable`'s set: ordinary link-local, multicast,
        // broadcast and unspecified are none of them what an operator meant,
        // however the policy is configured.
        let policy =
            EgressPolicy::new(allow("169.254.1.1,224.0.0.1,255.255.255.255,0.0.0.0"), true);
        for literal in ["169.254.1.1", "224.0.0.1", "255.255.255.255", "0.0.0.0"] {
            assert!(
                !policy.permits_address(addr(literal)),
                "{literal} must be refused however the policy is configured"
            );
        }
    }

    #[test]
    fn a_resolution_with_one_bad_address_is_refused_entirely() {
        let policy = EgressPolicy::new(allow("93.184.216.34"), false);
        let resolved = vec![sock("93.184.216.34"), sock("8.8.8.8")];

        let error = policy.vet("example.com", resolved).unwrap_err();

        match error {
            EgressRefusal::NotAllowed { address, .. } => {
                assert_eq!(address, addr("8.8.8.8"));
            }
            other => panic!("expected NotAllowed naming the refused address, got {other:?}"),
        }
    }

    #[test]
    fn a_refusal_says_which_rule_refused_it() {
        let never_routable = EgressPolicy::new(allow("169.254.169.254"), false)
            .vet("metadata.internal", vec![sock("169.254.169.254")])
            .unwrap_err();
        assert!(matches!(
            never_routable,
            EgressRefusal::NeverRoutable { .. }
        ));

        let not_allowed = EgressPolicy::new(allow("93.184.216.34"), false)
            .vet("example.com", vec![sock("8.8.8.8")])
            .unwrap_err();
        assert!(matches!(not_allowed, EgressRefusal::NotAllowed { .. }));
    }

    #[test]
    fn an_empty_resolution_is_a_resolve_error() {
        let policy = EgressPolicy::new(allow("93.184.216.34"), false);
        let error = policy.vet("example.com", Vec::new()).unwrap_err();
        assert!(matches!(error, EgressRefusal::Resolve { .. }));
    }

    #[test]
    fn a_permitted_resolution_returns_every_address_unchanged() {
        let policy = EgressPolicy::new(allow("93.184.216.34,8.8.8.8"), false);
        let resolved = vec![sock("93.184.216.34"), sock("8.8.8.8")];

        let vetted = policy
            .vet("example.com", resolved.clone())
            .expect("both addresses are allowlisted");

        assert_eq!(vetted, resolved);
    }
}
