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

    /// Whether the loopback relaxation is in force.
    ///
    /// Read by `worker::http_target`'s URL validation, which refuses a
    /// cleartext `http` target unless this is set *and* the host is loopback
    /// — the same relaxation this type applies to destinations, applied to
    /// transport. An accessor rather than a second copy of the flag on the
    /// caller's side, so the two answers cannot drift apart.
    pub fn allows_loopback(&self) -> bool {
        self.allow_loopback
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

    /// Whether this address may be dialled on its own account — not refused
    /// unconditionally, and covered by a CIDR rule.
    ///
    /// Strictly narrower than what [`Self::vet`] permits, because this has no
    /// host to consider: a name rule cannot answer for a bare address. The
    /// caller that has a name uses `vet`; the one that has only an address —
    /// [`Self::permits_host`]'s IP-literal path, where there is no name to
    /// have matched a rule — uses this.
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

    /// Whether a *name* rule on the allowlist names `host`.
    ///
    /// An IP-literal host is deliberately not a name here, even though
    /// [`Allowlist::permits_host`] answers for one: it folds a literal to
    /// [`Allowlist::permits_address`], and reading that answer as "the name is
    /// allowlisted" would let a single allowlisted literal vouch for every
    /// other address a resolution returned alongside it. A literal never
    /// reaches [`Self::vet`] — the connector dials it without calling a
    /// resolver — so this refuses to depend on that staying true.
    fn host_is_named(&self, host: &str) -> bool {
        let normalized = host.strip_suffix('.').unwrap_or(host);
        if normalized.parse::<IpAddr>().is_ok() {
            return false;
        }
        self.allow.permits_host(host)
    }

    /// Why `address` may not be dialled, or `None` when it may.
    ///
    /// `host_is_named` is [`Self::host_is_named`]'s answer for the host this
    /// resolution belongs to, hoisted out by [`Self::vet`] so one name lookup
    /// covers every address rather than one per address.
    fn refusal_for(
        &self,
        host: &str,
        host_is_named: bool,
        address: IpAddr,
    ) -> Option<EgressRefusal> {
        if !self.refused_unconditionally(address)
            && (host_is_named || self.allow.permits_address(address))
        {
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
    ///
    /// # Which rule may permit an address
    ///
    /// An address passes when it is not refused unconditionally **and**
    /// either a CIDR rule covers it or a name rule already named the host it
    /// came from. Both halves of that `or` are needed, and only the first was
    /// once checked here — a name-only allowlist (`api.example.com`) matched
    /// no address at all, so hostname allowlisting refused every dispatch.
    ///
    /// **The trade-off, stated rather than left to be worked out:** a name
    /// rule trusts DNS for that name. Whatever `api.example.com` resolves to
    /// is where the dispatch goes, so whoever controls that record controls
    /// the destination — bounded by the unconditional refusals, which a name
    /// rule cannot vouch past: loopback, link-local, the metadata literals,
    /// multicast and broadcast stay refused however the name is written. That
    /// bound is what makes a name rule sufficient on its own. An operator who
    /// wants the address space constrained too adds a CIDR beside the name,
    /// and the name then buys nothing an address outside that CIDR can use.
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

        let host_is_named = self.host_is_named(host);
        for socket in &resolved {
            if let Some(refusal) = self.refusal_for(host, host_is_named, socket.ip()) {
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

    /// Built from the parsed `IpAddr` rather than by formatting `"{literal}:443"`:
    /// the string form needs an IPv6 literal bracketed, and this takes both
    /// families without the caller having to remember which.
    fn sock(literal: &str) -> SocketAddr {
        SocketAddr::new(addr(literal), 443)
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

    /// Regression: a name rule is the documented primary configuration
    /// (`FLEXIQ_PUSH_TARGET_ALLOW=api.example.com` for a target like
    /// `https://svc.run.app`), and it used to dispatch nothing at all.
    /// `vet` ran every resolved address through `Allowlist::permits_address`,
    /// which matches only `AllowRule::Network` — so a name-only allowlist
    /// passed the name gate in `validate_target_url` and then refused every
    /// address the name resolved to as `NotAllowed`. Every earlier test used
    /// a CIDR or an IP literal (which parses as a `/32` network rule), so
    /// none of them touched this path.
    #[test]
    fn a_name_rule_permits_the_public_address_it_resolves_to() {
        let policy = EgressPolicy::new(allow("api.example.com"), false);
        let resolved = vec![sock("93.184.216.34")];

        let vetted = policy
            .vet("api.example.com", resolved.clone())
            .expect("a name rule must permit the address that name resolves to");

        assert_eq!(vetted, resolved);
    }

    #[test]
    fn a_suffix_rule_permits_the_public_address_a_subdomain_resolves_to() {
        let policy = EgressPolicy::new(allow(".run.app"), false);
        let resolved = vec![sock("93.184.216.34"), sock("8.8.8.8")];

        let vetted = policy
            .vet("svc.run.app", resolved.clone())
            .expect("a suffix rule must permit the addresses a subdomain resolves to");

        assert_eq!(vetted, resolved);
    }

    #[test]
    fn a_name_rule_does_not_vouch_for_a_different_name() {
        let policy = EgressPolicy::new(allow("api.example.com"), false);

        let refusal = policy
            .vet("evil.example.com", vec![sock("93.184.216.34")])
            .unwrap_err();

        assert!(matches!(refusal, EgressRefusal::NotAllowed { .. }));
    }

    #[test]
    fn a_name_rule_still_refuses_loopback_and_the_metadata_literals() {
        // The bound on the trade-off `vet` documents: a name rule trusts DNS
        // for that name, but the unconditional refusals are not inside what
        // it may vouch for, so a rebind onto loopback or IMDS still fails.
        let policy = EgressPolicy::new(allow("api.example.com"), false);
        for literal in [
            "127.0.0.1",
            "169.254.169.254",
            "fd00:ec2::254",
            "100.100.100.200",
        ] {
            let refusal = policy
                .vet("api.example.com", vec![sock(literal)])
                .unwrap_err();
            assert!(
                matches!(refusal, EgressRefusal::NeverRoutable { .. }),
                "{literal} must stay refused however the name is allowlisted"
            );
        }
    }

    #[test]
    fn a_cidr_only_allowlist_still_refuses_an_address_outside_it() {
        // The other half of the trade-off: an operator who wants the address
        // space constrained as well writes a CIDR, and a name that resolves
        // outside it is still refused.
        let policy = EgressPolicy::new(allow("93.184.216.0/24"), false);

        let refusal = policy
            .vet("api.example.com", vec![sock("8.8.8.8")])
            .unwrap_err();

        assert!(matches!(refusal, EgressRefusal::NotAllowed { .. }));
    }

    #[test]
    fn an_ip_literal_host_does_not_vouch_for_a_whole_resolution() {
        // `Allowlist::permits_host` folds a literal to `permits_address`, so
        // reading its answer as "the *name* is allowlisted" would let one
        // allowlisted literal vouch for every address alongside it. A literal
        // never reaches `vet` (the connector dials it without a resolver);
        // this pins that `vet` does not depend on that being true.
        let policy = EgressPolicy::new(allow("93.184.216.34"), false);

        let refusal = policy
            .vet(
                "93.184.216.34",
                vec![sock("93.184.216.34"), sock("8.8.8.8")],
            )
            .unwrap_err();

        assert!(matches!(refusal, EgressRefusal::NotAllowed { .. }));
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
