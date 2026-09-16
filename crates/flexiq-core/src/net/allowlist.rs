//! An allowlist of hosts and CIDRs, deny by default.
//!
//! [`Allowlist`] answers one question: does an operator's configuration name
//! this destination? It does not know about loopback, link-local, or the
//! cloud metadata addresses in [`super::is_never_routable`] — that refusal is
//! unconditional and applied by the caller as a separate layer, after this
//! allowlist has already said yes. Keeping the two apart means "named by the
//! operator" and "refused no matter what" read as two sentences in the code,
//! the way they already do in the issue that asked for this.
//!
//! An allowlist that names nothing is refused at construction
//! ([`AllowlistError::Empty`]) rather than treated as deny-all: a target that
//! can never dispatch a job is a configuration mistake, not a valid policy.

use std::net::IpAddr;

use thiserror::Error;

/// One entry of an allowlist.
#[derive(Debug, Clone)]
pub enum AllowRule {
    /// An exact hostname, compared case-insensitively.
    Host(String),
    /// A name and its subdomains, written with a leading dot.
    Suffix(String),
    /// A network, written in CIDR form.
    Network {
        /// The network's base address.
        base: IpAddr,
        /// Prefix length in bits: 0..=32 for an IPv4 base, 0..=128 for IPv6.
        prefix: u8,
    },
}

/// Why an allowlist could not be built.
#[derive(Error, Debug)]
pub enum AllowlistError {
    /// One entry could not be read. `entry` is echoed back so an operator can
    /// find it in their configuration.
    #[error("malformed allowlist entry '{entry}': {reason}")]
    Malformed {
        /// The offending segment, verbatim.
        entry: String,
        /// Why it was rejected.
        reason: String,
    },
    /// The allowlist named nothing.
    #[error("allowlist is empty")]
    Empty,
}

/// A set of hosts, subdomains, and networks a dispatch target must match.
///
/// Deny by default: nothing a rule does not name is permitted, and an
/// allowlist naming nothing fails to construct rather than quietly denying
/// everything (see [`AllowlistError::Empty`]).
///
/// **This is a matcher, not an egress policy.** It answers only "does some
/// rule name this?", and knows nothing about the destinations that are never
/// anyone's to grant — [`is_never_routable`](crate::net::is_never_routable)
/// addresses, loopback and the cloud metadata literals all pass it whenever a
/// rule happens to cover them. To decide whether an outbound destination may
/// be dialled, use `EgressPolicy` in `flexiq_core::http` (behind the
/// `http-target` feature), which layers the unconditional refusals over this;
/// named in prose rather than linked because it does not exist in a default
/// build.
#[derive(Debug, Clone)]
pub struct Allowlist {
    rules: Vec<AllowRule>,
}

impl Allowlist {
    /// Parses a comma-separated list of entries.
    ///
    /// Each segment is trimmed of ASCII whitespace; empty segments (a
    /// trailing or doubled comma) are skipped rather than rejected. A segment
    /// becomes, in order: a CIDR if it contains `/`, an [`AllowRule::Network`]
    /// with a full-length prefix if it parses as a bare `IpAddr`, an
    /// [`AllowRule::Suffix`] if it starts with `.`, or otherwise an
    /// [`AllowRule::Host`].
    pub fn parse(entries: &str) -> Result<Self, AllowlistError> {
        let mut rules = Vec::new();
        for segment in entries.split(',') {
            let segment = segment.trim_matches(|c: char| c.is_ascii_whitespace());
            if segment.is_empty() {
                continue;
            }
            rules.push(parse_entry(segment)?);
        }
        Self::from_rules(rules)
    }

    /// Builds an allowlist from already-parsed rules, e.g. loaded from typed
    /// configuration rather than a string.
    ///
    /// Applies the same checks [`Self::parse`] does: at least one rule, and
    /// every [`AllowRule::Network`] prefix within its address family's width.
    pub fn from_rules(rules: Vec<AllowRule>) -> Result<Self, AllowlistError> {
        if rules.is_empty() {
            return Err(AllowlistError::Empty);
        }
        for rule in &rules {
            if let AllowRule::Network { base, prefix } = rule {
                validate_prefix(*base, *prefix).map_err(|reason| AllowlistError::Malformed {
                    entry: format!("{base}/{prefix}"),
                    reason,
                })?;
            }
        }
        Ok(Self { rules })
    }

    /// Whether `host` matches some rule.
    ///
    /// The comparison is case-insensitive and ignores one trailing `.` (a
    /// fully-qualified name and its root-relative form are the same name). A
    /// host that parses as an IP literal is matched as an address instead —
    /// callers pass `url::Host`-derived strings, which are already
    /// unbracketed, so a bracketed literal is not this function's problem.
    ///
    /// That literal path is the reason this is the wrong function to gate an
    /// outbound dial on: against a rule of `127.0.0.0/8` it answers `true` for
    /// `"127.0.0.1"`. See the type's own docs — `EgressPolicy` is what adds
    /// the unconditional refusals.
    pub fn permits_host(&self, host: &str) -> bool {
        let lowered = host.to_ascii_lowercase();
        let normalized = lowered.strip_suffix('.').unwrap_or(&lowered);
        if let Ok(address) = normalized.parse::<IpAddr>() {
            return self.permits_address(address);
        }
        self.rules.iter().any(|rule| match rule {
            AllowRule::Host(h) => normalized == h,
            AllowRule::Suffix(base) => {
                normalized == base || normalized.ends_with(&format!(".{base}"))
            }
            AllowRule::Network { .. } => false,
        })
    }

    /// Whether `address` falls inside some [`AllowRule::Network`].
    ///
    /// An IPv4-mapped IPv6 address is folded to IPv4 on both sides before
    /// comparing, so a rule written in either family matches the same real
    /// addresses regardless of which representation the caller hands in.
    pub fn permits_address(&self, address: IpAddr) -> bool {
        self.rules.iter().any(|rule| match rule {
            AllowRule::Network { base, prefix } => network_contains(*base, *prefix, address),
            AllowRule::Host(_) | AllowRule::Suffix(_) => false,
        })
    }

    /// The parsed rules, in the order they were given.
    pub fn rules(&self) -> &[AllowRule] {
        &self.rules
    }
}

/// Parses one non-empty, already-trimmed segment into a rule.
fn parse_entry(segment: &str) -> Result<AllowRule, AllowlistError> {
    let malformed = |reason: String| AllowlistError::Malformed {
        entry: segment.to_string(),
        reason,
    };

    if segment.contains('/') {
        // `split_once` takes only the first '/'; a second one survives into
        // `right`, which is how "10.0.0.0/8/9" is caught below. The `else`
        // arm is unreachable (the `contains` above guarantees a match) but
        // costs nothing to make an error instead of a panic.
        let Some((left, right)) = segment.split_once('/') else {
            return Err(malformed("a CIDR entry takes exactly one '/'".to_string()));
        };
        if right.contains('/') {
            return Err(malformed("a CIDR entry takes exactly one '/'".to_string()));
        }
        let base: IpAddr = left
            .parse()
            .map_err(|_| malformed(format!("'{left}' is not an IP address")))?;
        let prefix: u8 = right
            .parse()
            .map_err(|_| malformed(format!("'{right}' is not a valid prefix length")))?;
        validate_prefix(base, prefix).map_err(malformed)?;
        return Ok(AllowRule::Network { base, prefix });
    }

    if let Ok(address) = segment.parse::<IpAddr>() {
        // Normalise here, not at match time, so `permits_address` sees every
        // address-shaped rule (bare IP or CIDR) through the same code path.
        let prefix = match address {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        return Ok(AllowRule::Network {
            base: address,
            prefix,
        });
    }

    if let Some(rest) = segment.strip_prefix('.') {
        if rest.is_empty() {
            return Err(malformed(
                "a suffix rule needs a name after the leading dot".to_string(),
            ));
        }
        return Ok(AllowRule::Suffix(rest.to_ascii_lowercase()));
    }

    Ok(AllowRule::Host(segment.to_ascii_lowercase()))
}

/// Rejects a prefix wider than its address family allows.
fn validate_prefix(base: IpAddr, prefix: u8) -> Result<(), String> {
    let max = match base {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if prefix > max {
        Err(format!(
            "prefix /{prefix} exceeds /{max} for this address family"
        ))
    } else {
        Ok(())
    }
}

/// Folds an IPv4-mapped IPv6 address to its IPv4 form; anything else passes
/// through unchanged.
fn fold_mapped(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V4(_) => address,
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(address),
    }
}

/// Whether `address`'s first `prefix` bits match `base`'s.
///
/// Both sides are folded first so an IPv4-mapped rule base and an
/// IPv4-mapped test address compare as IPv4; a family mismatch that remains
/// after folding (a v4 rule against a genuine v6 address, or the reverse)
/// never matches.
fn network_contains(base: IpAddr, prefix: u8, address: IpAddr) -> bool {
    match (fold_mapped(base), fold_mapped(address)) {
        (IpAddr::V4(base), IpAddr::V4(address)) => {
            octets_match(&base.octets(), &address.octets(), prefix)
        }
        (IpAddr::V6(base), IpAddr::V6(address)) => {
            octets_match(&base.octets(), &address.octets(), prefix)
        }
        _ => false,
    }
}

/// Compares the first `prefix` bits of two equal-length octet slices: whole
/// bytes with `==`, the remaining bits with a mask built from the leftover
/// bit count.
fn octets_match(base: &[u8], address: &[u8], prefix: u8) -> bool {
    let prefix = prefix as usize;
    let bits = base.len() * 8;
    if prefix > bits {
        // Reachable only when a rule base was validated against a wider
        // family and then folded narrower at match time (an IPv4-mapped
        // literal parsed as v6, prefix validated up to /128, folded to a
        // 4-byte v4 address here). Such a rule can never be satisfied, so
        // refuse instead of indexing past the end of the octet slice.
        return false;
    }
    let whole_bytes = prefix / 8;
    let remainder = prefix % 8;
    if base[..whole_bytes] != address[..whole_bytes] {
        return false;
    }
    if remainder == 0 {
        return true;
    }
    let mask = 0xFFu8 << (8 - remainder);
    (base[whole_bytes] & mask) == (address[whole_bytes] & mask)
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

    #[test]
    fn an_empty_allowlist_is_refused_at_parse() {
        assert!(matches!(Allowlist::parse(""), Err(AllowlistError::Empty)));
        assert!(matches!(
            Allowlist::parse("  ,  "),
            Err(AllowlistError::Empty)
        ));
        assert!(matches!(
            Allowlist::from_rules(vec![]),
            Err(AllowlistError::Empty)
        ));
    }

    #[test]
    fn an_exact_host_matches_only_itself() {
        let list = allow("api.example.com");
        assert!(list.permits_host("api.example.com"));
        assert!(list.permits_host("API.EXAMPLE.COM"));
        assert!(list.permits_host("api.example.com."));
        assert!(!list.permits_host("evil.api.example.com"));
        assert!(!list.permits_host("api.example.com.evil.test"));
    }

    #[test]
    fn a_dot_suffix_matches_subdomains_but_not_a_lookalike() {
        let list = allow(".run.app");
        assert!(list.permits_host("run.app"));
        assert!(list.permits_host("svc.run.app"));
        assert!(!list.permits_host("evilrun.app"));
        assert!(!list.permits_host("run.app.evil.test"));
        assert!(!list.permits_host("app"));
    }

    #[test]
    fn a_cidr_matches_inside_its_prefix_and_not_outside() {
        let list = allow("10.42.0.0/16");
        assert!(list.permits_address(addr("10.42.0.1")));
        assert!(list.permits_address(addr("10.42.255.254")));
        assert!(!list.permits_address(addr("10.43.0.1")));
    }

    #[test]
    fn a_prefix_that_is_not_a_byte_boundary_matches_on_bits() {
        let list = allow("192.0.2.0/23");
        assert!(list.permits_address(addr("192.0.3.1")));
        assert!(!list.permits_address(addr("192.0.4.1")));

        // A /0 rule matches everything in its own family, and only its family.
        let wide_open_v4 = allow("0.0.0.0/0");
        assert!(wide_open_v4.permits_address(addr("8.8.8.8")));
        assert!(!wide_open_v4.permits_address(addr("2606:4700::1111")));
    }

    #[test]
    fn a_v6_cidr_matches_on_bits_not_bytes() {
        let list = allow("2001:db8::/33");
        assert!(list.permits_address(addr("2001:db8:7fff::1")));
        assert!(!list.permits_address(addr("2001:db8:8000::1")));
    }

    #[test]
    fn a_host_that_is_an_ip_literal_is_matched_as_an_address() {
        let list = allow("93.184.216.34");
        assert!(list.permits_host("93.184.216.34"));
        assert!(list.permits_address(addr("93.184.216.34")));
    }

    #[test]
    fn an_ipv4_mapped_address_is_matched_as_ipv4() {
        let list = allow("10.0.0.0/8");
        assert!(list.permits_address(addr("::ffff:10.0.0.1")));
    }

    /// Regression: `validate_prefix` measures the prefix against the family the
    /// entry was *written* in, so `::ffff:10.0.0.0/100` parses — /100 is inside
    /// /128 — and then folds to a four-byte base at match time. The guard in
    /// `octets_match` is what keeps that from slicing past the end, and the
    /// entry is reachable straight from operator input (`flexiq-server` parses
    /// `FLEXIQ_PUSH_TARGET_ALLOW` through this same `parse`).
    #[test]
    fn a_mapped_base_with_an_over_wide_prefix_refuses_rather_than_panicking() {
        let list = allow("::ffff:10.0.0.0/100");
        assert!(!list.permits_address(addr("10.0.0.1")));
        assert!(!list.permits_address(addr("::ffff:10.0.0.1")));
        assert!(!list.permits_host("10.0.0.1"));
        // /32 is the widest a folded v4 base can satisfy; /33 is the first
        // that cannot, so the guard is not only reached by extreme values.
        assert!(!allow("::ffff:10.0.0.0/33").permits_address(addr("10.0.0.1")));
    }

    #[test]
    fn families_do_not_cross() {
        let v4_only = allow("10.0.0.0/8");
        assert!(!v4_only.permits_address(addr("2606:4700::1111")));

        let v6_only = allow("2001:db8::/32");
        assert!(!v6_only.permits_address(addr("93.184.216.34")));
    }

    #[test]
    fn a_malformed_entry_names_itself() {
        for entry in [
            "10.0.0.0/33",
            "10.0.0.0/x",
            "not a host/8",
            ".",
            "10.0.0.0/8/9",
        ] {
            match Allowlist::parse(entry) {
                Err(AllowlistError::Malformed { entry: got, .. }) => {
                    assert_eq!(got, entry, "entry echoed back must match verbatim");
                }
                other => panic!("{entry} should be Malformed, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_trailing_comma_is_not_an_error() {
        let list = allow("api.example.com,");
        assert!(list.permits_host("api.example.com"));
        assert_eq!(list.rules().len(), 1);
    }
}
