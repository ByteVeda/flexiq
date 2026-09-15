//! Facts about IP space, for every outbound path in the workspace.
//!
//! Whether `100.64.0.0/10` is carrier-grade NAT is a property of the address
//! space, not a policy any one caller gets to have an opinion about. Two copies
//! of that knowledge drift the moment one of them learns about a new reserved
//! range, so it lives here once and the guards above it decide what to *do*
//! with the answer.
//!
//! Policy stays with the caller, and the two callers disagree on purpose: the
//! dashboard's webhook guard is a denylist with an operator escape hatch, while
//! the push-dispatch target is an allowlist that denies by default. Both ask
//! the same questions of the same predicates.
//!
//! Unconditional — no cargo feature. `flexiq-server` compiles in every build
//! and cannot depend on something gated off by default.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

pub mod allowlist;
pub use allowlist::{AllowRule, Allowlist, AllowlistError};

/// The IMDS address every major cloud answers on.
///
/// Inside `169.254.0.0/16`, so [`is_never_routable`] already covers it; named
/// here because a reader looking for it should find it rather than have to know
/// that link-local is where it lives.
pub const CLOUD_METADATA_V4: Ipv4Addr = Ipv4Addr::new(169, 254, 169, 254);

/// EC2's IPv6 instance-metadata address.
///
/// Inside `fc00::/7`, which is unique-local rather than link-local, so this one
/// has to be named to be refused unconditionally.
pub const CLOUD_METADATA_V6: Ipv6Addr = Ipv6Addr::new(0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x0254);

/// Alibaba Cloud's metadata address.
///
/// Inside `100.64.0.0/10` (carrier-grade NAT), which is private but not
/// link-local, so it too has to be named.
pub const ALIBABA_METADATA_V4: Ipv4Addr = Ipv4Addr::new(100, 100, 100, 200);

/// Whether `address` is private, reserved, or otherwise not a public
/// destination.
///
/// Everything `ipaddress.is_private` covers on the Python side, plus the ranges
/// that are never a legitimate outbound target.
pub fn is_private_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => is_private_v4(v4),
        // An IPv4-mapped address is an IPv4 destination; checking only the v6
        // predicates would let `::ffff:127.0.0.1` through.
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(mapped) => is_private_v4(mapped),
            None => is_private_v6(v6),
        },
    }
}

/// The strict subset refused whatever an allowlist says.
///
/// [`is_private_address`] is a default an operator may overrule — a CIDR on an
/// allowlist is them saying their pod network is a legitimate destination. This
/// is the part they may not: loopback and link-local reach the host and the
/// credential endpoint sitting on it, and the three metadata literals hand out
/// cloud credentials to anything that asks. Allowlisting one of these is never
/// what someone meant, so the guard does not offer it.
pub fn is_never_routable(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => is_never_routable_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(mapped) => is_never_routable_v4(mapped),
            None => is_never_routable_v6(v6),
        },
    }
}

/// Whether `address` is a loopback address, reading an IPv4-mapped IPv6
/// address as the IPv4 address it is.
///
/// Split out from [`is_never_routable`] because loopback is the one entry in
/// that set an embedder may legitimately want: a dispatch target on the same
/// host, or a test server on an ephemeral port. Link-local, the metadata
/// literals, multicast and broadcast have no such case and stay unconditional.
pub fn is_loopback_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(mapped) => mapped.is_loopback(),
            None => v6.is_loopback(),
        },
    }
}

fn is_never_routable_v4(address: Ipv4Addr) -> bool {
    address.is_loopback()
        // 169.254.0.0/16, which is where CLOUD_METADATA_V4 lives.
        || address.is_link_local()
        || address.is_unspecified()
        || address.is_multicast()
        || address.is_broadcast()
        || address == ALIBABA_METADATA_V4
}

fn is_never_routable_v6(address: Ipv6Addr) -> bool {
    address.is_loopback()
        || address.is_unspecified()
        || address.is_multicast()
        // fe80::/10 link-local.
        || (address.segments()[0] & 0xffc0) == 0xfe80
        || address == CLOUD_METADATA_V6
}

fn is_private_v4(address: Ipv4Addr) -> bool {
    let [first, second, third, _] = address.octets();
    address.is_private()
        || address.is_loopback()
        || address.is_link_local()
        || address.is_multicast()
        || address.is_broadcast()
        || address.is_unspecified()
        || address.is_documentation()
        // 100.64.0.0/10 carrier-grade NAT, 198.18.0.0/15 benchmarking,
        // 192.0.0.0/24 IETF protocol assignments, 240.0.0.0/4 reserved.
        || (first == 100 && (64..128).contains(&second))
        || (first == 198 && (18..20).contains(&second))
        || (first == 192 && second == 0 && third == 0)
        || first >= 240
}

fn is_private_v6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    address.is_loopback()
        || address.is_multicast()
        || address.is_unspecified()
        // fc00::/7 unique-local, fe80::/10 link-local, 2001:db8::/32 docs.
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
        || (segments[0] == 0x2001 && segments[1] == 0x0db8)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every address that must be refused however the caller is configured.
    const NEVER_ROUTABLE: [&str; 11] = [
        "127.0.0.1",
        "127.10.20.30",
        "::1",
        "::ffff:127.0.0.1",
        "169.254.169.254",
        "169.254.0.1",
        "fe80::1",
        "fd00:ec2::254",
        "100.100.100.200",
        "0.0.0.0",
        "224.0.0.1",
    ];

    fn parse(literal: &str) -> IpAddr {
        literal.parse().expect("test address parses")
    }

    #[test]
    fn loopback_link_local_and_metadata_are_never_routable() {
        for literal in NEVER_ROUTABLE {
            assert!(
                is_never_routable(parse(literal)),
                "{literal} must be refused whatever the allowlist says"
            );
        }
        assert!(is_never_routable(parse("255.255.255.255")));
    }

    #[test]
    fn never_routable_implies_private() {
        // The two predicates answer different questions, but they may not
        // disagree about the strictest case: an address nothing may reach is
        // not a public destination either.
        for literal in NEVER_ROUTABLE {
            assert!(
                is_private_address(parse(literal)),
                "{literal} is never routable, so it cannot be public"
            );
        }
    }

    #[test]
    fn a_pod_network_is_private_but_still_routable() {
        // The difference that matters: an operator may allowlist these, and
        // may not allowlist anything in the test above.
        for literal in ["10.42.0.1", "192.168.1.1", "172.16.0.1", "fc00::1"] {
            let address = parse(literal);
            assert!(is_private_address(address), "{literal} is private");
            assert!(
                !is_never_routable(address),
                "{literal} is refusable by policy, not by construction"
            );
        }
    }

    #[test]
    fn a_public_address_is_neither_private_nor_unroutable() {
        for literal in ["93.184.216.34", "8.8.8.8", "2606:4700::1111"] {
            let address = parse(literal);
            assert!(!is_private_address(address), "{literal} is public");
            assert!(!is_never_routable(address), "{literal} is reachable");
        }
    }

    #[test]
    fn an_ipv4_mapped_address_is_read_as_ipv4() {
        // Checking only the v6 predicates would let `::ffff:127.0.0.1` through,
        // and the request would go to loopback all the same.
        assert!(is_private_address(parse("::ffff:10.0.0.1")));
        assert!(is_never_routable(parse("::ffff:169.254.169.254")));
        assert!(!is_private_address(parse("::ffff:93.184.216.34")));
    }

    #[test]
    fn the_named_metadata_literals_are_what_they_claim() {
        assert!(is_never_routable(IpAddr::V4(CLOUD_METADATA_V4)));
        assert!(is_never_routable(IpAddr::V6(CLOUD_METADATA_V6)));
        assert!(is_never_routable(IpAddr::V4(ALIBABA_METADATA_V4)));
    }

    #[test]
    fn is_loopback_address_agrees_with_never_routable_on_loopback_only() {
        // Loopback is where the two predicates must agree...
        for literal in ["127.0.0.1", "127.10.20.30", "::1", "::ffff:127.0.0.1"] {
            let address = parse(literal);
            assert!(is_never_routable(address), "{literal} is never routable");
            assert!(is_loopback_address(address), "{literal} is loopback");
        }
        // ...and disagree everywhere else in the never-routable set: the
        // metadata literals are never a loopback address, however the caller
        // reads them.
        for literal in ["169.254.169.254", "100.100.100.200"] {
            let address = parse(literal);
            assert!(is_never_routable(address), "{literal} is never routable");
            assert!(!is_loopback_address(address), "{literal} is not loopback");
        }
    }
}
