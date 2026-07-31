//! Outbound URL safety for dashboard-configured webhooks.
//!
//! Anyone who can write to the settings store could otherwise turn this process
//! into an SSRF proxy against the pod network or a cloud metadata endpoint, so
//! loopback, link-local, and private destinations are refused. The check runs
//! again immediately before each send: a name that resolved publicly at
//! registration can be rebound later.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};

/// Escape hatch for local development against `http://localhost`.
const ALLOW_PRIVATE_VAR: &str = "TASKITO_WEBHOOKS_ALLOW_PRIVATE";

/// Names that mean "this host" or "this network" whatever DNS says.
const BLOCKED_HOSTNAMES: [&str; 4] = [
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
];
const BLOCKED_SUFFIXES: [&str; 6] = [
    ".localhost",
    ".local",
    ".internal",
    ".intranet",
    ".lan",
    ".private",
];

/// Why a webhook URL was refused. The message is safe to return to the caller.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct UnsafeWebhookUrl(String);

/// Reject `url` unless it targets a public http/https destination.
pub fn validate_webhook_url(url: &str) -> Result<(), UnsafeWebhookUrl> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| refuse("URL must include a scheme, http or https"))?;
    if !matches!(scheme.to_ascii_lowercase().as_str(), "http" | "https") {
        return Err(refuse(format!(
            "URL scheme must be http or https, got '{scheme}'"
        )));
    }

    let host = hostname(rest).ok_or_else(|| refuse("URL must include a hostname"))?;
    if std::env::var_os(ALLOW_PRIVATE_VAR).is_some() {
        return Ok(());
    }

    let lowered = host.to_ascii_lowercase();
    if BLOCKED_HOSTNAMES.contains(&lowered.as_str())
        || BLOCKED_SUFFIXES
            .iter()
            .any(|suffix| lowered.ends_with(suffix))
    {
        return Err(refuse(format!(
            "URL host '{host}' resolves to a private network"
        )));
    }

    for address in resolve(&lowered)? {
        if is_private(address) {
            return Err(refuse(format!(
                "URL host '{host}' resolves to private address {address}"
            )));
        }
    }
    Ok(())
}

/// Whether `path` is safe as a same-origin post-login redirect.
///
/// Only relative paths rooted at `/`. Absolute (`http://evil/x`) and
/// protocol-relative (`//evil/x`) targets are what make an open redirect.
pub fn is_safe_redirect(path: Option<&str>) -> bool {
    let Some(path) = path.filter(|candidate| !candidate.is_empty()) else {
        return false;
    };
    path.starts_with('/') && !path.starts_with("//") && !path.starts_with("/\\")
}

/// Host portion of everything after `scheme://`, without userinfo or port.
fn hostname(rest: &str) -> Option<String> {
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .filter(|value| !value.is_empty())?;
    // Userinfo can itself contain an `@`, so the host is after the last one.
    let host_port = authority.rsplit('@').next()?;

    let host = if let Some(end) = host_port.strip_prefix('[') {
        // IPv6 literal: `[::1]:8080`.
        end.split(']').next()?.to_string()
    } else {
        host_port.split(':').next()?.to_string()
    };
    (!host.is_empty()).then_some(host)
}

/// Literal IPs are checked as written; names are resolved first.
fn resolve(host: &str) -> Result<Vec<IpAddr>, UnsafeWebhookUrl> {
    if let Ok(literal) = host.parse::<IpAddr>() {
        return Ok(vec![literal]);
    }
    // The port is irrelevant to the address check but `to_socket_addrs`
    // requires one.
    (host, 443u16)
        .to_socket_addrs()
        .map(|addresses| addresses.map(|address| address.ip()).collect())
        .map_err(|error| refuse(format!("could not resolve '{host}': {error}")))
}

/// Everything `ipaddress.is_private` covers on the Python side, plus the
/// ranges that are never a legitimate webhook target.
fn is_private(address: IpAddr) -> bool {
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

fn is_private_v4(address: Ipv4Addr) -> bool {
    let [first, second, ..] = address.octets();
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
        || (first == 192 && second == 0 && address.octets()[2] == 0)
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

fn refuse(message: impl Into<String>) -> UnsafeWebhookUrl {
    UnsafeWebhookUrl(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_http_schemes_are_accepted() {
        assert!(validate_webhook_url("ftp://example.com/hook").is_err());
        assert!(validate_webhook_url("file:///etc/passwd").is_err());
        assert!(validate_webhook_url("example.com/hook").is_err());
    }

    #[test]
    fn loopback_and_private_literals_are_refused() {
        for url in [
            "http://127.0.0.1/hook",
            "http://10.0.0.5/hook",
            "http://192.168.1.1:9000/hook",
            "http://169.254.169.254/latest/meta-data",
            "http://[::1]/hook",
            "http://[::ffff:127.0.0.1]/hook",
            "http://100.64.0.1/hook",
            "http://0.0.0.0/hook",
        ] {
            assert!(validate_webhook_url(url).is_err(), "must refuse {url}");
        }
    }

    #[test]
    fn blocked_names_are_refused_without_resolving() {
        for url in [
            "https://localhost/hook",
            "https://api.internal/hook",
            "https://db.local/hook",
        ] {
            assert!(validate_webhook_url(url).is_err(), "must refuse {url}");
        }
    }

    #[test]
    fn a_public_literal_is_accepted() {
        validate_webhook_url("https://93.184.216.34/hook").expect("public address");
    }

    #[test]
    fn hostnames_are_extracted_from_full_urls() {
        assert_eq!(
            hostname("user:pw@example.com:8443/x?y=1").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            hostname("[2001:4860:4860::8888]:443/x").as_deref(),
            Some("2001:4860:4860::8888")
        );
        assert_eq!(hostname("/just-a-path"), None);
    }

    #[test]
    fn redirect_targets_must_be_same_origin_paths() {
        assert!(is_safe_redirect(Some("/jobs")));
        assert!(!is_safe_redirect(Some("//evil.example/x")));
        assert!(!is_safe_redirect(Some("https://evil.example/x")));
        assert!(!is_safe_redirect(Some("/\\evil.example")));
        assert!(!is_safe_redirect(Some("")));
        assert!(!is_safe_redirect(None));
    }
}
