//! One push target: an operator-configured URL every claimed job is POSTed
//! to, for platforms that start a process from an inbound request (Cloud Run,
//! Lambda, and similar).
//!
//! A push target is a [`WorkerDispatcher`](crate::worker::WorkerDispatcher),
//! deliberately not a [`Transport`](crate::worker::transport::Transport): an
//! HTTP request/response pair has no duplex stream to split and sends no
//! `hello` frame, so it announces no slots — its capacity is configuration,
//! not negotiation. `HttpTargetConfig`'s `capacity` field is that number.
//!
//! This module is the configuration and URL validation half of the contract;
//! the private `contract` submodule is what actually goes on the wire. The
//! dispatcher itself — the `WorkerDispatcher` that POSTs — arrives in a
//! later commit.

use std::time::Duration;

use crate::net::Allowlist;

mod contract;
pub use contract::{
    idempotency_key, Outcome, Refusal, ACCEPTED_NOT_SETTLED, ENVELOPE_CONTENT_TYPE, HDR_ATTEMPT,
    HDR_DEADLINE_MS, HDR_DISABLED_MIDDLEWARE, HDR_IDEMPOTENCY_KEY, HDR_JOB_ID, HDR_LEASE,
    HDR_MAX_ATTEMPTS, HDR_METADATA, HDR_NAMESPACE, HDR_OUTCOME, HDR_PROTOCOL_VERSION, HDR_QUEUE,
    HDR_RETRY, HDR_TASK,
};

/// How to reach one push target, and what it is allowed to do.
#[derive(Clone)]
pub struct HttpTargetConfig {
    /// Absolute `http`/`https` URL every job is POSTed to.
    pub url: String,
    /// Jobs this target may be running at once.
    ///
    /// Configuration, not negotiation: a push target sends no `hello` and
    /// announces no slots, so there is nothing to read this from.
    pub capacity: u32,
    /// Ceiling on one request, before the job's own timeout is considered.
    pub request_timeout: Duration,
    /// Ceiling on establishing the connection.
    pub connect_timeout: Duration,
    /// How long `shutdown` waits for in-flight requests before abandoning them.
    pub shutdown_drain: Duration,
    /// Destinations this target may resolve to. Deny by default.
    pub allow: Allowlist,
    /// Permit loopback and link-local destinations.
    ///
    /// A library knob for this crate's own tests and for an embedder that
    /// genuinely dispatches to a sidecar on localhost. `flexiq-server` never
    /// sets it, and refuses the environment variable that asks for it.
    pub allow_loopback: bool,
    /// Longest payload this target is sent.
    pub max_request_bytes: usize,
    /// Longest response body read back.
    pub max_response_bytes: usize,
    /// Longest encoded `x-flexiq-metadata` header sent. Over this, the header
    /// is dropped rather than the job failed.
    pub max_metadata_header_bytes: usize,
    /// `User-Agent` sent with every dispatch.
    pub user_agent: String,
}

impl HttpTargetConfig {
    /// A target at `url`, permitted to reach `allow`, running `capacity` jobs
    /// at once. Every other field takes its default.
    pub fn new(url: impl Into<String>, capacity: u32, allow: Allowlist) -> Self {
        Self {
            url: url.into(),
            capacity,
            // A cold-start executor may still be importing handler modules;
            // the job's own timeout narrows this further once it starts.
            request_timeout: Duration::from_secs(60),
            // A push target is a configured, presumably nearby endpoint; five
            // seconds is generous for a TCP+TLS handshake and fails a black
            // hole fast.
            connect_timeout: Duration::from_secs(5),
            // Matches `RemoteConfig::shutdown_drain`, so an operator tunes one
            // number regardless of which dispatcher is running.
            shutdown_drain: Duration::from_secs(30),
            allow,
            // Deny by default; an embedder flips this only for a sidecar it
            // controls. `flexiq-server` never sets it.
            allow_loopback: false,
            // 8 MiB: room for a real task payload without accepting an
            // unbounded request body.
            max_request_bytes: 8 * 1024 * 1024,
            // 1 MiB: a target's answer is a status and an outcome header, not
            // a copy of the task's own result.
            max_response_bytes: 1024 * 1024,
            // 8 KiB: past any realistic encoded metadata blob, short of the
            // header caps common proxies enforce.
            max_metadata_header_bytes: 8 * 1024,
            user_agent: format!("flexiq-core/{}", env!("CARGO_PKG_VERSION")),
        }
    }
}

/// Hand-written because none of these fields is a secret in this commit —
/// unlike `RemoteConfig::auth_token`, there is nothing here to redact.
impl std::fmt::Debug for HttpTargetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpTargetConfig")
            .field("url", &self.url)
            .field("capacity", &self.capacity)
            .field("request_timeout", &self.request_timeout)
            .field("connect_timeout", &self.connect_timeout)
            .field("shutdown_drain", &self.shutdown_drain)
            .field("allow", &self.allow)
            .field("allow_loopback", &self.allow_loopback)
            .field("max_request_bytes", &self.max_request_bytes)
            .field("max_response_bytes", &self.max_response_bytes)
            .field("max_metadata_header_bytes", &self.max_metadata_header_bytes)
            .field("user_agent", &self.user_agent)
            .finish()
    }
}

/// Why a push target could not be built, or a dispatch could not be made.
#[derive(Debug, thiserror::Error)]
pub enum HttpTargetError {
    /// The configured URL was empty or whitespace-only.
    #[error("push target URL is empty")]
    MissingUrl,
    /// The URL could not be parsed at all.
    #[error("push target URL is not usable: {0}")]
    Url(String),
    /// The URL's scheme is neither `http` nor `https`.
    #[error("push target URL scheme must be http or https, got '{0}'")]
    Scheme(String),
    /// The URL has no host component.
    #[error("push target URL must include a hostname")]
    NoHost,
    /// Credentials in the authority are never sent and would be silently
    /// dropped, so a URL carrying them is refused rather than half-honoured.
    #[error("push target URL must not carry userinfo")]
    Userinfo,
    /// The URL's host is not named by [`HttpTargetConfig::allow`].
    #[error("push target host '{0}' is not on the allowlist")]
    HostRefused(String),
    /// [`HttpTargetConfig::capacity`] was `0`.
    #[error("push target capacity must be at least 1")]
    ZeroCapacity,
}

/// Parse and vet a target URL: absolute, `http`/`https`, a host, no userinfo,
/// and a host the allowlist names.
///
/// Name-based rules are settled here; the addresses a name resolves to are
/// vetted at connect time by a later commit's resolver, because a name that
/// resolves publicly now can be rebound before the socket opens.
// `pub(crate)` with no caller yet: the dispatcher that builds a target from
// `HttpTargetConfig` arrives in a later commit. This file's own tests are
// the only caller until then.
#[allow(dead_code)]
pub(crate) fn validate_target_url(
    url: &str,
    allow: &Allowlist,
) -> Result<url::Url, HttpTargetError> {
    if url.trim().is_empty() {
        return Err(HttpTargetError::MissingUrl);
    }

    // `url` itself refuses an empty authority on a special scheme
    // (`https:///x`) with `ParseError::EmptyHost` before a `Url` ever comes
    // back, so that specific failure is read as `NoHost` rather than the
    // generic parse error.
    let parsed = url::Url::parse(url).map_err(|error| match error {
        url::ParseError::EmptyHost => HttpTargetError::NoHost,
        other => HttpTargetError::Url(other.to_string()),
    })?;

    match parsed.scheme() {
        "http" | "https" => {}
        other => return Err(HttpTargetError::Scheme(other.to_string())),
    }

    // Read through `url::Host` rather than `Url::host_str`: the latter keeps
    // an IPv6 literal's brackets, but `Allowlist::permits_host` expects the
    // unbracketed form every other caller gives it.
    let host = match parsed.host() {
        Some(url::Host::Domain(domain)) => domain.to_string(),
        Some(url::Host::Ipv4(v4)) => v4.to_string(),
        Some(url::Host::Ipv6(v6)) => v6.to_string(),
        None => return Err(HttpTargetError::NoHost),
    };

    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(HttpTargetError::Userinfo);
    }

    if !allow.permits_host(&host) {
        return Err(HttpTargetError::HostRefused(host));
    }

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(entries: &str) -> Allowlist {
        Allowlist::parse(entries).expect("test allowlist parses")
    }

    #[test]
    fn a_url_off_the_allowlist_is_refused_naming_the_host() {
        let allow = allow("api.example.com");
        let error = validate_target_url("https://evil.example.com/hook", &allow).unwrap_err();
        assert!(matches!(error, HttpTargetError::HostRefused(host) if host == "evil.example.com"));
    }

    #[test]
    fn a_non_http_scheme_is_refused() {
        let allow = allow("files.example.com");
        let error = validate_target_url("ftp://files.example.com/hook", &allow).unwrap_err();
        assert!(matches!(error, HttpTargetError::Scheme(scheme) if scheme == "ftp"));
    }

    #[test]
    fn an_empty_url_is_refused() {
        let allow = allow("api.example.com");
        assert!(matches!(
            validate_target_url("", &allow),
            Err(HttpTargetError::MissingUrl)
        ));
        assert!(matches!(
            validate_target_url("   ", &allow),
            Err(HttpTargetError::MissingUrl)
        ));
    }

    #[test]
    fn userinfo_in_the_authority_is_refused() {
        let allow = allow("api.example.com");
        let error =
            validate_target_url("https://user:pw@api.example.com/hook", &allow).unwrap_err();
        assert!(matches!(error, HttpTargetError::Userinfo));
    }

    #[test]
    fn a_url_with_no_host_is_refused() {
        // Not `"https:///nohost"`: the `url` crate collapses repeated
        // slashes after a special scheme's `//`, so that string parses with
        // host `"nohost"` rather than an empty one. An empty authority with
        // nothing after it is what actually triggers `url`'s own
        // `EmptyHost`, which this module reads as `NoHost`.
        let allow = allow("api.example.com");
        let error = validate_target_url("https:///", &allow).unwrap_err();
        assert!(matches!(error, HttpTargetError::NoHost));
    }

    #[test]
    fn a_permitted_host_round_trips_with_its_path_and_query() {
        let allow = allow("api.example.com");
        let parsed = validate_target_url("https://api.example.com/hook?job=1", &allow)
            .expect("permitted host must be accepted");
        assert_eq!(parsed.path(), "/hook");
        assert_eq!(parsed.query(), Some("job=1"));
    }

    #[test]
    fn an_ip_literal_host_is_matched_through_the_allowlist_address_path() {
        let permitted = allow("10.0.0.0/8");
        let parsed = validate_target_url("http://10.1.2.3:8080/hook", &permitted)
            .expect("an address inside the CIDR must be accepted");
        assert_eq!(parsed.host_str(), Some("10.1.2.3"));

        let elsewhere = allow("9.0.0.0/8");
        let refused = validate_target_url("http://10.1.2.3:8080/hook", &elsewhere);
        assert!(matches!(refused, Err(HttpTargetError::HostRefused(_))));
    }
}
