//! AWS Signature Version 4 (SigV4): canonicalisation, key derivation, the
//! credential chain that finds keys, and the [`Signer`] that wires them all
//! into an `Authorization` header.
//!
//! GitHub issue #844 names SigV4 as the outbound-auth scheme for Lambda
//! function URLs and API Gateway. It is hand-rolled — not built on the
//! `aws-sigv4`/`aws-credential-types` crates — because both declare
//! `rust-version = 1.94.1`, this repo's MSRV is **1.88**, and
//! `publish-crates.yml` gates `flexiq-core --all-features` at that floor.
//! `aws-config`, where the credential chain actually lives, drags a second
//! HTTP stack this crate could not route through its own egress-guarded
//! resolver anyway.
//!
//! The algorithm is fiddly, and the answer to that is vectors, not a
//! dependency: [`canonical`] and [`key`] are each pinned to AWS's own
//! published `aws-sig-v4-test-suite`, AWS's own worked
//! signing-key-derivation example, or an independent codebase's test fixture
//! (smithy-lang/smithy-rs's `aws-sigv4` crate) cross-checked by hand — never
//! from this code computing its own expected answer. This module's own
//! `a_full_request_matches_a_pinned_vector` test below extends that
//! discipline to the one combination no published vector covers (see its own
//! comment for the source).
//!
//! Three pieces on top of that pure half: [`credentials`] is the chain and
//! its sources, [`imds`] is the IMDSv2 call sequence one of those sources
//! uses, and this module is the [`Signer`] that turns a request plus a
//! credential into headers.

pub(crate) mod canonical;
mod credentials;
mod imds;
pub(crate) mod key;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::header::{HeaderMap, HeaderName, AUTHORIZATION};

use super::cache::CredentialCache;
use super::digest::{hmac_sha256_hex, sha256_hex};
use super::metadata::MetadataClient;
use super::{insert_header, AuthError, Signer, SigningRequest};
use canonical::{authorization_header, canonical_request, string_to_sign, timestamps};
pub use credentials::AwsCredentialSource;
use credentials::AwsCredentials;
use key::{credential_scope, signing_key};

/// SigV4 configuration for one push target.
#[derive(Clone, Debug)]
pub struct SigV4Config {
    /// Which of AWS's credential sources to sign with.
    pub source: AwsCredentialSource,
    /// Overrides the region inferred from the target URL.
    pub region: Option<String>,
    /// Overrides the service inferred from the target URL.
    pub service: Option<String>,
}

/// Signs each dispatch with AWS Signature Version 4.
pub struct SigV4Signer {
    source: AwsCredentialSource,
    region: String,
    service: String,
    metadata: MetadataClient,
    credentials: CredentialCache<AwsCredentials>,
    /// IMDSv2's session token, cached separately from the role credentials
    /// it is used to fetch — see `imds.rs`'s doc for why one outlives the
    /// other. Built unconditionally, even for a source that never touches
    /// IMDS: it is two empty locks, and branching construction to skip it
    /// would trade that for a second code path to keep correct.
    imds_token_cache: CredentialCache<String>,
}

impl SigV4Signer {
    /// Build a signer for one push target, inferring region and service from
    /// `target` unless `config` overrides them.
    pub fn new(config: SigV4Config, target: &url::Url) -> Result<Self, AuthError> {
        let (region, service) = resolve_region_and_service(
            target,
            config.region.as_deref(),
            config.service.as_deref(),
        )?;
        Ok(Self {
            source: config.source,
            region,
            service,
            metadata: MetadataClient::new()?,
            credentials: CredentialCache::new(),
            imds_token_cache: CredentialCache::new(),
        })
    }

    /// The pure core, with both impure inputs injected, so every AWS vector
    /// is a plain assertion with no clock and no network.
    ///
    /// No stale-credential extension: unlike the AWS SDKs, this never serves
    /// a credential known to be past its hard expiry when a refresh fails.
    /// [`CredentialCache::get_or_refresh`] already reuses a still-*usable*
    /// value on a refresh error — the same behaviour OIDC relies on — but a
    /// dispatch that cannot be signed with a credential known to be dead is
    /// a retryable failure the dispatcher already owns the policy for;
    /// serving a dead credential anyway would only move that failure
    /// somewhere with worse diagnostics.
    fn sign_at(
        &self,
        request: &SigningRequest<'_>,
        credentials: &AwsCredentials,
        now: DateTime<Utc>,
    ) -> Result<HeaderMap, AuthError> {
        let payload_sha256_hex = sha256_hex(request.body);
        // One clock reading, two derived strings: see `timestamps`'s own doc
        // for why calling `Utc::now()` a second time here would risk a scope
        // whose date disagrees with `x-amz-date`.
        let (amz_date, datestamp) = timestamps(now);

        let mut headers_to_sign = request.headers.clone();
        insert_header(
            &mut headers_to_sign,
            HeaderName::from_static("x-amz-date"),
            &amz_date,
            false,
        )?;
        insert_header(
            &mut headers_to_sign,
            HeaderName::from_static("x-amz-content-sha256"),
            &payload_sha256_hex,
            false,
        )?;
        // `host` is not inserted here: `canonical_request` synthesises it
        // from `request.url` itself and ignores anything under that name in
        // the map it is handed — see `canonical_headers`'s own doc.
        if let Some(session_token) = &credentials.session_token {
            let token_value = String::from_utf8_lossy(session_token.expose_secret()).into_owned();
            // The bug this exists to catch: signing without this header
            // would still produce a valid-looking `Authorization`, and it
            // would work perfectly against static keys (which carry no
            // token to omit) and fail on every role, the moment someone
            // actually deploys one — see `a_security_token_is_sent_and_signed`
            // below.
            insert_header(
                &mut headers_to_sign,
                HeaderName::from_static("x-amz-security-token"),
                &token_value,
                true,
            )?;
        }

        let (canonical, signed_headers) = canonical_request(
            request.method,
            request.url,
            &headers_to_sign,
            &payload_sha256_hex,
        );
        let scope = credential_scope(&datestamp, &self.region, &self.service);
        let sts = string_to_sign(&amz_date, &scope, &canonical);
        let key = signing_key(
            credentials.secret_access_key.expose_secret(),
            &datestamp,
            &self.region,
            &self.service,
        );
        let signature_hex = hmac_sha256_hex(&key, &sts).map_err(|_| {
            AuthError::Config(
                "sigv4 secret access key could not be used as a signing key".to_string(),
            )
        })?;
        let authz = authorization_header(
            &credentials.access_key_id,
            &scope,
            &signed_headers,
            &signature_hex,
        );

        // What is actually returned to merge into the request: the caller
        // never sees `headers_to_sign`, only the new headers this signer
        // adds — `Authorization` plus the same three (or four) it just
        // signed, so the receiver canonicalises exactly the request that was
        // signed.
        let mut output = HeaderMap::with_capacity(if credentials.session_token.is_some() {
            4
        } else {
            3
        });
        insert_header(&mut output, AUTHORIZATION, &authz, true)?;
        insert_header(
            &mut output,
            HeaderName::from_static("x-amz-date"),
            &amz_date,
            false,
        )?;
        insert_header(
            &mut output,
            HeaderName::from_static("x-amz-content-sha256"),
            &payload_sha256_hex,
            false,
        )?;
        if let Some(session_token) = &credentials.session_token {
            let token_value = String::from_utf8_lossy(session_token.expose_secret()).into_owned();
            insert_header(
                &mut output,
                HeaderName::from_static("x-amz-security-token"),
                &token_value,
                true,
            )?;
        }
        Ok(output)
    }
}

#[async_trait]
impl Signer for SigV4Signer {
    async fn sign(&self, request: &SigningRequest<'_>) -> Result<HeaderMap, AuthError> {
        let credentials = self
            .credentials
            .get_or_refresh(|| {
                credentials::fetch(&self.source, &self.metadata, &self.imds_token_cache)
            })
            .await?;
        self.sign_at(request, &credentials, Utc::now())
    }

    fn scheme(&self) -> &'static str {
        "sigv4"
    }
}

/// An explicit override wins outright; otherwise both are inferred from
/// `target`'s host together, so a host recognised for one but overridden for
/// the other still only needs the override to name what it changes.
fn resolve_region_and_service(
    target: &url::Url,
    region_override: Option<&str>,
    service_override: Option<&str>,
) -> Result<(String, String), AuthError> {
    if let (Some(region), Some(service)) = (region_override, service_override) {
        // Both given: the host is never even consulted, so a target that
        // matches neither recognised shape — a private ALB, a test stub — is
        // still usable when the operator names both explicitly.
        return Ok((region.to_string(), service.to_string()));
    }

    if let Some((inferred_region, inferred_service)) = infer_region_and_service(target) {
        return Ok((
            region_override
                .map(str::to_string)
                .unwrap_or(inferred_region),
            service_override
                .map(str::to_string)
                .unwrap_or(inferred_service),
        ));
    }

    // At least one of the two has nothing to fall back on. Name only what is
    // actually still missing: an operator who already set one should not be
    // told to set it again, which a message naming both unconditionally
    // would do every time only the other one was left out.
    let host = target.host_str().unwrap_or("<no host>");
    let (what, hint) = match (region_override.is_some(), service_override.is_some()) {
        (true, false) => ("a service", "SigV4Config::service"),
        (false, true) => ("a region", "SigV4Config::region"),
        // `(true, true)` is handled by the early return above and never
        // reaches here; falling back to naming both, rather than panicking,
        // costs nothing and keeps this function total.
        (false, false) | (true, true) => (
            "a region and service",
            "SigV4Config::region and SigV4Config::service",
        ),
    };
    Err(AuthError::Config(format!(
        "sigv4 could not infer {what} from host '{host}'; set {hint} explicitly"
    )))
}

/// Recognises exactly two host shapes: Lambda function URLs and API Gateway.
/// `None` for anything else, including a URL with no host at all — refused
/// rather than guessed at, by [`resolve_region_and_service`]: a wrong region
/// or service yields an opaque 403 from AWS, so guessing beyond these two
/// named, unambiguous patterns would trade a clear configuration error now
/// for a confusing one in production later.
fn infer_region_and_service(target: &url::Url) -> Option<(String, String)> {
    let host = target.host_str()?;

    if let Some(region) = region_between(host, "lambda-url", "on.aws") {
        return Some((region, "lambda".to_string()));
    }
    if let Some(region) = region_between(host, "execute-api", "amazonaws.com") {
        return Some((region, "execute-api".to_string()));
    }
    None
}

/// `<anything>.<marker>.<region>.<suffix>` → `Some(<region>)`, when `host`
/// actually has that shape for some single, non-empty path segment as
/// `<region>`. `suffix` may itself contain a dot (`"amazonaws.com"`).
fn region_between(host: &str, marker: &str, suffix: &str) -> Option<String> {
    let marker_needle = format!(".{marker}.");
    let marker_at = host.find(&marker_needle)?;
    let after_marker = &host[marker_at + marker_needle.len()..];
    let region = after_marker.strip_suffix(&format!(".{suffix}"))?;
    if region.is_empty() || region.contains('.') {
        return None;
    }
    Some(region.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::http_target::{HDR_JOB_ID, HDR_TASK};
    use crate::worker::Secret;

    fn test_source() -> AwsCredentialSource {
        AwsCredentialSource::Environment
    }

    fn signer_for(config: SigV4Config, target_url: &str) -> SigV4Signer {
        let target = url::Url::parse(target_url).expect("test target parses");
        SigV4Signer::new(config, &target).expect("signer constructs")
    }

    fn test_credentials(session_token: Option<&str>) -> AwsCredentials {
        AwsCredentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: Secret::new("wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"),
            session_token: session_token.map(Secret::new),
        }
    }

    /// `2015-08-30T12:36:00Z` — the same instant `canonical.rs`'s own
    /// `DATE`/`SCOPE` constants describe, so vectors here line up with those.
    fn fixed_now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2015-08-30T12:36:00Z")
            .expect("fixture timestamp parses")
            .with_timezone(&Utc)
    }

    fn empty_request_parts() -> (url::Url, HeaderMap) {
        (
            url::Url::parse("https://example.amazonaws.com/").expect("test url parses"),
            HeaderMap::new(),
        )
    }

    /// The `SignedHeaders=` field out of a rendered `Authorization` value,
    /// as a list — shared so a test that must check *which* headers were
    /// signed always discriminates the field itself, never a substring
    /// match against the whole header that a coincidental hit elsewhere in
    /// the value could satisfy for the wrong reason.
    fn signed_header_names(authz: &str) -> Vec<&str> {
        authz
            .split("SignedHeaders=")
            .nth(1)
            .and_then(|rest| rest.split(',').next())
            .expect("SignedHeaders field present")
            .split(';')
            .collect()
    }

    #[test]
    fn a_lambda_url_host_infers_its_region_and_service() {
        let config = SigV4Config {
            source: test_source(),
            region: None,
            service: None,
        };
        let signer = signer_for(config, "https://abc123xyz.lambda-url.us-east-1.on.aws/");
        assert_eq!(signer.region, "us-east-1");
        assert_eq!(signer.service, "lambda");
    }

    #[test]
    fn an_execute_api_host_infers_its_region_and_service() {
        let config = SigV4Config {
            source: test_source(),
            region: None,
            service: None,
        };
        let signer = signer_for(
            config,
            "https://tj9n5r0m12.execute-api.us-west-2.amazonaws.com/prod",
        );
        assert_eq!(signer.region, "us-west-2");
        assert_eq!(signer.service, "execute-api");
    }

    #[test]
    fn an_unrecognised_host_is_refused_and_names_what_it_could_not_infer() {
        let config = SigV4Config {
            source: test_source(),
            region: None,
            service: None,
        };
        let target = url::Url::parse("https://example.com/hook").expect("test url parses");
        // `SigV4Signer` carries no `Debug`, so `expect_err` cannot be used
        // here; a plain match needs none.
        let result = SigV4Signer::new(config, &target);
        match result {
            Ok(_) => panic!("an unrecognised host with nothing overridden must be refused"),
            Err(AuthError::Config(message)) => {
                assert!(
                    message.contains("example.com"),
                    "error must name the host it could not infer from: {message}"
                );
                assert!(
                    message.contains("region") && message.contains("service"),
                    "error must name what it could not infer: {message}"
                );
            }
            Err(other) => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn a_partial_override_on_an_unrecognised_host_names_only_what_is_still_missing() {
        let target = url::Url::parse("https://example.com/hook").expect("test url parses");

        let region_only = SigV4Config {
            source: test_source(),
            region: Some("us-east-1".to_string()),
            service: None,
        };
        match SigV4Signer::new(region_only, &target) {
            Ok(_) => panic!("an unrecognised host with only region set must still be refused"),
            Err(AuthError::Config(message)) => {
                assert!(
                    message.contains("service"),
                    "error must name the still-missing service: {message}"
                );
                assert!(
                    !message.contains("region"),
                    "region was already given and must not be named as missing: {message}"
                );
            }
            Err(other) => panic!("expected Config, got {other:?}"),
        }

        let service_only = SigV4Config {
            source: test_source(),
            region: None,
            service: Some("custom-service".to_string()),
        };
        match SigV4Signer::new(service_only, &target) {
            Ok(_) => panic!("an unrecognised host with only service set must still be refused"),
            Err(AuthError::Config(message)) => {
                assert!(
                    message.contains("region"),
                    "error must name the still-missing region: {message}"
                );
                assert!(
                    !message.contains("service"),
                    "service was already given and must not be named as missing: {message}"
                );
            }
            Err(other) => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn an_explicit_region_and_service_override_the_host() {
        let config = SigV4Config {
            source: test_source(),
            region: Some("eu-west-1".to_string()),
            service: Some("custom-service".to_string()),
        };
        // Matches neither recognised shape; only overriding both makes it
        // usable.
        let signer = signer_for(config, "https://internal.example.com/push");
        assert_eq!(signer.region, "eu-west-1");
        assert_eq!(signer.service, "custom-service");
    }

    #[test]
    fn a_security_token_is_sent_and_signed() {
        let config = SigV4Config {
            source: test_source(),
            region: Some("us-east-1".to_string()),
            service: Some("service".to_string()),
        };
        let signer = signer_for(config, "https://example.amazonaws.com/");
        let credentials = test_credentials(Some("AQoDYXdzEPT...token"));
        let (url, headers) = empty_request_parts();
        let request = SigningRequest {
            method: "GET",
            url: &url,
            body: b"",
            headers: &headers,
        };

        let signed = signer
            .sign_at(&request, &credentials, fixed_now())
            .expect("signs");

        let token_header = signed
            .get("x-amz-security-token")
            .expect("security token header present");
        assert_eq!(
            token_header.to_str().expect("header is ascii"),
            "AQoDYXdzEPT...token"
        );

        let authz = signed
            .get(AUTHORIZATION)
            .expect("authorization header present")
            .to_str()
            .expect("header is ascii");
        let signed_headers = signed_header_names(authz);
        assert!(
            signed_headers.contains(&"x-amz-security-token"),
            "x-amz-security-token missing from SignedHeaders: {signed_headers:?}"
        );
    }

    #[test]
    fn the_content_sha256_header_is_sent_and_signed() {
        let config = SigV4Config {
            source: test_source(),
            region: Some("us-east-1".to_string()),
            service: Some("service".to_string()),
        };
        let signer = signer_for(config, "https://example.amazonaws.com/");
        let credentials = test_credentials(None);
        let url = url::Url::parse("https://example.amazonaws.com/").expect("test url parses");
        let headers = HeaderMap::new();
        let request = SigningRequest {
            method: "POST",
            url: &url,
            body: b"hello",
            headers: &headers,
        };

        let signed = signer
            .sign_at(&request, &credentials, fixed_now())
            .expect("signs");

        let expected_hash = sha256_hex(b"hello");
        let content_hash_header = signed
            .get("x-amz-content-sha256")
            .expect("content-sha256 header present");
        assert_eq!(
            content_hash_header.to_str().expect("header is ascii"),
            expected_hash
        );

        let authz = signed
            .get(AUTHORIZATION)
            .expect("authorization header present")
            .to_str()
            .expect("header is ascii");
        let signed_headers = signed_header_names(authz);
        assert!(
            signed_headers.contains(&"x-amz-content-sha256"),
            "x-amz-content-sha256 missing from SignedHeaders: {signed_headers:?}"
        );
    }

    #[test]
    fn the_flexiq_dispatch_headers_are_signed_when_present() {
        // Pins `PUSH_DISPATCH_CONTRACT.md`'s claim that SigV4 covers the
        // whole `x-flexiq-*` set: `canonical.rs`'s
        // `a_security_token_is_signed_when_present` already covers the
        // generic "a present header gets signed" property, but nothing
        // asserted it for the specific headers the contract names.
        let config = SigV4Config {
            source: test_source(),
            region: Some("us-east-1".to_string()),
            service: Some("service".to_string()),
        };
        let signer = signer_for(config, "https://example.amazonaws.com/");
        let credentials = test_credentials(None);
        let url = url::Url::parse("https://example.amazonaws.com/").expect("test url parses");
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(HDR_JOB_ID),
            "job-123".parse().expect("header value parses"),
        );
        headers.insert(
            HeaderName::from_static(HDR_TASK),
            "send_email".parse().expect("header value parses"),
        );
        let request = SigningRequest {
            method: "POST",
            url: &url,
            body: b"",
            headers: &headers,
        };

        let signed = signer
            .sign_at(&request, &credentials, fixed_now())
            .expect("signs");

        let authz = signed
            .get(AUTHORIZATION)
            .expect("authorization header present")
            .to_str()
            .expect("header is ascii");
        let signed_headers = signed_header_names(authz);
        assert!(
            signed_headers.contains(&HDR_JOB_ID),
            "{HDR_JOB_ID} missing from SignedHeaders: {signed_headers:?}"
        );
        assert!(
            signed_headers.contains(&HDR_TASK),
            "{HDR_TASK} missing from SignedHeaders: {signed_headers:?}"
        );
    }

    #[test]
    fn the_authorization_and_security_token_headers_are_sensitive() {
        let config = SigV4Config {
            source: test_source(),
            region: Some("us-east-1".to_string()),
            service: Some("service".to_string()),
        };
        let signer = signer_for(config, "https://example.amazonaws.com/");
        let credentials = test_credentials(Some("session-token-secret-value"));
        let (url, headers) = empty_request_parts();
        let request = SigningRequest {
            method: "GET",
            url: &url,
            body: b"",
            headers: &headers,
        };

        let signed = signer
            .sign_at(&request, &credentials, fixed_now())
            .expect("signs");

        assert!(signed
            .get(AUTHORIZATION)
            .expect("authorization present")
            .is_sensitive());
        assert!(signed
            .get("x-amz-security-token")
            .expect("security token present")
            .is_sensitive());

        let rendered = format!("{signed:?}");
        assert!(rendered.contains("Sensitive"));
        assert!(!rendered.contains("session-token-secret-value"));
    }

    #[test]
    fn the_date_and_the_scope_describe_one_instant() {
        let config = SigV4Config {
            source: test_source(),
            region: Some("us-east-1".to_string()),
            service: Some("service".to_string()),
        };
        let signer = signer_for(config, "https://example.amazonaws.com/");
        let credentials = test_credentials(None);
        let (url, headers) = empty_request_parts();
        let request = SigningRequest {
            method: "GET",
            url: &url,
            body: b"",
            headers: &headers,
        };

        let signed = signer
            .sign_at(&request, &credentials, fixed_now())
            .expect("signs");

        let date_header = signed
            .get("x-amz-date")
            .expect("date header present")
            .to_str()
            .expect("header is ascii")
            .to_string();
        let authz = signed
            .get(AUTHORIZATION)
            .expect("authorization header present")
            .to_str()
            .expect("header is ascii")
            .to_string();

        // The scope's datestamp is the leading 8 digits of `x-amz-date`; both
        // came from the one `timestamps(now)` call inside `sign_at`, so they
        // cannot disagree about which day it is.
        let datestamp = &date_header[..8];
        assert!(
            authz.contains(&format!(
                "Credential=AKIDEXAMPLE/{datestamp}/us-east-1/service/aws4_request"
            )),
            "date and scope disagree: {authz}"
        );
    }

    #[test]
    fn a_full_request_matches_a_pinned_vector() {
        // Independently computed — not with this crate's own code, and not a
        // direct copy of a published vector either, because no single vector
        // in AWS's published `aws-sig-v4-test-suite` covers this exact
        // header set: that suite's own `get-vanilla` family only ever signs
        // `host`+`x-amz-date`, never `x-amz-content-sha256`, which this
        // crate's `sign_at` always adds (see its own doc). Every other input
        // — host, path, date, scope, empty body, and the
        // `AKIDEXAMPLE`/`wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY` credential
        // pair — is exactly `get-vanilla`'s own, already independently pinned
        // in `canonical.rs`/`key.rs`. Only the addition of the third header
        // is new, and it is computed here by a from-scratch Python
        // reimplementation of AWS's own published canonicalisation and
        // signing-key-derivation algorithm — not by calling any function
        // this crate defines:
        //
        //   python3 -c "
        //   import hmac, hashlib
        //   def sha256_hex(b): return hashlib.sha256(b).hexdigest()
        //   def h(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
        //   date, datestamp = '20150830T123600Z', '20150830'
        //   region, service = 'us-east-1', 'service'
        //   scope = f'{datestamp}/{region}/{service}/aws4_request'
        //   akid, secret = 'AKIDEXAMPLE', 'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY'
        //   payload_hash = sha256_hex(b'')
        //   headers = {'host': 'example.amazonaws.com', 'x-amz-content-sha256': payload_hash, 'x-amz-date': date}
        //   names = sorted(headers)
        //   canonical_headers = ''.join(f'{k}:{headers[k]}\n' for k in names)
        //   signed_headers = ';'.join(names)
        //   creq = '\n'.join(['GET', '/', '', canonical_headers, signed_headers, payload_hash])
        //   sts = '\n'.join(['AWS4-HMAC-SHA256', date, scope, sha256_hex(creq.encode())])
        //   k = h(h(h(h(('AWS4' + secret).encode(), datestamp), region), service), 'aws4_request')
        //   sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
        //   print(f'AWS4-HMAC-SHA256 Credential={akid}/{scope}, SignedHeaders={signed_headers}, Signature={sig}')
        //   "
        //
        // printed: AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=726c5c4879a6b4ccbbd3b24edbd6b8826d34f87450fbbf4e85546fc7ba9c1642
        let config = SigV4Config {
            source: test_source(),
            region: Some("us-east-1".to_string()),
            service: Some("service".to_string()),
        };
        let signer = signer_for(config, "https://example.amazonaws.com/");
        let credentials = test_credentials(None);
        let url = url::Url::parse("https://example.amazonaws.com/").expect("test url parses");
        let headers = HeaderMap::new();
        let request = SigningRequest {
            method: "GET",
            url: &url,
            body: b"",
            headers: &headers,
        };

        let signed = signer
            .sign_at(&request, &credentials, fixed_now())
            .expect("signs");

        let authz = signed
            .get(AUTHORIZATION)
            .expect("authorization header present")
            .to_str()
            .expect("header is ascii");
        assert_eq!(
            authz,
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, \
             Signature=726c5c4879a6b4ccbbd3b24edbd6b8826d34f87450fbbf4e85546fc7ba9c1642"
        );
    }

    #[test]
    fn the_config_never_reaches_a_formatter() {
        // Same technique as `auth/mod.rs`'s and `oidc/mod.rs`'s own tests of
        // this name: alternating letter/digit, so every 4-character window
        // carries a digit and cannot coincide with a purely alphabetic run
        // elsewhere in the rendered `Debug`.
        let secret_value = "m1n2o3p4q5r6s7t8";
        let config = SigV4Config {
            source: AwsCredentialSource::Static {
                access_key_id: "AKIDEXAMPLE".to_string(),
                secret_access_key: Secret::new(secret_value),
                session_token: None,
            },
            region: Some("us-east-1".to_string()),
            service: Some("service".to_string()),
        };
        let rendered = format!("{config:?}");

        assert!(!rendered.contains(secret_value));
        for window in secret_value.as_bytes().windows(4) {
            let fragment = std::str::from_utf8(window).expect("secret is ascii");
            assert!(
                !rendered.contains(fragment),
                "rendered debug leaked fragment {fragment:?}: {rendered}"
            );
        }
        // The access key id is not itself secret and must still print, or a
        // redaction that swallowed the whole struct would not be caught by
        // the assertions above alone.
        assert!(rendered.contains("AKIDEXAMPLE"));
    }
}
