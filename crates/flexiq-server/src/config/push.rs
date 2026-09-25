//! Where the scheduler pushes a claimed job, and the credential it presents
//! while doing it.
//!
//! GitHub issues #843/#844: instead of waiting for an executor to attach, the
//! scheduler can POST a claimed job straight to an operator-configured
//! endpoint. `flexiq-core` already ships the dispatcher
//! (`flexiq_core::HttpDispatchTarget`), the egress guard that pins DNS at
//! connect and denies by default, and all four outbound-auth schemes #844
//! names. This module reads a push target out of the environment and
//! validates what it can there; `runtime::push_target` turns the result into
//! the dispatcher the scheduler runs on.
//!
//! **`auth` is [`PushAuthConfig`], not `flexiq_core::OutboundAuth`.**
//! `OutboundAuth` lives behind the `http-target` cargo feature, and this
//! module has to compile in every build — exactly as
//! [`super::grpc::GrpcConfig`] compiles without the `grpc` feature.
//! `PushAuthConfig` is a feature-free description of what the operator asked
//! for, converted into an `OutboundAuth` by the `From` impl further down,
//! which is itself behind the gate. `flexiq_core::net::Allowlist` has no such
//! split — it is unconditional — so [`PushTargetConfig`] holds one directly.
//!
//! **Not exposed here, deliberately: two of OIDC's five credential sources
//! (`OAuth2ClientCredentials`, `File`) and one of SigV4's five
//! (`Static`).** Each needs several correlated fields — a token URL, a
//! client id, a secret, a scope and an auth style for OAuth2; an access key,
//! a secret key and a session token for a static AWS credential — and an env
//! surface for them deserves its own design rather than a handful more
//! `FLEXIQ_PUSH_TARGET_*` variables bolted on here. `flexiq-core`'s library
//! API supports all five of each; this is a decision about this module's
//! surface, not a limitation of the core.

use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use flexiq_core::net::Allowlist;
use flexiq_core::Secret;

// Reused rather than reimplemented: the same "a guessable secret is not a
// control" floor applies to a bearer token or an HMAC secret as it does to
// the attach token.
use crate::config::listen::secret;
use crate::config::{value, Env};

/// The variable that names the target and turns the section on.
pub const URL_VAR: &str = "FLEXIQ_PUSH_TARGET_URL";
/// Jobs the target may run at once.
pub const CAPACITY_VAR: &str = "FLEXIQ_PUSH_TARGET_CAPACITY";
/// Comma-separated hosts and CIDRs the target's resolved address must match.
pub const ALLOW_VAR: &str = "FLEXIQ_PUSH_TARGET_ALLOW";
/// Seconds one dispatch may take before it is abandoned as failed.
pub const TIMEOUT_VAR: &str = "FLEXIQ_PUSH_TARGET_TIMEOUT";
/// Seconds the connection may take to establish.
pub const CONNECT_TIMEOUT_VAR: &str = "FLEXIQ_PUSH_TARGET_CONNECT_TIMEOUT";
/// Seconds shutdown waits for in-flight dispatches before abandoning them.
pub const DRAIN_VAR: &str = "FLEXIQ_PUSH_TARGET_DRAIN";
/// Ceiling on one job's request body, in bytes.
pub const MAX_REQUEST_BYTES_VAR: &str = "FLEXIQ_PUSH_TARGET_MAX_REQUEST_BYTES";
/// Ceiling on one response body read back, in bytes.
pub const MAX_RESPONSE_BYTES_VAR: &str = "FLEXIQ_PUSH_TARGET_MAX_RESPONSE_BYTES";
/// Whether a `202 Accepted` hands the job off to be settled later, and over
/// what. `off` (the default) or `grpc`.
pub const SETTLE_VAR: &str = "FLEXIQ_PUSH_TARGET_SETTLE";
/// Which outbound-auth scheme to sign dispatches with.
pub const AUTH_VAR: &str = "FLEXIQ_PUSH_TARGET_AUTH";
/// Bearer secret, for `AUTH_VAR=bearer`.
pub const TOKEN_VAR: &str = "FLEXIQ_PUSH_TARGET_TOKEN";
/// HMAC-SHA256 signing secret, for `AUTH_VAR=hmac`.
pub const HMAC_SECRET_VAR: &str = "FLEXIQ_PUSH_TARGET_HMAC_SECRET";
/// Key identifier sent alongside an HMAC signature.
pub const HMAC_KEY_ID_VAR: &str = "FLEXIQ_PUSH_TARGET_HMAC_KEY_ID";
/// Which OIDC credential source to fetch from, for `AUTH_VAR=oidc`.
pub const OIDC_SOURCE_VAR: &str = "FLEXIQ_PUSH_TARGET_OIDC_SOURCE";
/// The `aud` claim the receiver checks, for `AUTH_VAR=oidc`.
pub const OIDC_AUDIENCE_VAR: &str = "FLEXIQ_PUSH_TARGET_OIDC_AUDIENCE";
/// A user-assigned Azure identity's client id.
pub const AZURE_CLIENT_ID_VAR: &str = "FLEXIQ_PUSH_TARGET_AZURE_CLIENT_ID";
/// A user-assigned Azure identity's object id.
pub const AZURE_OBJECT_ID_VAR: &str = "FLEXIQ_PUSH_TARGET_AZURE_OBJECT_ID";
/// A user-assigned Azure identity's resource id.
pub const AZURE_MSI_RES_ID_VAR: &str = "FLEXIQ_PUSH_TARGET_AZURE_MSI_RES_ID";
/// Which AWS credential source to sign with, for `AUTH_VAR=sigv4`.
pub const AWS_SOURCE_VAR: &str = "FLEXIQ_PUSH_TARGET_AWS_SOURCE";
/// Overrides the region SigV4 infers from the target URL.
pub const AWS_REGION_VAR: &str = "FLEXIQ_PUSH_TARGET_AWS_REGION";
/// Overrides the service SigV4 infers from the target URL.
pub const AWS_SERVICE_VAR: &str = "FLEXIQ_PUSH_TARGET_AWS_SERVICE";

/// Names the push targets of a process that pushes to more than one. Each
/// name reads its settings from `FLEXIQ_PUSH_<NAME>_*`.
pub const TARGETS_VAR: &str = "FLEXIQ_PUSH_TARGETS";

/// The prefix every single-target setting is spelt with, and the fallback a
/// named target reads a setting from when it does not set its own.
const TARGET_PREFIX: &str = "FLEXIQ_PUSH_TARGET_";

/// A name no target may take: `FLEXIQ_PUSH_TARGET_URL` is already the
/// single-target variable, so a target called `target` could not be told
/// from it.
const RESERVED_TARGET_NAMES: [&str; 2] = ["TARGET", "TARGETS"];

/// One minute.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// Five seconds.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Thirty seconds.
const DEFAULT_SHUTDOWN_DRAIN: Duration = Duration::from_secs(30);
/// 8 MiB.
const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
/// 1 MiB.
const DEFAULT_MAX_RESPONSE_BYTES: usize = 1024 * 1024;

/// Variables this path refuses outright rather than reading.
///
/// Neither is a missing feature waiting for a later commit — both are
/// deliberately absent, because honouring either would put a hole in the
/// guard the rest of this module builds.
const UNHONOURED_VARS: [(&str, &str); 2] = [
    (
        "FLEXIQ_PUSH_TARGET_ALLOW_PRIVATE",
        "there is no private-network escape hatch on this path. Put the network on \
         FLEXIQ_PUSH_TARGET_ALLOW as a CIDR instead; loopback, link-local and cloud \
         metadata are refused whatever the allowlist says.",
    ),
    (
        "FLEXIQ_PUSH_TARGET_PROXY",
        "push dispatch does not honour a proxy. A proxy resolves the target itself, \
         which moves the lookup outside the guard that pins DNS at connect.",
    ),
];

/// A push target the scheduler POSTs claimed jobs to, and the guards that
/// keep the socket from becoming an SSRF pivot.
///
/// What an operator asked for. `runtime::push_target` builds the dispatcher
/// from it at boot, and a target that will not construct stops the process
/// there rather than dead-lettering jobs later.
#[derive(Debug, Clone)]
pub struct PushTargetConfig {
    /// The URL the scheduler POSTs a claimed job to.
    pub url: String,
    /// Jobs the target may run at once. A push target advertises no slots of
    /// its own, so this is the only number the scheduler has to size
    /// dispatch concurrency by.
    pub capacity: u32,
    /// Hosts and CIDRs the target's resolved address must match. Deny by
    /// default — see `Allowlist`'s own doc.
    pub allow: Allowlist,
    /// How long one dispatch may take before it is abandoned as failed.
    pub request_timeout: Duration,
    /// How long the connection may take to establish.
    pub connect_timeout: Duration,
    /// How long shutdown waits for in-flight dispatches before abandoning
    /// them.
    pub shutdown_drain: Duration,
    /// Ceiling on the request body the scheduler will build for one job.
    pub max_request_bytes: usize,
    /// Ceiling on the response body the scheduler will read back.
    pub max_response_bytes: usize,
    /// How the scheduler proves to the target that it is the scheduler.
    pub auth: PushAuthConfig,
    /// Whether a `202 Accepted` hands the job off to be settled later.
    ///
    /// Off by default. A `202` has always been a refusal that dead-letters in
    /// one attempt with a greppable reason, so turning it into a wait silently
    /// would change a shipped promise — and the bug it would hide is a
    /// framework answering `202` by default, which is exactly what the
    /// mandatory outcome header exists to catch.
    pub settle_callbacks: bool,
}

/// What the operator asked for the scheduler to authenticate a push with —
/// feature-free by design, see this module's doc for why.
#[derive(Clone)]
pub enum PushAuthConfig {
    /// Send nothing.
    None,
    /// A static `Authorization: Bearer <secret>`.
    Bearer {
        /// The bearer secret.
        token: Secret,
    },
    /// HMAC-SHA256 over the request, with an optional key identifier.
    Hmac {
        /// The signing secret.
        secret: Secret,
        /// Sent alongside the signature so a receiver with more than one key
        /// knows which one verifies it.
        key_id: Option<String>,
    },
    /// A signed identity token, refreshed before it expires.
    Oidc {
        /// Which of the exposed credential sources to fetch from.
        source: PushOidcSource,
        /// The value the receiver's endpoint will check the token's `aud`
        /// against.
        audience: String,
    },
    /// AWS Signature Version 4.
    SigV4 {
        /// Which of AWS's credential sources to sign with.
        source: PushAwsSource,
        /// Overrides the region inferred from the target URL.
        region: Option<String>,
        /// Overrides the service inferred from the target URL.
        service: Option<String>,
    },
}

/// The OIDC credential sources this surface exposes — three of
/// `flexiq_core`'s five. See this module's doc for the two left out and why.
#[derive(Clone, Debug)]
pub enum PushOidcSource {
    /// GCE, GKE or Cloud Run's metadata server.
    GoogleMetadata,
    /// Azure IMDS — VMs, VMSS, AKS.
    AzureImds {
        /// A user-assigned identity's client id. At most one of `client_id`,
        /// `object_id` and `msi_res_id` may be set.
        client_id: Option<String>,
        /// A user-assigned identity's object id. See `client_id`'s doc.
        object_id: Option<String>,
        /// A user-assigned identity's Azure resource id. See `client_id`'s
        /// doc.
        msi_res_id: Option<String>,
    },
    /// Azure App Service, Functions or Container Apps' per-instance sidecar.
    AzureAppService,
}

/// The AWS credential sources this surface exposes — four of
/// `flexiq_core`'s five. See this module's doc for the one left out and why.
#[derive(Clone, Debug)]
pub enum PushAwsSource {
    /// Environment → container → IMDSv2, the AWS SDK's own default order.
    DefaultChain,
    /// `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`.
    Environment,
    /// ECS and EKS Pod Identity.
    ContainerCredentials,
    /// AWS's own EC2 instance metadata service, version 2 only.
    Imdsv2,
}

impl std::fmt::Debug for PushAuthConfig {
    /// Hand-written, not derived, so a secret field can never start printing
    /// itself just because a variant gained one — the redaction is a
    /// property of this impl, not of whatever type the field happens to be
    /// today. Proved by a test rather than a comment.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushAuthConfig::None => f.write_str("None"),
            PushAuthConfig::Bearer { .. } => f.write_str("Bearer { token: <redacted> }"),
            PushAuthConfig::Hmac { key_id, .. } => f
                .debug_struct("Hmac")
                .field("secret", &"<redacted>")
                .field("key_id", key_id)
                .finish(),
            PushAuthConfig::Oidc { source, audience } => f
                .debug_struct("Oidc")
                .field("source", source)
                .field("audience", audience)
                .finish(),
            PushAuthConfig::SigV4 {
                source,
                region,
                service,
            } => f
                .debug_struct("SigV4")
                .field("source", source)
                .field("region", region)
                .field("service", service)
                .finish(),
        }
    }
}

/// The feature-free description turned into the credential `flexiq-core`
/// actually signs with.
///
/// Here rather than in `runtime/`, beside the types it converts, so the two
/// stay adjacent when a source is added: a new [`PushOidcSource`] arm and the
/// `flexiq_core::http::auth::OidcSource` it maps onto are then one screen
/// apart instead of one module apart.
#[cfg(feature = "http-target")]
impl From<PushAuthConfig> for flexiq_core::OutboundAuth {
    fn from(auth: PushAuthConfig) -> Self {
        use flexiq_core::http::auth::{HmacConfig, OidcConfig, SigV4Config};
        use flexiq_core::OutboundAuth;

        match auth {
            PushAuthConfig::None => OutboundAuth::None,
            PushAuthConfig::Bearer { token } => OutboundAuth::Bearer(token),
            PushAuthConfig::Hmac { secret, key_id } => {
                OutboundAuth::Hmac(HmacConfig { key_id, secret })
            }
            PushAuthConfig::Oidc { source, audience } => OutboundAuth::Oidc(OidcConfig {
                source: source.into(),
                audience,
            }),
            PushAuthConfig::SigV4 {
                source,
                region,
                service,
            } => OutboundAuth::SigV4(SigV4Config {
                source: source.into(),
                region,
                service,
            }),
        }
    }
}

#[cfg(feature = "http-target")]
impl From<PushOidcSource> for flexiq_core::http::auth::OidcSource {
    fn from(source: PushOidcSource) -> Self {
        use flexiq_core::http::auth::OidcSource;

        match source {
            PushOidcSource::GoogleMetadata => OidcSource::GoogleMetadata,
            PushOidcSource::AzureImds {
                client_id,
                object_id,
                msi_res_id,
            } => OidcSource::AzureImds {
                client_id,
                object_id,
                msi_res_id,
            },
            PushOidcSource::AzureAppService => OidcSource::AzureAppService,
        }
    }
}

#[cfg(feature = "http-target")]
impl From<PushAwsSource> for flexiq_core::http::auth::AwsCredentialSource {
    fn from(source: PushAwsSource) -> Self {
        use flexiq_core::http::auth::AwsCredentialSource;

        match source {
            PushAwsSource::DefaultChain => AwsCredentialSource::DefaultChain,
            PushAwsSource::Environment => AwsCredentialSource::Environment,
            PushAwsSource::ContainerCredentials => AwsCredentialSource::ContainerCredentials,
            PushAwsSource::Imdsv2 => AwsCredentialSource::Imdsv2,
        }
    }
}

/// Parse the push-target block, or `None` when the section is disabled.
pub fn from_env(env: &Env) -> Result<Option<PushTargetConfig>> {
    let Some(url) = value(env, URL_VAR) else {
        return Ok(None);
    };

    // A binary without the feature has no dispatcher to send it through.
    // Ignoring the variable would leave a deployment that looks configured
    // and never POSTs a single job.
    if !cfg!(feature = "http-target") {
        bail!(
            "{URL_VAR} is set, but this binary was built without the `http-target` \
             cargo feature and has no push dispatcher to send it through. Rebuild \
             with `--features http-target`, or unset {URL_VAR}."
        );
    }

    for (name, guidance) in UNHONOURED_VARS {
        if value(env, name).is_some() {
            bail!("{name} is set, but this build does not read it — {guidance}");
        }
    }

    let capacity = capacity(env)?;
    let allow = allow(env)?;
    let request_timeout = seconds(
        env,
        TIMEOUT_VAR,
        DEFAULT_REQUEST_TIMEOUT,
        "a zero request budget has expired before the first byte goes out, so every dispatch \
         fails as a timeout against a target that was never given a chance to answer",
    )?;
    let connect_timeout = seconds(
        env,
        CONNECT_TIMEOUT_VAR,
        DEFAULT_CONNECT_TIMEOUT,
        "a zero connect budget has expired before the handshake starts, so no dispatch ever \
         reaches the target at all",
    )?;
    let shutdown_drain = seconds(
        env,
        DRAIN_VAR,
        DEFAULT_SHUTDOWN_DRAIN,
        "a zero drain gives an in-flight dispatch no time to settle either before or after the \
         abandon signal, so shutdown aborts every one of them and leaves their leases to the \
         stale-job reaper rather than failing them retryably. Use 1 for a near-immediate drain \
         that still settles",
    )?;
    let max_request_bytes = bytes(
        env,
        MAX_REQUEST_BYTES_VAR,
        DEFAULT_MAX_REQUEST_BYTES,
        "a zero request cap is smaller than the smallest payload there is, so every \
         dispatch is refused as oversized before it is sent — and that refusal is not \
         retryable, so every job dead-letters",
    )?;
    let max_response_bytes = bytes(
        env,
        MAX_RESPONSE_BYTES_VAR,
        DEFAULT_MAX_RESPONSE_BYTES,
        "a zero response cap makes any answer with a body oversized, so a target that \
         replies with anything at all fails its job non-retryably however it actually \
         went",
    )?;
    let auth = parse_auth(env)?;
    let settle_callbacks = parse_settle(env)?;

    Ok(Some(PushTargetConfig {
        url,
        capacity,
        allow,
        request_timeout,
        connect_timeout,
        shutdown_drain,
        max_request_bytes,
        max_response_bytes,
        auth,
        settle_callbacks,
    }))
}

/// Every push target this process dispatches to.
#[derive(Debug, Clone)]
pub struct PushConfig {
    /// One entry for the single-target shape, one per name otherwise.
    pub targets: Vec<NamedPushTarget>,
}

impl PushConfig {
    /// Whether any target accepts a `202` and waits for a settle callback.
    pub fn settle_callbacks(&self) -> bool {
        self.targets
            .iter()
            .any(|target| target.config.settle_callbacks)
    }

    /// Whether the targets were named in [`TARGETS_VAR`], rather than being
    /// the one `FLEXIQ_PUSH_TARGET_URL` describes.
    pub fn is_named(&self) -> bool {
        self.targets.iter().any(|target| target.name.is_some())
    }

    /// Every queue a named target serves, in declaration order. Empty for the
    /// single-target shape, which serves `FLEXIQ_QUEUES`.
    pub fn queues(&self) -> Vec<String> {
        self.targets
            .iter()
            .flat_map(|target| target.queues.iter().flatten().cloned())
            .collect()
    }
}

/// One push target, and the queues it serves when there is more than one.
#[derive(Debug, Clone)]
pub struct NamedPushTarget {
    /// The name from [`TARGETS_VAR`], lowercased. `None` for the single
    /// target `FLEXIQ_PUSH_TARGET_URL` describes.
    pub name: Option<String>,
    /// The queues this target serves, from `FLEXIQ_PUSH_<NAME>_QUEUES`.
    /// `None` for the single target, which serves `FLEXIQ_QUEUES`.
    pub queues: Option<Vec<String>>,
    /// Where, how and how much.
    pub config: PushTargetConfig,
}

/// Read the push targets this process dispatches to: the single one
/// `FLEXIQ_PUSH_TARGET_URL` names, or every one [`TARGETS_VAR`] names.
///
/// A named target reads each setting from `FLEXIQ_PUSH_<NAME>_<SETTING>` and
/// falls back to `FLEXIQ_PUSH_TARGET_<SETTING>`, so what every target shares —
/// an allowlist, a timeout, an auth scheme — is written once. It is parsed by
/// [`from_env`] over that merged view, so a named target is validated exactly
/// as the single one is, and refused with its name attached.
pub fn targets_from_env(env: &Env) -> Result<Option<PushConfig>> {
    let Some(raw) = value(env, TARGETS_VAR) else {
        return Ok(from_env(env)?.map(|config| PushConfig {
            targets: vec![NamedPushTarget {
                name: None,
                queues: None,
                config,
            }],
        }));
    };
    if value(env, URL_VAR).is_some() {
        bail!(
            "{TARGETS_VAR} and {URL_VAR} cannot both be set — with named targets, each              one's URL is FLEXIQ_PUSH_<NAME>_URL. Move {URL_VAR} onto a named target, or              unset {TARGETS_VAR} for a single target."
        );
    }

    let names = target_names(&raw)?;
    let mut targets = Vec::with_capacity(names.len());
    let mut owners: Vec<(String, String)> = Vec::new();
    for name in names {
        let prefix = format!("FLEXIQ_PUSH_{name}_");
        let url_var = format!("{prefix}URL");
        let queues_var = format!("{prefix}QUEUES");
        let lower = name.to_ascii_lowercase();

        if value(env, &url_var).is_none() {
            bail!("{url_var} is required: {TARGETS_VAR} names a target called {lower}");
        }
        let queues = queue_list(env, &queues_var).with_context(|| {
            format!(
                "{queues_var} is required: each named target serves its own queues, so                  two targets never claim the same job"
            )
        })?;
        for queue in &queues {
            if let Some((_, other)) = owners.iter().find(|(owned, _)| owned == queue) {
                bail!(
                    "queue {queue} is served by both push target {other} and push target                      {lower}. A queue belongs to one target, or the two would race each                      other for its jobs."
                );
            }
            owners.push((queue.clone(), lower.clone()));
        }

        let merged = target_view(env, &prefix);
        let config = from_env(&merged)
            .with_context(|| {
                format!(
                    "push target {lower} (read from {prefix}*, falling back to                      {TARGET_PREFIX}*)"
                )
            })?
            .expect("the URL was checked above, so the target is configured");
        targets.push(NamedPushTarget {
            name: Some(lower),
            queues: Some(queues),
            config,
        });
    }
    Ok(Some(PushConfig { targets }))
}

/// The names in [`TARGETS_VAR`], uppercased as their variables spell them.
fn target_names(raw: &str) -> Result<Vec<String>> {
    let mut names: Vec<String> = Vec::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        // Letters and digits only. An underscore would let two names spell the
        // same variable: target `a` with setting `CONNECT_TIMEOUT` and target
        // `a_connect` with setting `TIMEOUT` are both FLEXIQ_PUSH_A_CONNECT_TIMEOUT.
        if !entry.chars().all(|c| c.is_ascii_alphanumeric()) {
            bail!(
                "{TARGETS_VAR}: '{entry}' is not a target name — use letters and digits                  only, since the name is spelt into FLEXIQ_PUSH_<NAME>_* variables"
            );
        }
        let upper = entry.to_ascii_uppercase();
        if RESERVED_TARGET_NAMES.contains(&upper.as_str()) {
            bail!(
                "{TARGETS_VAR}: '{entry}' is reserved — FLEXIQ_PUSH_{upper}_* is already                  the single-target spelling. Choose another name."
            );
        }
        if names.contains(&upper) {
            bail!("{TARGETS_VAR} names '{entry}' twice");
        }
        names.push(upper);
    }
    if names.is_empty() {
        bail!("{TARGETS_VAR} is set but names no target");
    }
    Ok(names)
}

/// A comma-separated queue list, blanks dropped. `None` when unset or empty.
fn queue_list(env: &Env, key: &str) -> Option<Vec<String>> {
    let queues: Vec<String> = value(env, key)?
        .split(',')
        .map(str::trim)
        .filter(|queue| !queue.is_empty())
        .map(str::to_string)
        .collect();
    (!queues.is_empty()).then_some(queues)
}

/// The environment one named target is parsed from: every
/// `FLEXIQ_PUSH_<NAME>_<SETTING>` copied over `FLEXIQ_PUSH_TARGET_<SETTING>`.
fn target_view(env: &Env, prefix: &str) -> Env {
    let mut merged = env.clone();
    for (key, raw) in env {
        if let Some(setting) = key.strip_prefix(prefix) {
            merged.insert(format!("{TARGET_PREFIX}{setting}"), raw.clone());
        }
    }
    merged
}

/// Read [`SETTLE_VAR`]: `off` (the default) or `grpc`.
///
/// Spelled as a transport rather than a boolean because it names *where* a
/// target reports, and there is exactly one answer today. A `true` would have
/// to be reinterpreted the day there is a second, and a wire this specific
/// should not be renegotiated through a flag that means "yes".
fn parse_settle(env: &Env) -> Result<bool> {
    let Some(raw) = env.get(SETTLE_VAR) else {
        return Ok(false);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "off" => Ok(false),
        "grpc" => Ok(true),
        other => bail!(
            "{SETTLE_VAR}={other} is not a settle transport. Use `grpc` to accept a 202 and \
             wait for a callback on the executor door, or `off` to refuse one."
        ),
    }
}

/// Jobs the target may run at once — required, because nothing about a push
/// target's own wire traffic reveals it.
fn capacity(env: &Env) -> Result<u32> {
    let raw = value(env, CAPACITY_VAR).with_context(|| {
        format!(
            "{CAPACITY_VAR} is required — a push target announces no slots of its \
             own, so capacity cannot be inferred from anything it sends. Set it to \
             the number of jobs the target may run at once."
        )
    })?;
    let parsed: u32 = raw
        .parse()
        .with_context(|| format!("{CAPACITY_VAR} must be a whole number, got '{raw}'"))?;
    if parsed == 0 {
        bail!(
            "{CAPACITY_VAR} must be greater than zero — a target with no capacity \
             can run no jobs"
        );
    }
    Ok(parsed)
}

/// The allowlist a resolved dispatch address must match — required, because a
/// guard whose default is derived from the value it guards is not a guard.
fn allow(env: &Env) -> Result<Allowlist> {
    let raw = value(env, ALLOW_VAR).with_context(|| {
        format!(
            "{ALLOW_VAR} is required — a guard whose default is derived from the \
             value it guards is not a guard. List every host and CIDR the \
             scheduler may dispatch a job to."
        )
    })?;
    Allowlist::parse(&raw).map_err(|error| anyhow!("{ALLOW_VAR}: {error}"))
}

/// Read a whole number of seconds, or `default` when the variable is unset.
///
/// Zero is refused for every caller. None of these is a "no limit" switch —
/// each is a deadline, and a deadline of zero has already passed by the time
/// anything is measured against it, so it does not disable the budget, it
/// fails every use of it. `zero_means` says what that failure would look
/// like, because the failure itself carries no explanation: a dispatch that
/// times out instantly looks exactly like a target that never answered.
///
/// Shared with the events drain, so every drain variable reads the same way.
pub(crate) fn seconds(
    env: &Env,
    key: &str,
    default: Duration,
    zero_means: &str,
) -> Result<Duration> {
    let Some(raw) = value(env, key) else {
        return Ok(default);
    };
    let parsed: u64 = raw
        .parse()
        .with_context(|| format!("{key} must be a whole number of seconds, got '{raw}'"))?;
    if parsed == 0 {
        bail!("{key} must be greater than zero — {zero_means}");
    }
    Ok(Duration::from_secs(parsed))
}

/// Read a whole number of bytes, or `default` when the variable is unset.
///
/// Zero is refused, for the reason [`seconds`] refuses it: a cap of zero is
/// not "no cap", it is a cap nothing can satisfy, and what an operator sees is
/// every job failing for a reason that does not mention the setting they
/// changed. `zero_means` says which failure it would be.
fn bytes(env: &Env, key: &str, default: usize, zero_means: &str) -> Result<usize> {
    let Some(raw) = value(env, key) else {
        return Ok(default);
    };
    let parsed: usize = raw
        .parse()
        .with_context(|| format!("{key} must be a whole number of bytes, got '{raw}'"))?;
    if parsed == 0 {
        bail!("{key} must be greater than zero — {zero_means}");
    }
    Ok(parsed)
}

/// Parse `AUTH_VAR` and the fields the chosen scheme needs.
fn parse_auth(env: &Env) -> Result<PushAuthConfig> {
    let kind = value(env, AUTH_VAR).unwrap_or_else(|| "none".to_string());
    match kind.as_str() {
        "none" => Ok(PushAuthConfig::None),
        "bearer" => {
            let token = secret(env, TOKEN_VAR)?
                .with_context(|| format!("{TOKEN_VAR} is required when {AUTH_VAR}=bearer"))?;
            Ok(PushAuthConfig::Bearer { token })
        }
        "hmac" => {
            let hmac_secret = secret(env, HMAC_SECRET_VAR)?
                .with_context(|| format!("{HMAC_SECRET_VAR} is required when {AUTH_VAR}=hmac"))?;
            let key_id = value(env, HMAC_KEY_ID_VAR);
            Ok(PushAuthConfig::Hmac {
                secret: hmac_secret,
                key_id,
            })
        }
        "oidc" => {
            let source = parse_oidc_source(env)?;
            let audience = value(env, OIDC_AUDIENCE_VAR).with_context(|| {
                format!(
                    "{OIDC_AUDIENCE_VAR} is required when {AUTH_VAR}=oidc — every \
                     source this surface exposes rejects a request with no audience"
                )
            })?;
            Ok(PushAuthConfig::Oidc { source, audience })
        }
        "sigv4" => {
            let source = parse_aws_source(env)?;
            let region = value(env, AWS_REGION_VAR);
            let service = value(env, AWS_SERVICE_VAR);
            Ok(PushAuthConfig::SigV4 {
                source,
                region,
                service,
            })
        }
        other => bail!("{AUTH_VAR} must be one of none, bearer, hmac, oidc, sigv4 — got '{other}'"),
    }
}

/// Parse `OIDC_SOURCE_VAR` and, for `azure-imds`, its identity selector.
fn parse_oidc_source(env: &Env) -> Result<PushOidcSource> {
    let raw = value(env, OIDC_SOURCE_VAR).with_context(|| {
        format!(
            "{OIDC_SOURCE_VAR} is required when {AUTH_VAR}=oidc — set it to google, \
             azure-imds or azure-app-service"
        )
    })?;
    match raw.as_str() {
        "google" => Ok(PushOidcSource::GoogleMetadata),
        "azure-imds" => {
            let client_id = value(env, AZURE_CLIENT_ID_VAR);
            let object_id = value(env, AZURE_OBJECT_ID_VAR);
            let msi_res_id = value(env, AZURE_MSI_RES_ID_VAR);
            let selected = [
                client_id.is_some(),
                object_id.is_some(),
                msi_res_id.is_some(),
            ]
            .into_iter()
            .filter(|set| *set)
            .count();
            if selected > 1 {
                bail!(
                    "at most one of {AZURE_CLIENT_ID_VAR}, {AZURE_OBJECT_ID_VAR} or \
                     {AZURE_MSI_RES_ID_VAR} may be set — Azure IMDS takes one identity \
                     selector, and more than one leaves it to guess which identity to \
                     fetch a token for"
                );
            }
            Ok(PushOidcSource::AzureImds {
                client_id,
                object_id,
                msi_res_id,
            })
        }
        "azure-app-service" => Ok(PushOidcSource::AzureAppService),
        other => bail!(
            "{OIDC_SOURCE_VAR} must be one of google, azure-imds, azure-app-service — \
             got '{other}'"
        ),
    }
}

/// Parse `AWS_SOURCE_VAR`, defaulting to the SDK's own chain order.
fn parse_aws_source(env: &Env) -> Result<PushAwsSource> {
    let raw = value(env, AWS_SOURCE_VAR).unwrap_or_else(|| "default-chain".to_string());
    match raw.as_str() {
        "default-chain" => Ok(PushAwsSource::DefaultChain),
        "environment" => Ok(PushAwsSource::Environment),
        "container" => Ok(PushAwsSource::ContainerCredentials),
        "imds" => Ok(PushAwsSource::Imdsv2),
        other => bail!(
            "{AWS_SOURCE_VAR} must be one of default-chain, environment, container, \
             imds — got '{other}'"
        ),
    }
}

/// Remove push-target secrets from the process environment once they are
/// parsed, so no later in-process read or child process sees a bearer token
/// or an HMAC secret. Mirrors
/// `listen::scrub_attach_token` — one `remove_var` per secret rather than a
/// shared `fn scrub_vars(names: &[&str])`: two literal calls already read as
/// clearly as a loop over a two-element array would, and keeping this
/// function's body the same shape as `scrub_attach_token`'s is worth more
/// than the line saved by generalising two call sites.
pub fn scrub_push_target_secrets() {
    // Called once from `main`, before any thread that reads the environment
    // has been spawned.
    std::env::remove_var(TOKEN_VAR);
    std::env::remove_var(HMAC_SECRET_VAR);
    // A named target's secrets, which `targets_from_env` read the same way.
    // A name that fails validation was refused before this runs.
    let named = std::env::var(TARGETS_VAR).unwrap_or_default();
    for name in named.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let upper = name.to_ascii_uppercase();
        std::env::remove_var(format!("FLEXIQ_PUSH_{upper}_TOKEN"));
        std::env::remove_var(format!("FLEXIQ_PUSH_{upper}_HMAC_SECRET"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Env {
        pairs
            .iter()
            .map(|(key, val)| (key.to_string(), val.to_string()))
            .collect()
    }

    /// A minimal valid section: just enough to enable it.
    #[cfg(feature = "http-target")]
    const BASE: [(&str, &str); 3] = [
        (URL_VAR, "https://push.example.com/hook"),
        (CAPACITY_VAR, "10"),
        (ALLOW_VAR, "push.example.com"),
    ];

    /// Long enough to pass `listen::secret`'s length floor.
    const TOKEN: &str = "0123456789abcdef";

    #[test]
    fn no_target_variable_disables_the_section() {
        assert!(from_env(&env(&[])).expect("valid").is_none());
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_url_a_capacity_and_an_allowlist_are_enough() {
        let config = from_env(&env(&BASE)).expect("valid").expect("configured");
        assert_eq!(config.url, "https://push.example.com/hook");
        assert_eq!(config.capacity, 10);
        assert!(config.allow.permits_host("push.example.com"));
        // Asserted against the literals the brief specifies, not against the
        // `DEFAULT_*` constants under test: pinning to the constant would
        // still pass if the constant itself were wrong, proving only that the
        // wiring moved a value, not that the value is the specified one.
        assert_eq!(config.request_timeout, Duration::from_secs(60));
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.shutdown_drain, Duration::from_secs(30));
        assert_eq!(config.max_request_bytes, 8 * 1024 * 1024);
        assert_eq!(config.max_response_bytes, 1024 * 1024);
        assert!(matches!(config.auth, PushAuthConfig::None));
    }

    /// No push tunable is a "no limit" switch. For a duration, zero is a
    /// deadline that has already passed; for a byte cap, zero is a ceiling
    /// nothing fits under. Either way what an operator sees is every job
    /// failing for a reason that does not mention the setting they changed,
    /// so all five are refused at boot instead.
    #[cfg(feature = "http-target")]
    #[test]
    fn a_zero_tunable_is_refused_and_says_what_it_would_have_done() {
        for (key, expected) in [
            (TIMEOUT_VAR, "timeout"),
            (CONNECT_TIMEOUT_VAR, "handshake"),
            (DRAIN_VAR, "reaper"),
            (MAX_REQUEST_BYTES_VAR, "oversized"),
            (MAX_RESPONSE_BYTES_VAR, "oversized"),
        ] {
            let mut pairs = BASE.to_vec();
            pairs.push((key, "0"));
            let error = from_env(&env(&pairs)).expect_err("zero must be refused");

            let message = format!("{error:#}");
            assert!(message.contains(key), "must name the variable: {message}");
            assert!(
                message.contains(expected),
                "must say what zero would have done, got: {message}"
            );
        }
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn the_smallest_workable_tunable_is_accepted() {
        // The floor is zero, not some larger "sensible" number: a tight
        // budget is an operator's call, an impossible one is not. Same for a
        // one-byte cap — absurd, but it is a cap, and refusing it would be
        // this module inventing a policy rather than rejecting an
        // impossibility.
        let mut pairs = BASE.to_vec();
        pairs.extend([
            (TIMEOUT_VAR, "1"),
            (CONNECT_TIMEOUT_VAR, "1"),
            (DRAIN_VAR, "1"),
            (MAX_REQUEST_BYTES_VAR, "1"),
            (MAX_RESPONSE_BYTES_VAR, "1"),
        ]);
        let config = from_env(&env(&pairs))
            .expect("one second is tight but possible")
            .expect("configured");

        assert_eq!(config.request_timeout, Duration::from_secs(1));
        assert_eq!(config.connect_timeout, Duration::from_secs(1));
        assert_eq!(config.shutdown_drain, Duration::from_secs(1));
        assert_eq!(config.max_request_bytes, 1);
        assert_eq!(config.max_response_bytes, 1);
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_target_without_an_allowlist_is_refused() {
        let error =
            from_env(&env(&[(URL_VAR, BASE[0].1), (CAPACITY_VAR, "10")])).expect_err("must refuse");
        let message = error.to_string();
        assert!(message.contains(ALLOW_VAR), "unexpected message: {message}");
        assert!(
            message.contains("guard"),
            "message must explain why, not just that it is missing: {message}"
        );
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_target_without_a_capacity_is_refused() {
        let error = from_env(&env(&[
            (URL_VAR, BASE[0].1),
            (ALLOW_VAR, "push.example.com"),
        ]))
        .expect_err("must refuse");
        let message = error.to_string();
        assert!(
            message.contains(CAPACITY_VAR),
            "unexpected message: {message}"
        );
        assert!(
            message.contains("slots"),
            "message must explain why, not just that it is missing: {message}"
        );
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_capacity_of_zero_is_refused() {
        let error = from_env(&env(&[
            (URL_VAR, BASE[0].1),
            (CAPACITY_VAR, "0"),
            (ALLOW_VAR, "push.example.com"),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains(CAPACITY_VAR));
    }

    /// Every numeric tunable's parse failure has to name the variable, or an
    /// operator reads "invalid digit found in string" and has no idea which
    /// one.
    #[cfg(feature = "http-target")]
    #[test]
    fn a_tunable_that_is_not_a_number_names_its_variable() {
        for var in [
            CAPACITY_VAR,
            TIMEOUT_VAR,
            CONNECT_TIMEOUT_VAR,
            DRAIN_VAR,
            MAX_REQUEST_BYTES_VAR,
            MAX_RESPONSE_BYTES_VAR,
        ] {
            let error = from_env(&env(&[
                (URL_VAR, BASE[0].1),
                (CAPACITY_VAR, "5"),
                (ALLOW_VAR, "push.example.com"),
                (var, "lots"),
            ]))
            .expect_err("must refuse");
            assert!(
                error.to_string().contains(var),
                "unexpected message for {var}: {error}"
            );
        }
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_bad_allowlist_entry_fails_at_boot_and_names_the_entry() {
        let error = from_env(&env(&[
            (URL_VAR, BASE[0].1),
            (CAPACITY_VAR, "5"),
            (ALLOW_VAR, "not a host/8"),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains("not a host/8"));
    }

    #[cfg(not(feature = "http-target"))]
    #[test]
    fn a_build_without_the_feature_refuses_the_variable() {
        let error =
            from_env(&env(&[(URL_VAR, "https://push.example.com/hook")])).expect_err("must refuse");
        assert!(
            error.to_string().contains("`http-target` cargo feature"),
            "unexpected message: {error}"
        );
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn the_private_escape_hatch_variable_fails_loudly() {
        let (name, _) = UNHONOURED_VARS[0];
        let error =
            from_env(&env(&[BASE[0], BASE[1], BASE[2], (name, "1")])).expect_err("must refuse");
        assert!(error.to_string().contains(name));
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn the_proxy_variable_fails_loudly() {
        let (name, _) = UNHONOURED_VARS[1];
        let error = from_env(&env(&[
            BASE[0],
            BASE[1],
            BASE[2],
            (name, "http://proxy.internal:3128"),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains(name));
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn each_auth_kind_parses_and_an_unknown_one_names_what_it_accepts() {
        let none = from_env(&env(&BASE)).expect("valid").expect("configured");
        assert!(matches!(none.auth, PushAuthConfig::None));

        let bearer = from_env(&env(&[
            BASE[0],
            BASE[1],
            BASE[2],
            (AUTH_VAR, "bearer"),
            (TOKEN_VAR, TOKEN),
        ]))
        .expect("valid")
        .expect("configured");
        assert!(matches!(bearer.auth, PushAuthConfig::Bearer { .. }));

        let hmac = from_env(&env(&[
            BASE[0],
            BASE[1],
            BASE[2],
            (AUTH_VAR, "hmac"),
            (HMAC_SECRET_VAR, TOKEN),
        ]))
        .expect("valid")
        .expect("configured");
        assert!(matches!(hmac.auth, PushAuthConfig::Hmac { .. }));

        let oidc = from_env(&env(&[
            BASE[0],
            BASE[1],
            BASE[2],
            (AUTH_VAR, "oidc"),
            (OIDC_SOURCE_VAR, "google"),
            (OIDC_AUDIENCE_VAR, "https://push.example.com/hook"),
        ]))
        .expect("valid")
        .expect("configured");
        assert!(matches!(oidc.auth, PushAuthConfig::Oidc { .. }));

        let sigv4 = from_env(&env(&[BASE[0], BASE[1], BASE[2], (AUTH_VAR, "sigv4")]))
            .expect("valid")
            .expect("configured");
        assert!(matches!(sigv4.auth, PushAuthConfig::SigV4 { .. }));

        let error = from_env(&env(&[
            BASE[0],
            BASE[1],
            BASE[2],
            (AUTH_VAR, "carrier-pigeon"),
        ]))
        .expect_err("must refuse");
        let message = error.to_string();
        for accepted in ["none", "bearer", "hmac", "oidc", "sigv4"] {
            assert!(
                message.contains(accepted),
                "missing '{accepted}' in: {message}"
            );
        }
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn oidc_without_an_audience_is_refused() {
        let error = from_env(&env(&[
            BASE[0],
            BASE[1],
            BASE[2],
            (AUTH_VAR, "oidc"),
            (OIDC_SOURCE_VAR, "google"),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains(OIDC_AUDIENCE_VAR));
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn more_than_one_azure_identity_selector_is_refused() {
        let error = from_env(&env(&[
            BASE[0],
            BASE[1],
            BASE[2],
            (AUTH_VAR, "oidc"),
            (OIDC_SOURCE_VAR, "azure-imds"),
            (OIDC_AUDIENCE_VAR, "api://push"),
            (AZURE_CLIENT_ID_VAR, "client-1"),
            (AZURE_OBJECT_ID_VAR, "object-1"),
        ]))
        .expect_err("must refuse");
        let message = error.to_string();
        assert!(message.contains(AZURE_CLIENT_ID_VAR), "{message}");
        assert!(message.contains(AZURE_OBJECT_ID_VAR), "{message}");
    }

    /// The edge case sitting right next to the one above: exactly one
    /// selector set is the normal case, not a boundary, and must parse.
    #[cfg(feature = "http-target")]
    #[test]
    fn a_single_azure_identity_selector_is_accepted() {
        let config = from_env(&env(&[
            BASE[0],
            BASE[1],
            BASE[2],
            (AUTH_VAR, "oidc"),
            (OIDC_SOURCE_VAR, "azure-imds"),
            (OIDC_AUDIENCE_VAR, "api://push"),
            (AZURE_CLIENT_ID_VAR, "client-1"),
        ]))
        .expect("valid")
        .expect("configured");
        match config.auth {
            PushAuthConfig::Oidc {
                source:
                    PushOidcSource::AzureImds {
                        client_id,
                        object_id,
                        msi_res_id,
                    },
                ..
            } => {
                assert_eq!(client_id.as_deref(), Some("client-1"));
                assert!(object_id.is_none());
                assert!(msi_res_id.is_none());
            }
            other => panic!("expected AzureImds with client_id set, got {other:?}"),
        }
    }

    /// The only test in this module that touches the real process
    /// environment: every other test parses through the `Env` map, but
    /// scrubbing acts on `std::env` directly, the way
    /// `listen::scrub_attach_token` already does.
    #[test]
    fn every_secret_is_scrubbed_from_the_environment_after_parsing() {
        std::env::set_var(TOKEN_VAR, TOKEN);
        std::env::set_var(HMAC_SECRET_VAR, TOKEN);
        scrub_push_target_secrets();
        assert!(std::env::var(TOKEN_VAR).is_err());
        assert!(std::env::var(HMAC_SECRET_VAR).is_err());
    }

    #[test]
    fn a_named_target_scrubs_its_own_secrets_too() {
        std::env::set_var(TARGETS_VAR, "scrubtest");
        std::env::set_var("FLEXIQ_PUSH_SCRUBTEST_TOKEN", TOKEN);
        std::env::set_var("FLEXIQ_PUSH_SCRUBTEST_HMAC_SECRET", TOKEN);
        scrub_push_target_secrets();
        assert!(std::env::var("FLEXIQ_PUSH_SCRUBTEST_TOKEN").is_err());
        assert!(std::env::var("FLEXIQ_PUSH_SCRUBTEST_HMAC_SECRET").is_err());
        std::env::remove_var(TARGETS_VAR);
    }

    /// Two named targets sharing an allowlist and an auth scheme, each with
    /// its own URL, queues and audience.
    fn two_targets() -> Vec<(&'static str, &'static str)> {
        vec![
            (TARGETS_VAR, "orders, Billing"),
            ("FLEXIQ_PUSH_TARGET_ALLOW", ".run.app"),
            ("FLEXIQ_PUSH_TARGET_CAPACITY", "10"),
            ("FLEXIQ_PUSH_TARGET_AUTH", "oidc"),
            ("FLEXIQ_PUSH_TARGET_OIDC_SOURCE", "google"),
            ("FLEXIQ_PUSH_ORDERS_URL", "https://orders.run.app/flexiq"),
            ("FLEXIQ_PUSH_ORDERS_QUEUES", "orders,refunds"),
            ("FLEXIQ_PUSH_ORDERS_OIDC_AUDIENCE", "https://orders.run.app"),
            ("FLEXIQ_PUSH_BILLING_URL", "https://billing.run.app/flexiq"),
            ("FLEXIQ_PUSH_BILLING_QUEUES", "invoices"),
            ("FLEXIQ_PUSH_BILLING_CAPACITY", "3"),
            (
                "FLEXIQ_PUSH_BILLING_OIDC_AUDIENCE",
                "https://billing.run.app",
            ),
        ]
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn named_targets_read_their_own_settings_and_fall_back_to_the_shared_ones() {
        let push = targets_from_env(&env(&two_targets()))
            .expect("two named targets parse")
            .expect("the section is on");
        assert!(push.is_named());
        assert_eq!(push.targets.len(), 2);

        let orders = &push.targets[0];
        assert_eq!(orders.name.as_deref(), Some("orders"));
        assert_eq!(
            orders.queues.as_deref(),
            Some(&["orders".to_string(), "refunds".to_string()][..])
        );
        assert_eq!(orders.config.url, "https://orders.run.app/flexiq");
        assert_eq!(orders.config.capacity, 10, "the shared capacity applies");

        let billing = &push.targets[1];
        assert_eq!(billing.name.as_deref(), Some("billing"));
        assert_eq!(billing.config.capacity, 3, "its own capacity wins");
        match &billing.config.auth {
            PushAuthConfig::Oidc { audience, .. } => {
                assert_eq!(audience, "https://billing.run.app")
            }
            other => panic!("expected oidc, got {other:?}"),
        }

        assert_eq!(push.queues(), vec!["orders", "refunds", "invoices"]);
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_single_target_is_one_unnamed_entry() {
        let push = targets_from_env(&env(&[
            (URL_VAR, "https://push.example.com/hook"),
            (CAPACITY_VAR, "2"),
            (ALLOW_VAR, "push.example.com"),
        ]))
        .expect("parses")
        .expect("the section is on");
        assert!(!push.is_named());
        assert_eq!(push.targets.len(), 1);
        assert!(push.targets[0].name.is_none() && push.targets[0].queues.is_none());
        assert!(push.queues().is_empty());
    }

    fn refusal(pairs: &[(&str, &str)]) -> String {
        format!(
            "{:#}",
            targets_from_env(&env(pairs)).expect_err("must refuse")
        )
    }

    #[test]
    fn named_targets_and_the_single_url_are_refused_together() {
        let mut pairs = two_targets();
        pairs.push((URL_VAR, "https://push.example.com/hook"));
        let error = refusal(&pairs);
        assert!(
            error.contains(TARGETS_VAR) && error.contains(URL_VAR),
            "{error}"
        );
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_named_target_needs_its_url_and_its_queues() {
        let without_url: Vec<_> = two_targets()
            .into_iter()
            .filter(|(key, _)| *key != "FLEXIQ_PUSH_BILLING_URL")
            .collect();
        assert!(refusal(&without_url).contains("FLEXIQ_PUSH_BILLING_URL"));

        let without_queues: Vec<_> = two_targets()
            .into_iter()
            .filter(|(key, _)| *key != "FLEXIQ_PUSH_ORDERS_QUEUES")
            .collect();
        assert!(refusal(&without_queues).contains("FLEXIQ_PUSH_ORDERS_QUEUES"));
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_queue_belongs_to_one_target() {
        let mut pairs = two_targets();
        pairs.retain(|(key, _)| *key != "FLEXIQ_PUSH_BILLING_QUEUES");
        pairs.push(("FLEXIQ_PUSH_BILLING_QUEUES", "invoices, refunds"));
        let error = refusal(&pairs);
        assert!(
            error.contains("refunds") && error.contains("orders") && error.contains("billing"),
            "{error}"
        );
    }

    #[test]
    fn a_target_name_must_be_letters_and_digits_and_not_reserved() {
        assert!(refusal(&[(TARGETS_VAR, "a_b")]).contains("letters and digits"));
        assert!(refusal(&[(TARGETS_VAR, "target")]).contains("reserved"));
        assert!(refusal(&[(TARGETS_VAR, "a, A")]).contains("twice"));
        assert!(refusal(&[(TARGETS_VAR, " , ")]).contains("names no target"));
    }

    #[cfg(feature = "http-target")]
    #[test]
    fn a_named_targets_bad_setting_is_refused_with_its_name() {
        let mut pairs = two_targets();
        pairs.push(("FLEXIQ_PUSH_BILLING_TIMEOUT", "0"));
        let error = refusal(&pairs);
        assert!(error.contains("push target billing"), "{error}");
        // The refused variables are refused under a named target's spelling
        // too, not only the shared one.
        let mut pairs = two_targets();
        pairs.push(("FLEXIQ_PUSH_ORDERS_PROXY", "http://proxy:3128"));
        assert!(refusal(&pairs).contains("push target orders"));
    }

    /// Every sliding window of the secret is absent from the formatted
    /// output, not just the whole value — a formatter that echoed half the
    /// secret would still be a leak.
    fn assert_no_secret_leak(secret_value: &str, rendered: &str) {
        assert!(!rendered.contains(secret_value));
        for window in secret_value.as_bytes().windows(4) {
            let fragment = std::str::from_utf8(window).expect("secret is ascii");
            assert!(
                !rendered.contains(fragment),
                "rendered debug leaked fragment {fragment:?}: {rendered}"
            );
        }
    }

    #[test]
    fn no_secret_reaches_a_formatter() {
        // The negative check alone (`assert_no_secret_leak`) would still pass
        // if the whole struct printed a fixed placeholder for every variant —
        // e.g. `f.write_str("<all redacted>")` regardless of shape. These
        // positive assertions prove real, non-secret data survives alongside
        // the redaction, so the impl is field-scoped rather than wholesale.
        let bearer_secret = "a1b2c3d4e5f6g7h8";
        let bearer = PushAuthConfig::Bearer {
            token: Secret::new(bearer_secret),
        };
        let bearer_rendered = format!("{bearer:?}");
        assert!(
            bearer_rendered.contains("Bearer"),
            "the variant name is not secret and must still print: {bearer_rendered}"
        );
        assert_no_secret_leak(bearer_secret, &bearer_rendered);

        let hmac_secret = "h8g7f6e5d4c3b2a1";
        let config = PushTargetConfig {
            url: "https://push.example.com/hook".to_string(),
            capacity: 5,
            allow: Allowlist::parse("push.example.com").expect("test allowlist parses"),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            shutdown_drain: DEFAULT_SHUTDOWN_DRAIN,
            max_request_bytes: DEFAULT_MAX_REQUEST_BYTES,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            auth: PushAuthConfig::Hmac {
                secret: Secret::new(hmac_secret),
                key_id: Some("key-1".to_string()),
            },
            settle_callbacks: false,
        };
        let rendered = format!("{config:?}");
        assert!(
            rendered.contains("push.example.com"),
            "the url is not secret and must still print: {rendered}"
        );
        assert!(
            rendered.contains("key-1"),
            "key_id is not secret and must still print: {rendered}"
        );
        assert_no_secret_leak(hmac_secret, &rendered);
    }
}
