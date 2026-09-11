//! Where `fq` dials, and what it presents when it gets there.
//!
//! Two rules the rest of the crate depends on.
//!
//! The transport is chosen by the endpoint's scheme and never guessed. The
//! server terminates no TLS of its own, so a default would be wrong for one
//! deployment or the other, and being wrong would surface as a handshake error
//! naming nothing an operator can act on.
//!
//! The credential comes from the environment only. A token in `argv` is a token
//! in `ps` and in shell history, which is the position all three SDK CLIs
//! already take for their attach tokens. There is no namespace here at all: a
//! namespace is fixed when the token is minted and the gRPC role serves exactly
//! one, so a flag would be a control the server ignores.

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Uri};

use crate::pb::producer_service_client::ProducerServiceClient;

/// The only place a credential comes from.
pub const TOKEN_VAR: &str = "FLEXIQ_TOKEN";

/// How long one call may take before the client gives up.
///
/// The door's own `FLEXIQ_GRPC_REQUEST_TIMEOUT` defaults to the same 30
/// seconds, so this is the server's number rather than a tighter one invented
/// here. Without it a peer that accepts the connection and then answers
/// nothing leaves the command pending forever — tonic applies no deadline of
/// its own.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the TCP handshake may take.
///
/// Separate from [`REQUEST_TIMEOUT`] because a blackholed address never
/// completes a handshake at all, and without this the wait is the operating
/// system's, which is minutes.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// A ready client: the generated one, with the credential attached.
pub type Client = ProducerServiceClient<InterceptedService<Channel, Bearer>>;

/// Where the door is, and how to reach it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// A TCP address, with TLS when the scheme said `https`.
    Tcp {
        /// The address to dial.
        uri: Uri,
        /// Whether to negotiate TLS against the platform's root store.
        tls: bool,
    },
    /// A Unix domain socket.
    Unix(PathBuf),
}

/// Read an endpoint, refusing to guess a transport.
pub fn parse_endpoint(text: &str) -> Result<Endpoint> {
    if let Some(path) = text.strip_prefix("unix:") {
        if path.is_empty() {
            return Err(anyhow!(
                "`unix:` needs a socket path, as in unix:/run/flexiq-grpc.sock"
            ));
        }
        return Ok(Endpoint::Unix(PathBuf::from(path)));
    }
    let tls = match text.split_once("://") {
        Some(("http", _)) => false,
        Some(("https", _)) => true,
        _ => {
            return Err(anyhow!(
                "`{text}` names no transport. Write http://host:port for plaintext, \
                 https://host:port for TLS, or unix:/path/to.sock for a Unix socket"
            ))
        }
    };
    let uri = text
        .parse::<Uri>()
        .with_context(|| format!("`{text}` is not a valid URI"))?;
    Ok(Endpoint::Tcp { uri, tls })
}

/// The credential, from the environment.
pub fn token_from_env() -> Result<String> {
    token_from(std::env::var(TOKEN_VAR).ok())
}

/// The half of [`token_from_env`] that does not read the environment.
///
/// Split out so the tests do not mutate a process-wide variable other tests in
/// the same binary share.
fn token_from(value: Option<String>) -> Result<String> {
    match value {
        Some(token) if !token.trim().is_empty() => Ok(token),
        _ => Err(anyhow!(
            "no credential: set {TOKEN_VAR} to a token minted with `flexiq-server token create`"
        )),
    }
}

/// Attaches a bearer credential to every request.
///
/// A named type rather than a closure so the client it wraps has a nameable
/// type, which [`Client`] is.
#[derive(Clone)]
pub struct Bearer(MetadataValue<Ascii>);

impl Bearer {
    /// Present `token` on every call.
    pub fn new(token: &str) -> Result<Self> {
        format!("Bearer {token}")
            .parse()
            .map(Self)
            .map_err(|_| anyhow!("the token in {TOKEN_VAR} is not ASCII"))
    }
}

impl tonic::service::Interceptor for Bearer {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}

/// Dial `endpoint` and return a client that presents `token`.
pub async fn connect(endpoint: &str, token: &str) -> Result<Client> {
    let bearer = Bearer::new(token)?;
    let channel = match parse_endpoint(endpoint)? {
        Endpoint::Tcp { uri, tls } => connect_tcp(uri, tls).await,
        Endpoint::Unix(path) => connect_unix(&path).await,
    }
    .with_context(|| format!("connecting to {endpoint}"))?;

    Ok(ProducerServiceClient::with_interceptor(channel, bearer))
}

/// Dial a TCP address, negotiating TLS when the scheme asked for it.
async fn connect_tcp(uri: Uri, tls: bool) -> Result<Channel> {
    if !tls && !is_local(&uri) {
        // The credential rides this connection in a header. Over plaintext to
        // somewhere other than this machine, anything on the path can read it
        // and replay it until it expires.
        //
        // A warning rather than a refusal: the door terminates no TLS itself,
        // so `http://` to a sidecar or a mesh proxy on the same host is the
        // supported deployment, and a peer behind a TLS-terminating ingress is
        // reached as `https://`. Refusing plaintext outright would reject the
        // configuration the server documents.
        eprintln!(
            "warning: sending {TOKEN_VAR} in cleartext to {}. Use https:// or unix: unless the \
             path to this host is already private.",
            uri.host().unwrap_or("the endpoint")
        );
    }
    let mut builder = tonic::transport::Endpoint::from(uri)
        .timeout(REQUEST_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT);
    if tls {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(builder.connect().await?)
}

/// Whether `uri` names this machine.
///
/// A literal loopback address or `localhost`. A name that merely resolves to
/// loopback is treated as remote: resolution happens later and can change,
/// and a warning that depends on DNS is not one an operator can reason about.
fn is_local(uri: &Uri) -> bool {
    match uri.host() {
        Some("localhost") => true,
        // An IPv6 literal arrives bracketed.
        Some(host) => host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback()),
        None => false,
    }
}

/// Dial a Unix socket.
///
/// The URI is a placeholder: tonic needs one to build a channel and the
/// connector ignores it, because the socket path is what selects the peer.
/// `TokioIo` presents the `UnixStream` in the IO shape hyper expects.
async fn connect_unix(path: &Path) -> Result<Channel> {
    let path = path.to_path_buf();
    Ok(tonic::transport::Endpoint::try_from("http://[::1]:50051")
        .expect("a valid placeholder URI")
        .timeout(REQUEST_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .connect_with_connector(tower::service_fn(move |_: Uri| {
            let path = path.clone();
            async move {
                let stream = tokio::net::UnixStream::connect(path).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
            }
        }))
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_is_plaintext_and_https_is_not() {
        let Endpoint::Tcp { tls, .. } = parse_endpoint("http://127.0.0.1:50051").expect("parses")
        else {
            panic!("a http:// endpoint is a TCP one");
        };
        assert!(!tls);
        let Endpoint::Tcp { tls, .. } =
            parse_endpoint("https://queue.example:443").expect("parses")
        else {
            panic!("a https:// endpoint is a TCP one");
        };
        assert!(tls);
    }

    #[test]
    fn a_unix_endpoint_keeps_its_path() {
        let Endpoint::Unix(path) = parse_endpoint("unix:/run/flexiq-grpc.sock").expect("parses")
        else {
            panic!("a unix: endpoint is a socket one");
        };
        assert_eq!(path, Path::new("/run/flexiq-grpc.sock"));
    }

    /// The likeliest mistake is pasting what grpcurl takes, which has no
    /// scheme. Guessing one would silently pick plaintext or silently pick TLS,
    /// so the error names all three instead.
    #[test]
    fn a_schemeless_endpoint_is_refused_by_name() {
        let error = parse_endpoint("127.0.0.1:50051").expect_err("no scheme");
        let text = error.to_string();
        assert!(text.contains("http://"), "{text}");
        assert!(text.contains("https://"), "{text}");
        assert!(text.contains("unix:"), "{text}");
    }

    #[test]
    fn a_bare_unix_scheme_is_refused() {
        assert!(parse_endpoint("unix:").is_err());
    }

    #[test]
    fn a_missing_token_names_the_variable() {
        let error = token_from(None).expect_err("no token");
        assert!(error.to_string().contains(TOKEN_VAR));
        let error = token_from(Some("   ".to_string())).expect_err("blank token");
        assert!(error.to_string().contains(TOKEN_VAR));
    }

    /// The cleartext warning keys off this, so a wrong answer either cries
    /// wolf on every local run or stays silent on the case that matters.
    #[test]
    fn loopback_is_local_and_a_routable_address_is_not() {
        let local = |text: &str| {
            let Endpoint::Tcp { uri, .. } = parse_endpoint(text).expect("parses") else {
                panic!("a TCP endpoint");
            };
            is_local(&uri)
        };
        assert!(local("http://127.0.0.1:50051"));
        assert!(local("http://localhost:50051"));
        assert!(local("http://[::1]:50051"));
        assert!(!local("http://10.0.0.4:50051"));
        assert!(!local("http://queue.example:50051"));
    }

    #[test]
    fn a_token_becomes_a_bearer_header() {
        let bearer = Bearer::new("fqt_0123456789abcdef.secret").expect("ASCII");
        assert_eq!(
            bearer.0.to_str().expect("ASCII"),
            "Bearer fqt_0123456789abcdef.secret"
        );
    }
}
