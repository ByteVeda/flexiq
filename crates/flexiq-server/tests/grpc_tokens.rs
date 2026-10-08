//! End-to-end: the token RPCs on `flexiq.admin.v1` (#851), over a real socket.
//!
//! What is pinned is the no-escalation contract: only a `tokens` credential
//! reaches these methods, on either door; it mints only grants one of its own
//! covers, never past its own expiry, and only in its own namespace; it
//! revokes only what it covers, or itself; and the secret leaves the server
//! once, in the create response.
#![cfg(feature = "grpc")]

mod support;

use std::time::Duration;

use flexiq_core::storage::Storage;
use flexiq_core::AuditFilter;
use flexiq_server::config::grpc::GrpcConfig;
use flexiq_server::config::listen::ListenAddress;
use flexiq_server::grpc::pb::admin::admin_service_client::AdminServiceClient;
use flexiq_server::grpc::pb::admin::{
    ApiToken, CreateTokenRequest, GetTokenRequest, ListTokensRequest, RevokeTokenRequest,
    TokenStatus,
};
use flexiq_server::grpc::pb::producer_service_client::ProducerServiceClient;
use flexiq_server::grpc::pb::{enqueue_request, EnqueueOptions, EnqueueRequest};
use flexiq_server::grpc::status::reason;
use flexiq_server::grpc::Listener;
use flexiq_server::runtime::shutdown::Shutdown;
use flexiq_server::tokens::{store, Grants, NewToken};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tonic_types::StatusExt;

use support::{temp_storage, temp_workflows, Bearer, TempStorage};

/// The namespace this door serves.
const NAMESPACE: &str = "grpc-tokens-tests";
/// Another tenant on the same database.
const OTHER: &str = "grpc-tokens-other";

type Admin = AdminServiceClient<InterceptedService<Channel, Bearer>>;

/// A running listener over one database.
struct Harness {
    channel: Channel,
    base: String,
    storage: TempStorage,
    shutdown: Shutdown,
    served: tokio::task::JoinHandle<anyhow::Result<()>>,
}

/// A token minted straight into the store.
struct Minted {
    plaintext: String,
    id: String,
}

impl Harness {
    async fn start(label: &str) -> Self {
        let storage = temp_storage(label);
        let shutdown = Shutdown::default();
        let listener = Listener::bind(&GrpcConfig::new(
            ListenAddress::Tcp("127.0.0.1:0".parse().expect("valid address")),
            NAMESPACE,
        ))
        .await
        .expect("bind");
        let addr = listener
            .local_addr()
            .expect("a TCP listener has an address");
        let served = tokio::spawn(listener.serve(
            (*storage).clone(),
            temp_workflows(&storage),
            None,
            shutdown.clone(),
        ));
        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("a valid endpoint")
            .connect()
            .await
            .expect("the listener must accept a connection");
        Self {
            channel,
            base: format!("http://{addr}"),
            storage,
            shutdown,
            served,
        }
    }

    /// Mint `grants` in `namespace`, living `days`.
    fn mint_in(&self, namespace: &str, grants: &[&str], days: Option<i64>) -> Minted {
        let grants = Grants::parse_all(grants.iter().copied()).expect("valid grants");
        let request =
            NewToken::new("seed", grants, namespace, days, None).expect("a valid mint request");
        let (row, plaintext) = store::create(&*self.storage, request).expect("mint");
        Minted {
            plaintext,
            id: row.id,
        }
    }

    fn mint(&self, grants: &[&str]) -> Minted {
        self.mint_in(NAMESPACE, grants, None)
    }

    fn admin(&self, token: &str) -> Admin {
        AdminServiceClient::with_interceptor(self.channel.clone(), Bearer::new(token))
    }

    fn producer(&self, token: &str) -> ProducerServiceClient<InterceptedService<Channel, Bearer>> {
        ProducerServiceClient::with_interceptor(self.channel.clone(), Bearer::new(token))
    }

    async fn http(&self, request: reqwest::RequestBuilder) -> (StatusCode, Value) {
        let response = request.send().await.expect("the listener answers");
        let status = response.status();
        let body = response.json().await.expect("a JSON body");
        (status, body)
    }

    async fn stop(self) {
        self.shutdown.trigger();
        self.served
            .await
            .expect("the serve task must not panic")
            .expect("a shutdown is not an error");
    }
}

fn create(name: &str, scopes: &[&str], days: Option<i32>) -> CreateTokenRequest {
    CreateTokenRequest {
        name: name.to_string(),
        scopes: scopes.iter().map(|grant| grant.to_string()).collect(),
        expire_days: days,
    }
}

fn enqueue(queue: &str) -> EnqueueRequest {
    EnqueueRequest {
        task_name: "send".into(),
        body: Some(enqueue_request::Body::Raw(vec![1])),
        options: Some(EnqueueOptions {
            queue: queue.into(),
            ..Default::default()
        }),
    }
}

/// The `reason` and `scope` metadata a refusal carries.
fn details(status: &Status) -> (String, Option<String>) {
    let all = status.get_error_details();
    let info = all.error_info().expect("every error carries an ErrorInfo");
    (
        info.reason.clone(),
        info.metadata.get(reason::KEY_SCOPE).cloned(),
    )
}

/// The secret half of `fqt_<id>.<secret>`.
fn secret_of(token: &str) -> &str {
    token.rsplit_once('.').expect("fqt_<id>.<secret>").1
}

fn assert_no_secret(tokens: &[ApiToken], plaintext: &str) {
    let dump = format!("{tokens:?}");
    assert!(
        !dump.contains(secret_of(plaintext)),
        "a token carried the secret: {dump}"
    );
}

/// The whole life of a minted token: it works, it lists, it reads, it
/// revokes, and then it stops working.
#[tokio::test]
async fn a_minted_token_works_lists_and_stops_working_once_revoked() {
    let harness = Harness::start("tokens-lifecycle").await;
    let caller = harness.mint(&["tokens", "produce"]);
    let mut admin = harness.admin(&caller.plaintext);

    let created = admin
        .create_token(create("worker", &["produce:queue=emails"], Some(30)))
        .await
        .expect("a covered mint")
        .into_inner();
    let token = created.token.expect("the token");
    let secret = created.secret;
    assert!(
        secret.starts_with(&format!("fqt_{}.", token.id)),
        "{secret}"
    );
    assert_eq!(token.scopes, ["produce:queue=emails"]);
    assert_eq!(token.namespace, NAMESPACE);
    assert_eq!(token.status, TokenStatus::Active as i32);
    assert_eq!(
        token.created_by.as_deref(),
        Some(format!("token:{}", caller.id).as_str())
    );
    let lifetime = token.expires_at.expect("an expiry").seconds
        - token.created_at.expect("a mint time").seconds;
    assert_eq!(lifetime, 30 * 24 * 60 * 60);

    // It is a working credential, held to its grant.
    harness
        .producer(&secret)
        .enqueue(enqueue("emails"))
        .await
        .expect("the minted token enqueues where it may");
    let refused = harness
        .producer(&secret)
        .enqueue(enqueue("billing"))
        .await
        .expect_err("and nowhere else");
    assert_eq!(refused.code(), Code::PermissionDenied);

    let listed = admin
        .list_tokens(ListTokensRequest {})
        .await
        .expect("list")
        .into_inner()
        .tokens;
    let ids: Vec<&str> = listed.iter().map(|token| token.id.as_str()).collect();
    assert!(ids.contains(&token.id.as_str()) && ids.contains(&caller.id.as_str()));
    assert_no_secret(&listed, &secret);
    assert_no_secret(&listed, &caller.plaintext);

    let read = admin
        .get_token(GetTokenRequest {
            token_id: token.id.clone(),
        })
        .await
        .expect("get")
        .into_inner()
        .token
        .expect("the token");
    assert_eq!(read.name, "worker");
    assert_no_secret(std::slice::from_ref(&read), &secret);

    let revoked = admin
        .revoke_token(RevokeTokenRequest {
            token_id: token.id.clone(),
        })
        .await
        .expect("a covered revoke")
        .into_inner()
        .token
        .expect("the token");
    assert_eq!(revoked.status, TokenStatus::Revoked as i32);
    let again = admin
        .revoke_token(RevokeTokenRequest {
            token_id: token.id.clone(),
        })
        .await
        .expect("revoking twice is idempotent")
        .into_inner()
        .token
        .expect("the token");
    assert_eq!(again.revoked_at, revoked.revoked_at, "unchanged");

    let status = harness
        .producer(&secret)
        .enqueue(enqueue("emails"))
        .await
        .expect_err("a revoked token stops working");
    assert_eq!(status.code(), Code::Unauthenticated);
    harness.stop().await;
}

/// Rule 1: every requested grant must be covered by one of the caller's — the
/// same scope, and a queue and task pattern at least as wide.
#[tokio::test]
async fn a_token_cannot_mint_a_grant_it_does_not_cover() {
    let harness = Harness::start("tokens-escalation").await;
    let caller = harness.mint(&["tokens", "produce:queue=emails-*"]);
    let mut admin = harness.admin(&caller.plaintext);

    for (asked, scope) in [
        ("produce", "produce"),
        ("produce:queue=billing", "produce"),
        ("produce:queue=emails", "produce"),
        ("produce:task=send", "produce"),
        ("read", "read"),
        ("execute", "execute"),
        ("inspect", "inspect"),
        ("admin", "admin"),
    ] {
        let status = admin
            .create_token(create("wider", &["produce:queue=emails-eu", asked], None))
            .await
            .expect_err(asked);
        assert_eq!(status.code(), Code::PermissionDenied, "{asked}");
        let (why, lacked) = details(&status);
        assert_eq!(why, reason::SCOPE_DENIED, "{asked}");
        assert_eq!(lacked.as_deref(), Some(scope), "{asked}");
        assert!(
            status.message().contains(asked),
            "{asked}: {}",
            status.message()
        );
    }

    // Narrower, and a `tokens` grant it holds itself, are both fine. An
    // explicit lifetime: the default would outlive a caller minted at it.
    admin
        .create_token(create(
            "narrower",
            &[
                "produce:queue=emails-eu",
                "produce:queue=emails-us-*,task=send",
                "tokens",
            ],
            Some(30),
        ))
        .await
        .expect("covered grants mint");

    // Nothing was written by the refusals: two tokens, the seed and the mint.
    let listed = admin
        .list_tokens(ListTokensRequest {})
        .await
        .expect("list")
        .into_inner()
        .tokens;
    assert_eq!(listed.len(), 2);
    harness.stop().await;
}

/// Rule 2: a minted token expires no later than the caller — refused, never
/// shortened, so a token cannot renew itself forever.
#[tokio::test]
async fn a_token_cannot_outlive_the_one_that_minted_it() {
    let harness = Harness::start("tokens-expiry").await;
    let caller = harness.mint_in(NAMESPACE, &["tokens", "read"], Some(10));
    let mut admin = harness.admin(&caller.plaintext);

    for days in [None, Some(10), Some(365)] {
        let status = admin
            .create_token(create("long", &["read"], days))
            .await
            .expect_err("it would outlive the caller");
        assert_eq!(status.code(), Code::InvalidArgument, "{days:?}");
        assert_eq!(details(&status).0, reason::INVALID_REQUEST);
        assert!(
            status.message().contains("at most 9 days"),
            "{days:?}: {}",
            status.message()
        );
        // An omitted lifetime is reported as the default, not as a field sent.
        assert_eq!(
            status.message().contains("(the default)"),
            days.is_none(),
            "{days:?}: {}",
            status.message()
        );
    }
    let minted = admin
        .create_token(create("short", &["read"], Some(9)))
        .await
        .expect("within the caller's life")
        .into_inner()
        .token
        .expect("the token");

    let stored = store::get(&*harness.storage, &minted.id)
        .expect("read")
        .expect("present");
    let ceiling = store::get(&*harness.storage, &caller.id)
        .expect("read")
        .expect("present")
        .expires_at;
    assert!(stored.expires_at <= ceiling);
    harness.stop().await;
}

/// The model's own rules hold on this door too.
#[tokio::test]
async fn a_mint_the_model_refuses_is_an_invalid_request() {
    let harness = Harness::start("tokens-validation").await;
    let caller = harness.mint(&["tokens", "produce"]);
    let mut admin = harness.admin(&caller.plaintext);

    for request in [
        create("", &["produce"], None),
        create(&"x".repeat(65), &["produce"], None),
        create("ci", &[], None),
        create("ci", &["teleport"], None),
        create("ci", &["tokens:queue=emails"], None),
        create("ci", &["produce"], Some(0)),
        create("ci", &["produce"], Some(366)),
    ] {
        let status = admin
            .create_token(request.clone())
            .await
            .expect_err("refused");
        assert_eq!(status.code(), Code::InvalidArgument, "{request:?}");
    }
    harness.stop().await;
}

/// Rule 3: another namespace's token is absent — not listed, not readable, not
/// revocable — however wide the caller.
#[tokio::test]
async fn another_namespaces_token_is_absent() {
    let harness = Harness::start("tokens-namespace").await;
    let caller = harness.mint(&["tokens", "produce", "read", "execute", "inspect", "admin"]);
    let foreign = harness.mint_in(OTHER, &["read"], None);
    let mut admin = harness.admin(&caller.plaintext);

    let listed = admin
        .list_tokens(ListTokensRequest {})
        .await
        .expect("list")
        .into_inner()
        .tokens;
    assert!(listed.iter().all(|token| token.namespace == NAMESPACE));
    assert!(!listed.iter().any(|token| token.id == foreign.id));

    for status in [
        admin
            .get_token(GetTokenRequest {
                token_id: foreign.id.clone(),
            })
            .await
            .expect_err("not readable"),
        admin
            .revoke_token(RevokeTokenRequest {
                token_id: foreign.id.clone(),
            })
            .await
            .expect_err("not revocable"),
        admin
            .get_token(GetTokenRequest {
                token_id: "nope".to_string(),
            })
            .await
            .expect_err("never minted"),
    ] {
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(details(&status).0, reason::TOKEN_NOT_FOUND);
    }
    let still = store::get(&*harness.storage, &foreign.id)
        .expect("read")
        .expect("present");
    assert!(still.revoked_at.is_none(), "the foreign token is untouched");
    harness.stop().await;
}

/// Rule 4: a narrow `tokens` holder cannot revoke a broader token, but may
/// revoke what it covers, and itself.
#[tokio::test]
async fn a_token_revokes_only_what_it_covers_or_itself() {
    let harness = Harness::start("tokens-revoke").await;
    let caller = harness.mint(&["tokens", "produce:queue=emails-*"]);
    let broader = harness.mint(&["produce"]);
    let narrower = harness.mint(&["produce:queue=emails-eu"]);
    let operator = harness.mint(&["tokens", "admin"]);
    let mut admin = harness.admin(&caller.plaintext);

    for id in [&broader.id, &operator.id] {
        let status = admin
            .revoke_token(RevokeTokenRequest {
                token_id: id.clone(),
            })
            .await
            .expect_err("a broader token is not the caller's to revoke");
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(details(&status).0, reason::SCOPE_DENIED);
        let row = store::get(&*harness.storage, id)
            .expect("read")
            .expect("present");
        assert!(row.revoked_at.is_none());
    }

    admin
        .revoke_token(RevokeTokenRequest {
            token_id: narrower.id.clone(),
        })
        .await
        .expect("a covered token is revocable");

    admin
        .revoke_token(RevokeTokenRequest {
            token_id: caller.id.clone(),
        })
        .await
        .expect("a token may always revoke itself");
    let status = admin
        .list_tokens(ListTokensRequest {})
        .await
        .expect_err("and then it is no credential at all");
    assert_eq!(status.code(), Code::Unauthenticated);
    harness.stop().await;
}

/// `admin` does not imply `tokens`, on either door, reads included.
#[tokio::test]
async fn an_operator_token_without_tokens_reaches_no_token_method() {
    let harness = Harness::start("tokens-scope").await;
    let operator = harness.mint(&["produce", "read", "execute", "inspect", "admin"]);
    let target = harness.mint(&["read"]);
    let mut admin = harness.admin(&operator.plaintext);

    let refusals = [
        admin
            .create_token(create("x", &["read"], None))
            .await
            .expect_err("create"),
        admin
            .get_token(GetTokenRequest {
                token_id: target.id.clone(),
            })
            .await
            .expect_err("get"),
        admin
            .list_tokens(ListTokensRequest {})
            .await
            .expect_err("list"),
        admin
            .revoke_token(RevokeTokenRequest {
                token_id: target.id.clone(),
            })
            .await
            .expect_err("revoke"),
    ];
    for status in refusals {
        assert_eq!(status.code(), Code::PermissionDenied);
        assert_eq!(
            details(&status),
            (reason::SCOPE_DENIED.to_string(), Some("tokens".to_string()))
        );
    }

    // The JSON facade's `/v1/admin` split must not hand these to `admin` or
    // `inspect`.
    let client = reqwest::Client::new();
    let url = |path: &str| format!("{}{path}", harness.base);
    for request in [
        client
            .post(url("/v1/admin/tokens"))
            .json(&json!({"name": "x", "scopes": ["read"]})),
        client.get(url("/v1/admin/tokens")),
        client.get(url(&format!("/v1/admin/tokens/{}", target.id))),
        client.post(url(&format!("/v1/admin/tokens/{}:revoke", target.id))),
    ] {
        let (status, body) = harness.http(request.bearer_auth(&operator.plaintext)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["error"]["details"][0]["reason"], reason::SCOPE_DENIED);
    }
    let row = store::get(&*harness.storage, &target.id)
        .expect("read")
        .expect("present");
    assert!(row.revoked_at.is_none());
    assert_eq!(
        store::list(&*harness.storage, Some(NAMESPACE))
            .expect("list")
            .len(),
        2,
        "nothing was minted"
    );
    harness.stop().await;
}

/// The facade mints and lists exactly as gRPC does, and its listing never
/// carries the secret.
#[tokio::test]
async fn the_json_facade_mints_and_lists_without_the_secret() {
    let harness = Harness::start("tokens-facade").await;
    let caller = harness.mint(&["tokens", "read"]);
    let client = reqwest::Client::new();

    let (status, created) = harness
        .http(
            client
                .post(format!("{}/v1/admin/tokens", harness.base))
                .bearer_auth(&caller.plaintext)
                .json(&json!({"name": "dash", "scopes": ["read"], "expireDays": 7})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let secret = created["secret"]
        .as_str()
        .expect("the secret, once")
        .to_string();
    assert_eq!(created["token"]["status"], "TOKEN_STATUS_ACTIVE");
    assert_eq!(created["token"]["scopes"], json!(["read"]));

    let (status, listed) = harness
        .http(
            client
                .get(format!("{}/v1/admin/tokens", harness.base))
                .bearer_auth(&caller.plaintext),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    let rendered = listed.to_string();
    assert!(!rendered.contains(secret_of(&secret)), "{rendered}");
    assert!(
        !rendered.contains(secret_of(&caller.plaintext)),
        "{rendered}"
    );
    assert!(!rendered.contains("hash"), "{rendered}");
    assert_eq!(listed["tokens"].as_array().map(Vec::len), Some(2));

    let (status, refused) = harness
        .http(
            client
                .post(format!("{}/v1/admin/tokens", harness.base))
                .bearer_auth(&caller.plaintext)
                .json(&json!({"name": "wide", "scopes": ["produce"]})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{refused}");
    harness.stop().await;
}

/// The facade reads one token and revokes it, and the revoke is repeatable.
#[tokio::test]
async fn the_json_facade_gets_and_revokes_one_token() {
    let harness = Harness::start("tokens-facade-one").await;
    let caller = harness.mint(&["tokens", "read"]);
    let target = harness.mint(&["read"]);
    let client = reqwest::Client::new();
    let one = format!("{}/v1/admin/tokens/{}", harness.base, target.id);

    let (status, got) = harness
        .http(client.get(&one).bearer_auth(&caller.plaintext))
        .await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["token"]["id"], target.id.as_str());
    assert_eq!(got["token"]["status"], "TOKEN_STATUS_ACTIVE");
    let rendered = got.to_string();
    assert!(
        !rendered.contains(secret_of(&target.plaintext)),
        "{rendered}"
    );

    for _ in 0..2 {
        let (status, revoked) = harness
            .http(
                client
                    .post(format!("{one}:revoke"))
                    .bearer_auth(&caller.plaintext),
            )
            .await;
        assert_eq!(status, StatusCode::OK, "{revoked}");
        assert_eq!(revoked["token"]["status"], "TOKEN_STATUS_REVOKED");
    }

    let (status, missing) = harness
        .http(
            client
                .get(format!("{}/v1/admin/tokens/nope", harness.base))
                .bearer_auth(&caller.plaintext),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
    assert_eq!(
        missing["error"]["details"][0]["reason"],
        reason::TOKEN_NOT_FOUND
    );
    harness.stop().await;
}

/// An out-of-range lifetime is a 400 on the facade, as it is on gRPC.
#[tokio::test]
async fn the_json_facade_refuses_an_out_of_range_expiry() {
    let harness = Harness::start("tokens-facade-expiry").await;
    let caller = harness.mint(&["tokens", "read"]);
    let client = reqwest::Client::new();

    for days in [0, -1, 366] {
        let (status, refused) = harness
            .http(
                client
                    .post(format!("{}/v1/admin/tokens", harness.base))
                    .bearer_auth(&caller.plaintext)
                    .json(&json!({"name": "x", "scopes": ["read"], "expireDays": days})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{days}: {refused}");
        assert_eq!(
            refused["error"]["details"][0]["reason"],
            reason::INVALID_REQUEST
        );
    }
    assert_eq!(
        store::list(&*harness.storage, Some(NAMESPACE))
            .expect("list")
            .len(),
        1,
        "nothing was minted"
    );
    harness.stop().await;
}

/// A mint and a revoke are on the audit trail, naming the token — and no
/// record carries the secret.
#[tokio::test]
async fn mints_and_revokes_are_audited_without_the_secret() {
    let harness = Harness::start("tokens-audit").await;
    let caller = harness.mint(&["tokens", "read"]);
    let mut admin = harness.admin(&caller.plaintext);
    let created = admin
        .create_token(create("audited", &["read"], Some(1)))
        .await
        .expect("mint")
        .into_inner();
    let id = created.token.expect("the token").id;
    admin
        .revoke_token(RevokeTokenRequest {
            token_id: id.clone(),
        })
        .await
        .expect("revoke");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let records = loop {
        let records = harness
            .storage
            .list_audit_after(NAMESPACE, &AuditFilter::default(), 100, None)
            .expect("list the trail");
        if records.len() >= 2 {
            break records;
        }
        assert!(tokio::time::Instant::now() < deadline, "{records:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    for operation in ["CreateToken", "RevokeToken"] {
        let record = records
            .iter()
            .find(|record| record.operation.ends_with(operation))
            .unwrap_or_else(|| panic!("no {operation} record: {records:?}"));
        assert_eq!(record.target_kind.as_deref(), Some("token"));
        assert_eq!(record.target.as_deref(), Some(id.as_str()));
        assert_eq!(record.token_id, caller.id);
        assert_eq!(record.access, "write");
    }
    let dump = format!("{records:?}");
    assert!(!dump.contains(secret_of(&created.secret)), "{dump}");
    harness.stop().await;
}
