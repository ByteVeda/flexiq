//! The `tls://` attach surface as a Rust executor reaches it: through the
//! `flexiq` entry point, with the `attach-tls` feature forwarded to the core.
#![cfg(feature = "attach-tls")]

use std::time::Duration;

use flexiq::{AttachAddress, AttachTls};

#[test]
fn a_tls_address_parses_through_the_entry_point() {
    let address = AttachAddress::parse("tls://scheduler:7777").expect("parse");
    assert_eq!(address.to_string(), "tls://scheduler:7777");
}

#[test]
fn tls_material_beside_a_plaintext_address_is_refused() {
    let tls = AttachTls {
        ca: Some("ca.pem".into()),
        ..AttachTls::default()
    };
    let refused = AttachAddress::parse("127.0.0.1:1")
        .expect("parse")
        .connect_with(Duration::from_millis(100), Some(&tls));
    let Err(error) = refused else {
        panic!("a plaintext dial must not carry TLS material");
    };
    assert!(error.to_string().contains("tls://"), "{error}");
}

/// The feature is what turns a `tls://` dial from `Unsupported` into a real
/// handshake attempt: with nothing listening, it fails on the connect instead.
#[test]
fn the_feature_reaches_the_core_dialler() {
    let Err(error) = AttachAddress::parse("tls://127.0.0.1:1")
        .expect("parse")
        .connect_with(Duration::from_millis(500), None)
    else {
        panic!("nothing listens on port 1");
    };
    assert_ne!(error.kind(), std::io::ErrorKind::Unsupported, "{error}");
}
