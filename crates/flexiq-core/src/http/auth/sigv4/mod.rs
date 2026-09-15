//! AWS Signature Version 4 (SigV4): canonicalisation and key derivation.
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
//! dependency: every expected value in this module's tests comes from AWS's
//! own published `aws-sig-v4-test-suite`, from AWS's own worked
//! signing-key-derivation example, or from an independent codebase's test
//! fixture (smithy-lang/smithy-rs's `aws-sigv4` crate) cross-checked by hand
//! — never from this code computing its own expected answer.
//!
//! This module is the pure half: canonicalisation ([`canonical`]) and key
//! derivation ([`key`]), each `pub(crate)` and independently testable
//! against those vectors. No [`super::Signer`] implementation, no credential
//! chain, no [`super::OutboundAuth`] variant — those are a later commit,
//! deliberately split out so each commit reviews against its own vectors
//! rather than one commit's bug hiding behind another's.

pub(crate) mod canonical;
pub(crate) mod key;
