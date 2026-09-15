//! SigV4 signing-key derivation: four chained HMACs.

use super::super::digest::hmac_sha256;

/// Derive the SigV4 signing key for one date, region and service.
///
/// Four chained HMACs, each step's **raw** output keying the next:
/// `HMAC(HMAC(HMAC(HMAC("AWS4" + secret, datestamp), region), service), "aws4_request")`.
pub(crate) fn signing_key(secret: &[u8], datestamp: &str, region: &str, service: &str) -> [u8; 32] {
    let mut prefixed_secret = Vec::with_capacity(4 + secret.len());
    prefixed_secret.extend_from_slice(b"AWS4");
    prefixed_secret.extend_from_slice(secret);

    let k_date = hmac_sha256(&prefixed_secret, datestamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

/// The credential scope a signature is bound to:
/// `<datestamp>/<region>/<service>/aws4_request`.
pub(crate) fn credential_scope(datestamp: &str, region: &str, service: &str) -> String {
    format!("{datestamp}/{region}/{service}/aws4_request")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::auth::digest::hex_lower;

    #[test]
    fn the_signing_key_matches_aws_published_example() {
        // AWS's own worked example for SigV4 signing-key derivation
        // (docs.aws.amazon.com "Create a signed AWS API request", the
        // classic 20150830 / us-east-1 / iam / AKIDEXAMPLE example), for
        // secret `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`.
        //
        // Independently computed before being pinned here, rather than
        // trusted from the brief that named it:
        //
        //   python3 -c "
        //   import hmac, hashlib
        //   secret = b'AWS4' + b'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY'
        //   kDate = hmac.new(secret, b'20150830', hashlib.sha256).digest()
        //   kRegion = hmac.new(kDate, b'us-east-1', hashlib.sha256).digest()
        //   kService = hmac.new(kRegion, b'iam', hashlib.sha256).digest()
        //   kSigning = hmac.new(kService, b'aws4_request', hashlib.sha256).digest()
        //   print(kSigning.hex())
        //   "
        //
        // printed: c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9
        //
        // Cross-checked a second, independent way: smithy-lang/smithy-rs's
        // own `aws-sigv4` crate pins, in
        // `aws/rust-runtime/aws-sigv4/src/sign/v4.rs`'s
        // `test_signature_calculation`, the signature produced by HMAC-SHA256
        // of a fixed canonical-request string under this exact key, for this
        // exact secret/date/region/service. Reproducing that HMAC here with
        // the value below as the key:
        //
        //   python3 -c "
        //   import hmac, hashlib
        //   k_signing = bytes.fromhex('c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9')
        //   creq = ('AWS4-HMAC-SHA256\n20150830T123600Z\n'
        //           '20150830/us-east-1/iam/aws4_request\n'
        //           'f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59')
        //   print(hmac.new(k_signing, creq.encode(), hashlib.sha256).hexdigest())
        //   "
        //
        // printed: 5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7
        //
        // — matching smithy-rs's own pinned `expected` for that test exactly:
        // an independent codebase's independently-computed key agrees.
        let signing = signing_key(
            b"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex_lower(&signing),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn the_credential_scope_is_four_slash_joined_fields() {
        assert_eq!(
            credential_scope("20150830", "us-east-1", "iam"),
            "20150830/us-east-1/iam/aws4_request"
        );
    }

    #[test]
    fn a_different_service_derives_a_different_key() {
        // Same secret, date and region as the pinned AWS example; only the
        // service changes. Not itself a published vector — just a guard
        // against a copy-paste that ignores one of the four inputs.
        let iam_key = signing_key(
            b"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        let lambda_key = signing_key(
            b"wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "lambda",
        );
        assert_ne!(iam_key, lambda_key);
    }
}
