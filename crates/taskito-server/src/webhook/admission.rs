//! The slice of `admission.k8s.io/v1` this webhook speaks.
//!
//! Hand-written rather than pulled from a Kubernetes client crate: the request
//! is one envelope around a pod and the response is one envelope around a
//! patch, and a client library would drag in an API surface — and a release
//! cadence — for two structs.
//!
//! `apiVersion` and `kind` are echoed rather than hardcoded, because the API
//! server rejects a response whose envelope does not match the request it sent.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An `AdmissionReview` arriving from the API server.
#[derive(Debug, Deserialize)]
pub struct AdmissionReview {
    #[serde(rename = "apiVersion")]
    pub api_version: Option<String>,
    pub kind: Option<String>,
    pub request: Option<AdmissionRequest>,
}

/// The request half. Only the fields this webhook reads are modelled.
#[derive(Debug, Deserialize)]
pub struct AdmissionRequest {
    /// Correlation id the response must echo.
    pub uid: String,
    /// The object under admission — a pod, given the rules the chart installs.
    #[serde(default)]
    pub object: Value,
    /// Namespace the pod is being created in, for logging.
    #[serde(default)]
    pub namespace: Option<String>,
}

/// The response half.
#[derive(Debug, Serialize)]
pub struct AdmissionReviewResponse {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub response: AdmissionResponse,
}

/// The verdict for one request.
#[derive(Debug, Serialize)]
pub struct AdmissionResponse {
    pub uid: String,
    pub allowed: bool,
    #[serde(rename = "patchType", skip_serializing_if = "Option::is_none")]
    pub patch_type: Option<String>,
    /// Base64 of the RFC 6902 patch, which is how the API expects it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<AdmissionStatus>,
}

/// Why a pod was refused. Surfaces verbatim in the `kubectl` error, so it is
/// the only place an operator learns a annotation was wrong.
#[derive(Debug, Serialize)]
pub struct AdmissionStatus {
    pub code: u16,
    pub message: String,
}

/// The envelope version to answer with when the request carried none.
const DEFAULT_API_VERSION: &str = "admission.k8s.io/v1";
const DEFAULT_KIND: &str = "AdmissionReview";

impl AdmissionReviewResponse {
    /// Admit `uid` unchanged.
    pub fn allow(review: &AdmissionReview, uid: String) -> Self {
        Self::wrap(
            review,
            AdmissionResponse {
                uid,
                allowed: true,
                patch_type: None,
                patch: None,
                status: None,
            },
        )
    }

    /// Admit `uid` with a patch applied.
    pub fn patch(
        review: &AdmissionReview,
        uid: String,
        patch: &[Value],
    ) -> serde_json::Result<Self> {
        use base64::Engine;

        let encoded = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(patch)?);
        Ok(Self::wrap(
            review,
            AdmissionResponse {
                uid,
                allowed: true,
                patch_type: Some("JSONPatch".to_string()),
                patch: Some(encoded),
                status: None,
            },
        ))
    }

    /// Refuse `uid`, telling the operator why.
    pub fn deny(review: &AdmissionReview, uid: String, message: String) -> Self {
        Self::wrap(
            review,
            AdmissionResponse {
                uid,
                allowed: false,
                patch_type: None,
                patch: None,
                status: Some(AdmissionStatus { code: 400, message }),
            },
        )
    }

    fn wrap(review: &AdmissionReview, response: AdmissionResponse) -> Self {
        Self {
            api_version: review
                .api_version
                .clone()
                .unwrap_or_else(|| DEFAULT_API_VERSION.to_string()),
            kind: review
                .kind
                .clone()
                .unwrap_or_else(|| DEFAULT_KIND.to_string()),
            response,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn review() -> AdmissionReview {
        serde_json::from_value(json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": { "uid": "abc-123", "namespace": "prod", "object": {} },
        }))
        .expect("parse")
    }

    #[test]
    fn a_request_is_parsed_down_to_the_fields_we_read() {
        let review = review();
        let request = review.request.expect("a request");
        assert_eq!(request.uid, "abc-123");
        assert_eq!(request.namespace.as_deref(), Some("prod"));
    }

    #[test]
    fn an_allow_echoes_the_envelope_and_the_uid() {
        let review = review();
        let response = AdmissionReviewResponse::allow(&review, "abc-123".to_string());
        assert_eq!(response.api_version, "admission.k8s.io/v1");
        assert_eq!(response.kind, "AdmissionReview");
        assert!(response.response.allowed);
        assert!(response.response.patch.is_none());
    }

    #[test]
    fn a_patch_is_base64_of_the_operations() {
        use base64::Engine;

        let review = review();
        let ops = vec![json!({ "op": "add", "path": "/spec/containers/-", "value": {} })];
        let response =
            AdmissionReviewResponse::patch(&review, "abc-123".to_string(), &ops).expect("encode");
        assert_eq!(response.response.patch_type.as_deref(), Some("JSONPatch"));

        let encoded = response.response.patch.expect("a patch");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("valid base64");
        assert_eq!(
            serde_json::from_slice::<Vec<Value>>(&decoded).expect("valid json"),
            ops
        );
    }

    #[test]
    fn a_denial_carries_the_reason() {
        let review = review();
        let response = AdmissionReviewResponse::deny(
            &review,
            "abc-123".to_string(),
            "taskito.dev/attach is required".to_string(),
        );
        assert!(!response.response.allowed);
        let status = response.response.status.expect("a status");
        assert_eq!(status.code, 400);
        assert!(status.message.contains("taskito.dev/attach"));
    }

    #[test]
    fn a_missing_envelope_falls_back_to_v1() {
        let review: AdmissionReview =
            serde_json::from_value(json!({ "request": { "uid": "x", "object": {} } }))
                .expect("parse");
        let response = AdmissionReviewResponse::allow(&review, "x".to_string());
        assert_eq!(response.api_version, DEFAULT_API_VERSION);
        assert_eq!(response.kind, DEFAULT_KIND);
    }
}
