//! The `google.api.http` option on each RPC — the one thing `prost` will not
//! read for us.
//!
//! `google.api.http` is an *extension* of `google.protobuf.MethodOptions`, and
//! prost drops extension fields on decode: `prost_types::MethodOptions` comes
//! back with nothing in it. So the descriptor is decoded a second time through
//! the mirror types below, which carry the descriptor's own field numbers down
//! to the one extension tag, 72295728. Nothing else in the crate needs this —
//! every other read goes through `prost-types` unchanged.

use prost::Message as _;

use crate::Error;

/// The HTTP method a binding answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Verb {
    /// `GET`, legal only where the RPC declares `NO_SIDE_EFFECTS`.
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
    /// `PATCH`.
    Patch,
    /// `DELETE`.
    Delete,
}

impl Verb {
    /// The lowercase spelling, which is what an OpenAPI path item keys on.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Put => "put",
            Self::Patch => "patch",
            Self::Delete => "delete",
        }
    }
}

/// One route: an RPC, reachable at one path with one verb.
///
/// A binding rather than an RPC, because `additional_bindings` lets one RPC
/// have several — `QueueStats` answers both a queue's path and the namespace's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    /// The service, unqualified.
    pub service: String,
    /// The method, exactly as the `.proto` spells it.
    pub method: String,
    /// The HTTP method.
    pub verb: Verb,
    /// The path template, `{field}` for a bound field and a trailing `:verb`
    /// for a custom method.
    pub path: String,
    /// The request field the body carries: `*` for the whole message, `None`
    /// when the binding takes no body at all.
    pub body: Option<String>,
    /// `0` for the rule itself and `1..` for each `additional_bindings` entry,
    /// so an operation id derived from it is unique and does not move when an
    /// unrelated RPC is added.
    pub ordinal: usize,
}

impl Binding {
    /// The path with any custom-method suffix removed, and the field names it
    /// binds.
    ///
    /// `/v1/jobs/{job_id}:cancel` binds `job_id`; the `:cancel` is a verb on
    /// the path (AIP-136), not part of the parameter.
    pub fn path_fields(&self) -> Vec<String> {
        let mut fields = Vec::new();
        let mut rest = self.path.as_str();
        while let Some(open) = rest.find('{') {
            let Some(close) = rest[open..].find('}') else {
                break;
            };
            let inside = &rest[open + 1..open + close];
            // `{name=**}` is legal in a path template. Only the name is the
            // parameter; the pattern after it constrains what it matches.
            let name = inside.split('=').next().unwrap_or(inside);
            fields.push(name.to_string());
            rest = &rest[open + close + 1..];
        }
        fields
    }
}

/// Every binding the RPCs of `package` declare, in declaration order.
///
/// An RPC with no `google.api.http` option contributes nothing, which is how
/// `flexiq.executor.v1` stays off this door.
pub fn bindings(descriptor: &[u8], package: &str) -> Result<Vec<Binding>, Error> {
    let files = FileSet::decode(descriptor)?.file;
    let mut found = Vec::new();
    for file in files {
        if file.package() != package {
            continue;
        }
        for service in &file.service {
            for method in &service.method {
                let Some(rule) = method.options.as_ref().and_then(|o| o.http.as_ref()) else {
                    continue;
                };
                let qualified = format!("{}.{}", service.name(), method.name());
                for (ordinal, rule) in std::iter::once(rule)
                    .chain(&rule.additional_bindings)
                    .enumerate()
                {
                    let (verb, path) = pattern(rule, &qualified)?;
                    found.push(Binding {
                        service: service.name().to_string(),
                        method: method.name().to_string(),
                        verb,
                        path,
                        body: rule.body.clone().filter(|body| !body.is_empty()),
                        ordinal,
                    });
                }
            }
        }
    }
    Ok(found)
}

/// The one verb and path a rule names, or the reason it names none or several.
fn pattern(rule: &HttpRule, qualified: &str) -> Result<(Verb, String), Error> {
    let candidates = [
        (Verb::Get, &rule.get),
        (Verb::Post, &rule.post),
        (Verb::Put, &rule.put),
        (Verb::Patch, &rule.patch),
        (Verb::Delete, &rule.delete),
    ];
    let mut set = candidates
        .into_iter()
        .filter_map(|(verb, path)| path.as_ref().map(|path| (verb, path.clone())));
    match (set.next(), set.next()) {
        (Some(only), None) => Ok(only),
        _ => Err(Error::Pattern(qualified.to_string())),
    }
}

// ── The mirror types ─────────────────────────────────────────────────
//
// Each carries the descriptor's own field numbers, and only the fields on the
// way to the extension. Decoding is forward-compatible by construction: prost
// skips what these do not declare, which is everything else in the descriptor.

/// `google.protobuf.FileDescriptorSet`.
#[derive(Clone, PartialEq, prost::Message)]
struct FileSet {
    #[prost(message, repeated, tag = "1")]
    file: Vec<File>,
}

/// `google.protobuf.FileDescriptorProto`.
#[derive(Clone, PartialEq, prost::Message)]
struct File {
    #[prost(string, optional, tag = "2")]
    package: Option<String>,
    #[prost(message, repeated, tag = "6")]
    service: Vec<Service>,
}

/// `google.protobuf.ServiceDescriptorProto`.
#[derive(Clone, PartialEq, prost::Message)]
struct Service {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(message, repeated, tag = "2")]
    method: Vec<Method>,
}

/// `google.protobuf.MethodDescriptorProto`.
#[derive(Clone, PartialEq, prost::Message)]
struct Method {
    #[prost(string, optional, tag = "1")]
    name: Option<String>,
    #[prost(message, optional, tag = "4")]
    options: Option<MethodOptions>,
}

/// `google.protobuf.MethodOptions`, carrying only the extension.
///
/// 72295728 is the field number google/api/annotations.proto assigns
/// `google.api.http`, and as permanent as any other. It is spelled out here
/// because prost has no way to be told about an extension.
#[derive(Clone, PartialEq, prost::Message)]
struct MethodOptions {
    #[prost(message, optional, tag = "72295728")]
    http: Option<HttpRule>,
}

/// `google.api.HttpRule`. The five verbs are arms of one oneof, which on the
/// wire is five ordinary fields.
#[derive(Clone, PartialEq, prost::Message)]
struct HttpRule {
    #[prost(string, optional, tag = "2")]
    get: Option<String>,
    #[prost(string, optional, tag = "3")]
    put: Option<String>,
    #[prost(string, optional, tag = "4")]
    post: Option<String>,
    #[prost(string, optional, tag = "5")]
    delete: Option<String>,
    #[prost(string, optional, tag = "6")]
    patch: Option<String>,
    #[prost(string, optional, tag = "7")]
    body: Option<String>,
    #[prost(message, repeated, tag = "11")]
    additional_bindings: Vec<HttpRule>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(path: &str) -> Binding {
        Binding {
            service: "ProducerService".to_string(),
            method: "CancelJob".to_string(),
            verb: Verb::Post,
            path: path.to_string(),
            body: None,
            ordinal: 0,
        }
    }

    #[test]
    fn a_custom_method_suffix_is_not_part_of_the_parameter() {
        assert_eq!(
            binding("/v1/jobs/{job_id}:cancel").path_fields(),
            ["job_id"]
        );
    }

    #[test]
    fn a_path_with_no_parameters_binds_nothing() {
        assert!(binding("/v1/stats").path_fields().is_empty());
    }

    #[test]
    fn every_parameter_in_a_path_is_reported_in_order() {
        assert_eq!(
            binding("/v1/queues/{queue}/jobs/{job_id}").path_fields(),
            ["queue", "job_id"]
        );
    }

    #[test]
    fn a_pattern_after_a_parameter_is_not_part_of_its_name() {
        assert_eq!(binding("/v1/{name=jobs/*}").path_fields(), ["name"]);
    }

    #[test]
    fn a_rule_naming_no_verb_is_refused() {
        let rule = HttpRule::default();
        assert!(pattern(&rule, "ProducerService.Enqueue").is_err());
    }

    #[test]
    fn a_rule_naming_two_verbs_is_refused() {
        let rule = HttpRule {
            get: Some("/v1/jobs".to_string()),
            post: Some("/v1/jobs".to_string()),
            ..HttpRule::default()
        };
        assert!(pattern(&rule, "ProducerService.Enqueue").is_err());
    }
}
