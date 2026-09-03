//! Reading the committed contract, for the tests that hold this door to it.
//!
//! Both of the facade's drift checks ask the same artifact the same kind of
//! question — *what does `flexiq.v1` actually say?* — so they ask it through
//! one reader. The artifact is `contracts/descriptor.binpb`: what buf lints,
//! what `buf breaking` gates, what `build.rs` generates the Rust types from and
//! what the server hands out over reflection. A test that consulted anything
//! else would be checking a copy.

use std::collections::BTreeSet;

use prost::Message as _;
use prost_types::{FileDescriptorProto, FileDescriptorSet};

use crate::grpc::pb;

/// The `flexiq.v1` package, whose RPCs the facade must cover in full.
pub const PRODUCER_PACKAGE: &str = "flexiq.v1";

/// The executor package, which it must not cover at all.
pub const EXECUTOR_PACKAGE: &str = "flexiq.executor.v1";

/// One RPC, as the contract declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rpc {
    /// The service it belongs to, unqualified.
    pub service: String,
    /// The method name, as the `.proto` spells it.
    pub method: String,
    /// Whether the method declares `idempotency_level = NO_SIDE_EFFECTS`.
    pub no_side_effects: bool,
}

fn files() -> Vec<FileDescriptorProto> {
    FileDescriptorSet::decode(pb::FILE_DESCRIPTOR_SET)
        .expect("contracts/descriptor.binpb is a FileDescriptorSet")
        .file
}

/// Every RPC declared in `package`.
pub fn rpcs(package: &str) -> Vec<Rpc> {
    use prost_types::method_options::IdempotencyLevel;

    let mut found = Vec::new();
    for file in files() {
        if file.package() != package {
            continue;
        }
        for service in &file.service {
            for method in &service.method {
                found.push(Rpc {
                    service: service.name().to_string(),
                    method: method.name().to_string(),
                    no_side_effects: method.options.as_ref().is_some_and(|options| {
                        options.idempotency_level() == IdempotencyLevel::NoSideEffects
                    }),
                });
            }
        }
    }
    found
}

/// Every JSON field name of one `flexiq.v1` message.
///
/// `json_name` is what a JSON writer must emit and a JSON reader must accept;
/// buf fills it in, and a field that somehow arrived without one falls back to
/// the lowerCamelCase form the specification derives.
pub fn json_names(message: &str) -> BTreeSet<String> {
    for file in files() {
        if file.package() != PRODUCER_PACKAGE {
            continue;
        }
        for declared in &file.message_type {
            if declared.name() == message {
                return declared
                    .field
                    .iter()
                    .map(|field| {
                        let json_name = field.json_name();
                        if json_name.is_empty() {
                            lower_camel_case(field.name())
                        } else {
                            json_name.to_string()
                        }
                    })
                    .collect();
            }
        }
    }
    panic!("{PRODUCER_PACKAGE}.{message} is not in the committed descriptor");
}

/// The specification's own derivation: drop each underscore and capitalise the
/// letter that followed it.
fn lower_camel_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut capitalise = false;
    for character in name.chars() {
        if character == '_' {
            capitalise = true;
        } else if capitalise {
            out.extend(character.to_uppercase());
            capitalise = false;
        } else {
            out.push(character);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_producer_package_declares_the_rpcs_the_service_implements() {
        let rpcs = rpcs(PRODUCER_PACKAGE);
        assert!(!rpcs.is_empty(), "the descriptor carries no producer RPCs");
        assert!(rpcs.iter().all(|rpc| rpc.service == "ProducerService"));
        assert!(rpcs
            .iter()
            .any(|rpc| rpc.method == "GetJob" && rpc.no_side_effects));
        assert!(rpcs
            .iter()
            .any(|rpc| rpc.method == "Enqueue" && !rpc.no_side_effects));
    }

    #[test]
    fn a_message_reports_the_json_names_the_contract_gives_it() {
        let names = json_names("QueueStatsResponse");
        assert!(names.contains("pending") && names.contains("cancelled"));
        assert!(json_names("Job").contains("taskName"));
    }

    #[test]
    fn the_camel_case_fallback_matches_the_specification() {
        assert_eq!(lower_camel_case("task_name"), "taskName");
        assert_eq!(lower_camel_case("id"), "id");
        assert_eq!(lower_camel_case("result_ttl"), "resultTtl");
    }
}
