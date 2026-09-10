//! The OpenAPI document itself.
//!
//! One binding becomes one operation. What the operation says about parameters
//! is not a choice made here — it falls out of the `google.api.http` rule the
//! way every transcoder reads it: a field named in the path template is a path
//! parameter, a `body: "*"` takes the rest as the request body, and with no
//! body the remaining fields are the query string. That is the same derivation
//! the facade's handlers implement, which is why the two can be held to each
//! other by a test.
//!
//! ## Determinism
//!
//! `scripts/proto-check.sh` compares bytes, so this may not vary for a fixed
//! descriptor. `serde_json::Map` is a `BTreeMap`, so every object is key-sorted
//! and every array is in the contract's own declaration order.

use std::collections::BTreeSet;

use serde_json::{json, Map, Value};

use crate::binding::{bindings, Binding};
use crate::descriptor::{Contract, Field};
use crate::schema::{self, Registry};
use crate::Error;

/// The package whose RPCs this door serves.
///
/// `flexiq.executor.v1` is deliberately absent and cannot be added: a worker
/// surface has different credentials, different failure modes and no reason to
/// be reachable from a browser.
pub const PRODUCER_PACKAGE: &str = "flexiq.v1";

/// The OpenAPI document for the JSON facade, as the bytes to commit.
pub fn document(descriptor: &[u8]) -> Result<String, Error> {
    let contract = Contract::load(descriptor)?;
    let bindings = bindings(descriptor, PRODUCER_PACKAGE)?;
    let mut registry = Registry::new(&contract);

    let mut paths: Map<String, Value> = Map::new();
    let mut services: BTreeSet<String> = BTreeSet::new();
    let mut requests: Vec<String> = Vec::new();
    let mut responses: Vec<String> = Vec::new();

    for binding in &bindings {
        let method = contract.method(&binding.service, &binding.method)?;
        services.insert(binding.service.clone());
        requests.push(method.input.clone());
        responses.push(method.output.clone());

        let operation = operation(&contract, &mut registry, binding)?;
        let item = paths
            .entry(binding.path.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        let Some(item) = item.as_object_mut() else {
            unreachable!("a path item is created as an object")
        };
        item.insert(binding.verb.as_str().to_string(), operation);
    }

    // A request body is refused when it carries a field the reader does not
    // know. A response is not, so a message on both sides stays open — the
    // laxer of the two answers is the one that does not break a client.
    let mut strict = schema::closure(&contract, &requests)?;
    for reachable in schema::closure(&contract, &responses)? {
        strict.remove(&reachable);
    }
    registry.deny_unknown(&strict);

    let tags: Vec<Value> = services
        .into_iter()
        .map(|service| json!({ "name": service }))
        .collect();

    let document = json!({
        "openapi": "3.1.0",
        "info": info(),
        "servers": [{
            "url": "/",
            "description": "The gRPC listener. It speaks HTTP/1.1 on the same port, so these paths are served wherever FLEXIQ_GRPC_LISTEN is bound."
        }],
        "security": [{ "bearerAuth": [] }],
        "tags": tags,
        "paths": Value::Object(paths),
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "An API token, minted by `flexiq-server token`. It carries the namespace, so no request names one."
                }
            },
            "schemas": Value::Object(registry.into_schemas().into_iter().collect())
        }
    });

    let mut rendered =
        serde_json::to_string_pretty(&document).map_err(|error| Error::Unsupported {
            element: "the document".to_string(),
            reason: format!("it would not serialise: {error}"),
        })?;
    rendered.push('\n');
    Ok(rendered)
}

/// The document's own metadata.
///
/// `version` is the contract's, not the release's. The wire contract is
/// `flexiq.v1` and stays `v1` for as long as the package does, whereas a
/// release version here would restale the committed document on every bump and
/// put it in `scripts/version.mjs`'s list for no gain.
fn info() -> Value {
    json!({
        "title": "FlexiQ producer API",
        "version": "v1",
        "summary": "Submit work, read it back, cancel it, count it.",
        "description": "The JSON facade of the FlexiQ gRPC producer door, transcoded from \
    `contracts/proto/flexiq/v1`. Every path here reaches the same service method a gRPC client \
    reaches, on the same listener and behind the same token check.\n\n\
    Requests are refused when they carry a field the contract does not declare. Responses are not \
    closed: a later release may add one.\n\n\
    A listener serves exactly one namespace and it comes from the token, so no request names one. A \
    job in another namespace is indistinguishable from a job that does not exist.\n\n\
    This document is generated. Edit `contracts/proto/flexiq/v1/producer_service.proto` and run \
    `scripts/proto-check.sh --fix`.",
        "license": { "name": "MIT", "identifier": "MIT" }
    })
}

/// One binding, as an operation.
fn operation(
    contract: &Contract,
    registry: &mut Registry<'_>,
    binding: &Binding,
) -> Result<Value, Error> {
    let method = contract.method(&binding.service, &binding.method)?;
    let input = contract.message(&method.input)?;
    let bound = binding.path_fields();

    let mut parameters: Vec<Value> = Vec::new();
    for name in &bound {
        let field = input
            .fields
            .iter()
            .find(|field| &field.name == name)
            .ok_or_else(|| Error::Undeclared(format!("{}.{name}", input.full_name)))?;
        parameters.push(parameter(registry, field, name, "path")?);
    }

    let mut operation = Map::new();
    operation.insert("operationId".to_string(), operation_id(binding).into());
    operation.insert("tags".to_string(), json!([binding.service.clone()]));
    if let Some(comment) = &method.comment {
        // The first line is what a list of operations shows; the rest is the
        // detail view. A one-line comment is both, and saying it twice in a
        // generated artifact helps nobody.
        let summary = comment.lines().next().unwrap_or(comment);
        operation.insert("summary".to_string(), summary.into());
        if summary != comment {
            operation.insert("description".to_string(), comment.as_str().into());
        }
    }

    match binding.body.as_deref() {
        Some("*") => {
            let schema = registry.reference(&method.input)?;
            operation.insert(
                "requestBody".to_string(),
                json!({
                    "required": true,
                    "content": { "application/json": { "schema": schema } }
                }),
            );
        }
        // A named body field would make the remaining fields a query string on
        // a write, which nothing in this contract does. Refusing is better than
        // describing a shape no handler implements.
        Some(named) => {
            return Err(Error::Unsupported {
                element: format!("{}.{}", binding.service, binding.method),
                reason: format!("`body: \"{named}\"` names a field; only `*` is described"),
            })
        }
        None => {
            // Everything the path did not bind is a query parameter, which is
            // exactly what the facade's readers take.
            for field in &input.fields {
                if bound.contains(&field.name) {
                    continue;
                }
                parameters.push(parameter(registry, field, &field.json_name, "query")?);
            }
        }
    }

    if !parameters.is_empty() {
        operation.insert("parameters".to_string(), Value::Array(parameters));
    }

    let success = registry.reference(&method.output)?;
    operation.insert(
        "responses".to_string(),
        json!({
            "200": {
                "description": "The call succeeded.",
                "content": { "application/json": { "schema": success } }
            },
            "default": {
                "description": "The call did not succeed. The HTTP status is a function of the \
        `google.rpc.Code` in the body, which is the finer answer and the one to branch on.",
                "content": {
                    "application/json": {
                        "schema": { "$ref": format!("#/components/schemas/{}", schema::ERROR) }
                    }
                }
            }
        }),
    );

    Ok(Value::Object(operation))
}

/// One parameter, in the path or in the query string.
fn parameter(
    registry: &mut Registry<'_>,
    field: &Field,
    name: &str,
    location: &str,
) -> Result<Value, Error> {
    let mut parameter = Map::new();
    parameter.insert("name".to_string(), name.into());
    parameter.insert("in".to_string(), location.into());
    parameter.insert("required".to_string(), (location == "path").into());
    if let Some(comment) = &field.comment {
        parameter.insert("description".to_string(), comment.as_str().into());
    }
    if field.repeated {
        parameter.insert("style".to_string(), "form".into());
        parameter.insert("explode".to_string(), true.into());
    }
    parameter.insert("schema".to_string(), registry.field_shape(field)?);
    Ok(Value::Object(parameter))
}

/// A stable, unique id for an operation.
///
/// The ordinal is what keeps a second binding on one RPC — `GET /v1/stats`
/// beside `GET /v1/queues/{queue}/stats` — from colliding with the first, and
/// it does not move when an unrelated RPC is added.
fn operation_id(binding: &Binding) -> String {
    match binding.ordinal {
        0 => format!("{}_{}", binding.service, binding.method),
        nth => format!("{}_{}_{}", binding.service, binding.method, nth + 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::Verb;

    fn binding(ordinal: usize) -> Binding {
        Binding {
            service: "ProducerService".to_string(),
            method: "QueueStats".to_string(),
            verb: Verb::Get,
            path: "/v1/stats".to_string(),
            body: None,
            ordinal,
        }
    }

    #[test]
    fn the_first_binding_of_an_rpc_is_named_after_it() {
        assert_eq!(operation_id(&binding(0)), "ProducerService_QueueStats");
    }

    #[test]
    fn a_further_binding_of_one_rpc_gets_its_own_id() {
        assert_eq!(operation_id(&binding(1)), "ProducerService_QueueStats_2");
        assert_eq!(operation_id(&binding(2)), "ProducerService_QueueStats_3");
    }
}
