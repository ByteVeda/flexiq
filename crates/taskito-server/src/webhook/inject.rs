//! Turn an opted-in pod into a JSON patch that adds an executor sidecar.
//!
//! The sidecar reuses the app container's own image reference, which is the
//! whole trick: the image is already on the node because the app container
//! needs it, so injection costs a process rather than a pull, and it works for
//! any language without the injector knowing which one.
//!
//! Everything here is pure — pod JSON in, patch out — so the interesting cases
//! are unit-testable without an API server.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::webhook::annotations::{self, InjectionSpec};

/// Name given to the injected container. Also the idempotency key: a pod that
/// already has one is left alone, because admission runs again on update and a
/// second sidecar would double the pod's slots without anyone asking.
pub const SIDECAR_NAME: &str = "taskito-executor";

/// Annotation stamped on a mutated pod, recording that this injector ran.
pub const INJECTED_MARKER: &str = "taskito.dev/injected";

/// One RFC 6902 operation.
pub type PatchOp = Value;

/// Build the patch for `pod`, or `None` when there is nothing to do.
///
/// `Ok(None)` covers both "did not opt in" and "already injected"; an `Err` is
/// a pod that asked for injection and described it wrongly.
pub fn patch_for(pod: &Value) -> Result<Option<Vec<PatchOp>>> {
    let annotations = read_annotations(pod);
    let Some(spec) = annotations::parse(&annotations)? else {
        return Ok(None);
    };
    if already_injected(pod) {
        return Ok(None);
    }

    let source = source_container(pod, spec.source_container.as_deref())?;
    let sidecar = build_sidecar(&spec, source)?;

    let mut ops = vec![json!({
        "op": "add",
        "path": "/spec/containers/-",
        "value": sidecar,
    })];
    ops.push(marker_op(&annotations));
    Ok(Some(ops))
}

/// Pod annotations as a plain map. A pod with none yields an empty map rather
/// than an error — that is simply a pod that did not opt in.
fn read_annotations(pod: &Value) -> std::collections::BTreeMap<String, String> {
    pod.pointer("/metadata/annotations")
        .and_then(Value::as_object)
        .map(|map| {
            map.iter()
                .filter_map(|(key, value)| {
                    value.as_str().map(|text| (key.clone(), text.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn already_injected(pod: &Value) -> bool {
    containers(pod)
        .iter()
        .any(|container| container.get("name").and_then(Value::as_str) == Some(SIDECAR_NAME))
}

fn containers(pod: &Value) -> Vec<&Value> {
    pod.pointer("/spec/containers")
        .and_then(Value::as_array)
        .map(|list| list.iter().collect())
        .unwrap_or_default()
}

/// The container whose image and environment the sidecar copies.
fn source_container<'a>(pod: &'a Value, wanted: Option<&str>) -> Result<&'a Value> {
    let containers = containers(pod);
    if containers.is_empty() {
        bail!("the pod has no containers to copy an image from");
    }
    match wanted {
        // Named explicitly: a miss is a typo, and falling back to the first
        // container would inject against the wrong image without saying so.
        Some(name) => containers
            .into_iter()
            .find(|container| container.get("name").and_then(Value::as_str) == Some(name))
            .with_context(|| {
                format!(
                    "{} names container '{name}', which this pod does not have",
                    annotations::CONTAINER
                )
            }),
        None => Ok(containers[0]),
    }
}

/// Assemble the sidecar container spec.
fn build_sidecar(spec: &InjectionSpec, source: &Value) -> Result<Value> {
    let image = source
        .get("image")
        .and_then(Value::as_str)
        .with_context(|| {
            format!(
                "container '{}' has no image to copy",
                source
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("<unnamed>")
            )
        })?;

    let mut container = json!({
        "name": SIDECAR_NAME,
        "image": image,
        "command": spec.command,
        "env": environment(spec, source),
    });

    // The app's own image pull policy applies: the sidecar resolves the very
    // same reference, so a different policy could pull a different digest.
    if let Some(policy) = source.get("imagePullPolicy") {
        container["imagePullPolicy"] = policy.clone();
    }
    if let Some(env_from) = source.get("envFrom").filter(|_| spec.inherit_env) {
        container["envFrom"] = env_from.clone();
    }
    if let Some(volume) = &spec.socket_volume {
        container["volumeMounts"] = json!([{
            "name": volume,
            "mountPath": socket_dir(&spec.attach)?,
        }]);
    }
    Ok(container)
}

/// The directory the socket lives in — what the sidecar has to mount, since a
/// volume mounts a directory and the annotation names a file inside it.
fn socket_dir(attach: &str) -> Result<String> {
    let path = attach
        .strip_prefix("unix:")
        .expect("callers check the scheme first");
    let parent = std::path::Path::new(path).parent().with_context(|| {
        format!(
            "{} has no directory to mount: {attach}",
            annotations::ATTACH
        )
    })?;
    if parent.as_os_str().is_empty() {
        bail!(
            "{}={attach} must be an absolute path so the socket's directory can be mounted",
            annotations::ATTACH
        );
    }
    // A socket directly under `/` would mount the volume over the container's
    // root, hiding the very binary the sidecar was told to run.
    if parent == std::path::Path::new("/") {
        bail!(
            "{}={attach} puts the socket at the filesystem root, and mounting a volume \
             at / would hide the image's own files. Put it in a directory, e.g. \
             unix:/run/taskito/attach.sock",
            annotations::ATTACH
        );
    }
    Ok(parent.to_string_lossy().into_owned())
}

/// The sidecar's environment: what the app container had, then what the
/// executor needs. Ours go last so a `FLEXIQ_ATTACH` inherited from the app
/// cannot override the address the annotation asked for.
fn environment(spec: &InjectionSpec, source: &Value) -> Vec<Value> {
    let mut env: Vec<Value> = if spec.inherit_env {
        source
            .get("env")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            // A name the executor sets itself would be shadowed anyway; drop it
            // here so the container spec has no duplicate keys to puzzle over.
            .filter(|entry| {
                !matches!(
                    entry.get("name").and_then(Value::as_str),
                    Some("FLEXIQ_ATTACH") | Some("FLEXIQ_SLOTS") | Some("FLEXIQ_ATTACH_TOKEN")
                )
            })
            .collect()
    } else {
        Vec::new()
    };

    env.push(json!({ "name": "FLEXIQ_ATTACH", "value": spec.attach }));
    env.push(json!({ "name": "FLEXIQ_SLOTS", "value": spec.slots.to_string() }));
    if let Some(token) = &spec.token {
        env.push(json!({
            "name": "FLEXIQ_ATTACH_TOKEN",
            "valueFrom": { "secretKeyRef": { "name": token.name, "key": token.key } },
        }));
    }
    env
}

/// Stamp the marker annotation. A pod with no annotations object at all needs
/// the map created first, or the `add` targets a path that does not exist.
fn marker_op(annotations: &std::collections::BTreeMap<String, String>) -> PatchOp {
    if annotations.is_empty() {
        return json!({
            "op": "add",
            "path": "/metadata/annotations",
            "value": { INJECTED_MARKER: "true" },
        });
    }
    json!({
        "op": "add",
        // `/` is `~1` in a JSON Pointer, so the annotation key has to be escaped
        // or the patch addresses a nested object that does not exist.
        "path": format!("/metadata/annotations/{}", INJECTED_MARKER.replace('~', "~0").replace('/', "~1")),
        "value": "true",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webhook::annotations::{ATTACH, COMMAND, CONTAINER, INJECT, SLOTS, TOKEN_SECRET};

    fn pod(annotations: Value, containers: Value) -> Value {
        json!({
            "metadata": { "name": "app-1", "annotations": annotations },
            "spec": { "containers": containers },
        })
    }

    fn opted_in() -> Value {
        json!({
            INJECT: "true",
            ATTACH: "taskito-scheduler:7777",
            COMMAND: "taskito executor --app myapp:queue",
        })
    }

    fn app_container() -> Value {
        json!([{ "name": "app", "image": "myapp:1.4.2" }])
    }

    /// The container the patch would add.
    fn sidecar(ops: &[PatchOp]) -> &Value {
        &ops.iter()
            .find(|op| op["path"] == "/spec/containers/-")
            .expect("a container op")["value"]
    }

    #[test]
    fn a_pod_that_did_not_opt_in_is_not_patched() {
        let pod = pod(json!({}), app_container());
        assert!(patch_for(&pod).expect("valid").is_none());
    }

    #[test]
    fn a_pod_with_no_annotations_at_all_is_not_patched() {
        let pod = json!({ "spec": { "containers": app_container() } });
        assert!(patch_for(&pod).expect("valid").is_none());
    }

    #[test]
    fn the_sidecar_reuses_the_app_image() {
        let ops = patch_for(&pod(opted_in(), app_container()))
            .expect("valid")
            .expect("patched");
        let sidecar = sidecar(&ops);
        assert_eq!(sidecar["image"], "myapp:1.4.2");
        assert_eq!(sidecar["name"], SIDECAR_NAME);
        assert_eq!(
            sidecar["command"],
            json!(["taskito", "executor", "--app", "myapp:queue"])
        );
    }

    #[test]
    fn the_address_and_slots_arrive_as_environment() {
        let mut annotations = opted_in();
        annotations[SLOTS] = json!("4");
        let ops = patch_for(&pod(annotations, app_container()))
            .expect("valid")
            .expect("patched");
        let env = sidecar(&ops)["env"].as_array().expect("env").clone();
        assert!(
            env.contains(&json!({ "name": "FLEXIQ_ATTACH", "value": "taskito-scheduler:7777" }))
        );
        assert!(env.contains(&json!({ "name": "FLEXIQ_SLOTS", "value": "4" })));
    }

    #[test]
    fn a_token_secret_becomes_a_secret_key_ref() {
        let mut annotations = opted_in();
        annotations[TOKEN_SECRET] = json!("taskito");
        let ops = patch_for(&pod(annotations, app_container()))
            .expect("valid")
            .expect("patched");
        let env = sidecar(&ops)["env"].as_array().expect("env").clone();
        assert!(env.contains(&json!({
            "name": "FLEXIQ_ATTACH_TOKEN",
            "valueFrom": { "secretKeyRef": { "name": "taskito", "key": "token" } },
        })));
    }

    #[test]
    fn the_app_environment_is_inherited() {
        let containers = json!([{
            "name": "app",
            "image": "myapp:1.4.2",
            "env": [{ "name": "DATABASE_URL", "value": "postgres://x" }],
            "envFrom": [{ "configMapRef": { "name": "app-config" } }],
        }]);
        let ops = patch_for(&pod(opted_in(), containers))
            .expect("valid")
            .expect("patched");
        let sidecar = sidecar(&ops);
        let env = sidecar["env"].as_array().expect("env");
        assert!(env.contains(&json!({ "name": "DATABASE_URL", "value": "postgres://x" })));
        assert_eq!(
            sidecar["envFrom"],
            json!([{ "configMapRef": { "name": "app-config" } }])
        );
    }

    #[test]
    fn an_inherited_attach_address_does_not_win() {
        let containers = json!([{
            "name": "app",
            "image": "myapp:1.4.2",
            "env": [{ "name": "FLEXIQ_ATTACH", "value": "wrong:1234" }],
        }]);
        let ops = patch_for(&pod(opted_in(), containers))
            .expect("valid")
            .expect("patched");
        let env = sidecar(&ops)["env"].as_array().expect("env").clone();
        let addresses: Vec<_> = env
            .iter()
            .filter(|entry| entry["name"] == "FLEXIQ_ATTACH")
            .collect();
        assert_eq!(addresses.len(), 1, "no duplicate key may survive");
        assert_eq!(addresses[0]["value"], "taskito-scheduler:7777");
    }

    #[test]
    fn a_named_container_is_used() {
        let containers = json!([
            { "name": "sidecar-proxy", "image": "proxy:1" },
            { "name": "app", "image": "myapp:1.4.2" },
        ]);
        let mut annotations = opted_in();
        annotations[CONTAINER] = json!("app");
        let ops = patch_for(&pod(annotations, containers))
            .expect("valid")
            .expect("patched");
        assert_eq!(sidecar(&ops)["image"], "myapp:1.4.2");
    }

    #[test]
    fn a_named_container_that_is_missing_is_an_error() {
        let mut annotations = opted_in();
        annotations[CONTAINER] = json!("nope");
        let error = patch_for(&pod(annotations, app_container())).expect_err("must reject");
        assert!(error.to_string().contains("nope"));
    }

    #[test]
    fn a_unix_attach_mounts_the_sockets_directory() {
        let mut annotations = opted_in();
        annotations[ATTACH] = json!("unix:/run/taskito/attach.sock");
        annotations[crate::webhook::annotations::SOCKET_VOLUME] = json!("attach");
        let ops = patch_for(&pod(annotations, app_container()))
            .expect("valid")
            .expect("patched");
        assert_eq!(
            sidecar(&ops)["volumeMounts"],
            json!([{ "name": "attach", "mountPath": "/run/taskito" }])
        );
    }

    #[test]
    fn a_socket_at_the_filesystem_root_is_rejected() {
        let mut annotations = opted_in();
        annotations[ATTACH] = json!("unix:/attach.sock");
        annotations[crate::webhook::annotations::SOCKET_VOLUME] = json!("attach");
        // Mounting at / would shadow the image, so the sidecar could not even
        // start the command it was given.
        let error = patch_for(&pod(annotations, app_container())).expect_err("must reject");
        assert!(error.to_string().contains("filesystem root"));
    }

    #[test]
    fn injecting_twice_is_a_no_op() {
        let containers = json!([
            { "name": "app", "image": "myapp:1.4.2" },
            { "name": SIDECAR_NAME, "image": "myapp:1.4.2" },
        ]);
        assert!(patch_for(&pod(opted_in(), containers))
            .expect("valid")
            .is_none());
    }

    #[test]
    fn the_marker_annotation_escapes_its_slash() {
        let ops = patch_for(&pod(opted_in(), app_container()))
            .expect("valid")
            .expect("patched");
        let marker = ops
            .iter()
            .find(|op| op["path"] != "/spec/containers/-")
            .expect("a marker op");
        assert_eq!(
            marker["path"],
            "/metadata/annotations/taskito.dev~1injected"
        );
    }

    #[test]
    fn the_pull_policy_follows_the_app() {
        let containers = json!([{
            "name": "app",
            "image": "myapp:1.4.2",
            "imagePullPolicy": "Always",
        }]);
        let ops = patch_for(&pod(opted_in(), containers))
            .expect("valid")
            .expect("patched");
        assert_eq!(sidecar(&ops)["imagePullPolicy"], "Always");
    }
}
