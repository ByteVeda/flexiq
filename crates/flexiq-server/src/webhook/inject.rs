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
pub const SIDECAR_NAME: &str = "flexiq-executor";

/// Annotation stamped on a mutated pod, recording that this injector ran.
pub const INJECTED_MARKER: &str = "flexiq.dev/injected";

/// Volume carrying the CA bundle for a `tls://` attach.
const TLS_CA_VOLUME: &str = "flexiq-attach-ca";
/// Where the sidecar sees it.
const TLS_CA_DIR: &str = "/etc/flexiq/attach-ca";
/// Volume carrying the client certificate for mTLS.
const TLS_CLIENT_VOLUME: &str = "flexiq-attach-client";
/// Where the sidecar sees it.
const TLS_CLIENT_DIR: &str = "/etc/flexiq/attach-client";

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
    ops.extend(volume_ops(pod, tls_volumes(&spec))?);
    ops.push(marker_op(&annotations));
    Ok(Some(ops))
}

/// Secret volumes for the attach TLS material the annotations named.
fn tls_volumes(spec: &InjectionSpec) -> Vec<Value> {
    let mut volumes = Vec::new();
    if let Some(ca) = &spec.tls_ca {
        volumes.push(json!({ "name": TLS_CA_VOLUME, "secret": { "secretName": ca.name } }));
    }
    if let Some(client) = &spec.tls_client {
        volumes.push(json!({ "name": TLS_CLIENT_VOLUME, "secret": { "secretName": client } }));
    }
    volumes
}

/// Ops adding `volumes` to the pod. A pod with no `volumes` array needs it
/// created whole, since appending targets a path that does not exist.
fn volume_ops(pod: &Value, volumes: Vec<Value>) -> Result<Vec<PatchOp>> {
    if volumes.is_empty() {
        return Ok(Vec::new());
    }
    let Some(existing) = pod.pointer("/spec/volumes").and_then(Value::as_array) else {
        return Ok(vec![
            json!({ "op": "add", "path": "/spec/volumes", "value": volumes }),
        ]);
    };
    // A clash would make the API server reject the patched pod with a
    // duplicate-name error that never mentions the injector.
    for volume in &volumes {
        let name = volume["name"].as_str().unwrap_or_default();
        if existing
            .iter()
            .any(|other| other.get("name").and_then(Value::as_str) == Some(name))
        {
            bail!("the pod already has a volume named '{name}', which the injector needs for attach TLS — rename it");
        }
    }
    Ok(volumes
        .into_iter()
        .map(|volume| json!({ "op": "add", "path": "/spec/volumes/-", "value": volume }))
        .collect())
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
    let mut mounts = Vec::new();
    if let Some(volume) = &spec.socket_volume {
        mounts.push(json!({
            "name": volume,
            "mountPath": socket_dir(&spec.attach)?,
        }));
    }
    // Whole-directory mounts, never `subPath`: the kubelet refreshes a Secret
    // volume in place when it rotates, but a subPath mount stays frozen.
    if spec.tls_ca.is_some() {
        mounts.push(json!({ "name": TLS_CA_VOLUME, "mountPath": TLS_CA_DIR, "readOnly": true }));
    }
    if spec.tls_client.is_some() {
        mounts.push(
            json!({ "name": TLS_CLIENT_VOLUME, "mountPath": TLS_CLIENT_DIR, "readOnly": true }),
        );
    }
    if !mounts.is_empty() {
        container["volumeMounts"] = Value::Array(mounts);
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
             unix:/run/flexiq/attach.sock",
            annotations::ATTACH
        );
    }
    Ok(parent.to_string_lossy().into_owned())
}

/// The sidecar's environment: what the app container had, then what the
/// executor needs. Ours go last so a `FLEXIQ_ATTACH` inherited from the app
/// cannot override the address the annotation asked for.
fn environment(spec: &InjectionSpec, source: &Value) -> Vec<Value> {
    let mut owned = vec!["FLEXIQ_ATTACH", "FLEXIQ_SLOTS", "FLEXIQ_ATTACH_TOKEN"];
    // TLS paths only when the annotations supply them: otherwise an inherited
    // one may name a CA baked into the image, which is still a valid setup.
    if spec.tls_ca.is_some() {
        owned.push("FLEXIQ_ATTACH_TLS_CA");
    }
    if spec.tls_client.is_some() {
        owned.extend(["FLEXIQ_ATTACH_TLS_CERT", "FLEXIQ_ATTACH_TLS_KEY"]);
    }

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
                !entry
                    .get("name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| owned.contains(&name))
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
    if let Some(ca) = &spec.tls_ca {
        env.push(
            json!({ "name": "FLEXIQ_ATTACH_TLS_CA", "value": format!("{TLS_CA_DIR}/{}", ca.key) }),
        );
    }
    if spec.tls_client.is_some() {
        // A `kubernetes.io/tls` Secret always carries these two keys.
        env.push(json!({ "name": "FLEXIQ_ATTACH_TLS_CERT", "value": format!("{TLS_CLIENT_DIR}/tls.crt") }));
        env.push(json!({ "name": "FLEXIQ_ATTACH_TLS_KEY", "value": format!("{TLS_CLIENT_DIR}/tls.key") }));
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
    use crate::webhook::annotations::{
        ATTACH, COMMAND, CONTAINER, INJECT, SLOTS, TLS_CA_KEY, TLS_CA_SECRET, TLS_CLIENT_SECRET,
        TOKEN_SECRET,
    };

    fn pod(annotations: Value, containers: Value) -> Value {
        json!({
            "metadata": { "name": "app-1", "annotations": annotations },
            "spec": { "containers": containers },
        })
    }

    fn opted_in() -> Value {
        json!({
            INJECT: "true",
            ATTACH: "flexiq-scheduler:7777",
            COMMAND: "flexiq executor --app myapp:queue",
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
            json!(["flexiq", "executor", "--app", "myapp:queue"])
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
        assert!(env.contains(&json!({ "name": "FLEXIQ_ATTACH", "value": "flexiq-scheduler:7777" })));
        assert!(env.contains(&json!({ "name": "FLEXIQ_SLOTS", "value": "4" })));
    }

    #[test]
    fn a_token_secret_becomes_a_secret_key_ref() {
        let mut annotations = opted_in();
        annotations[TOKEN_SECRET] = json!("flexiq");
        let ops = patch_for(&pod(annotations, app_container()))
            .expect("valid")
            .expect("patched");
        let env = sidecar(&ops)["env"].as_array().expect("env").clone();
        assert!(env.contains(&json!({
            "name": "FLEXIQ_ATTACH_TOKEN",
            "valueFrom": { "secretKeyRef": { "name": "flexiq", "key": "token" } },
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
        assert_eq!(addresses[0]["value"], "flexiq-scheduler:7777");
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
        annotations[ATTACH] = json!("unix:/run/flexiq/attach.sock");
        annotations[crate::webhook::annotations::SOCKET_VOLUME] = json!("attach");
        let ops = patch_for(&pod(annotations, app_container()))
            .expect("valid")
            .expect("patched");
        assert_eq!(
            sidecar(&ops)["volumeMounts"],
            json!([{ "name": "attach", "mountPath": "/run/flexiq" }])
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
        assert_eq!(marker["path"], "/metadata/annotations/flexiq.dev~1injected");
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

    fn tls_opted_in() -> Value {
        let mut annotations = opted_in();
        annotations[ATTACH] = json!("tls://flexiq-scheduler:7777");
        annotations[TLS_CA_SECRET] = json!("attach-ca");
        annotations
    }

    fn env_value<'a>(sidecar: &'a Value, name: &str) -> Vec<&'a Value> {
        sidecar["env"]
            .as_array()
            .expect("env")
            .iter()
            .filter(|entry| entry["name"] == name)
            .collect()
    }

    /// Volume ops, flattened to the volumes they add.
    fn added_volumes(ops: &[PatchOp]) -> Vec<Value> {
        ops.iter()
            .filter_map(|op| match op["path"].as_str() {
                Some("/spec/volumes") => op["value"].as_array().cloned(),
                Some("/spec/volumes/-") => Some(vec![op["value"].clone()]),
                _ => None,
            })
            .flatten()
            .collect()
    }

    #[test]
    fn a_ca_secret_is_mounted_and_named_in_the_environment() {
        let ops = patch_for(&pod(tls_opted_in(), app_container()))
            .expect("valid")
            .expect("patched");
        let sidecar = sidecar(&ops);
        assert_eq!(
            sidecar["volumeMounts"],
            json!([{ "name": TLS_CA_VOLUME, "mountPath": TLS_CA_DIR, "readOnly": true }])
        );
        assert_eq!(
            env_value(sidecar, "FLEXIQ_ATTACH_TLS_CA"),
            vec![
                &json!({ "name": "FLEXIQ_ATTACH_TLS_CA", "value": "/etc/flexiq/attach-ca/ca.crt" })
            ]
        );
        assert!(env_value(sidecar, "FLEXIQ_ATTACH_TLS_CERT").is_empty());
        assert_eq!(
            added_volumes(&ops),
            vec![json!({ "name": TLS_CA_VOLUME, "secret": { "secretName": "attach-ca" } })]
        );
    }

    #[test]
    fn a_ca_key_override_names_that_file() {
        let mut annotations = tls_opted_in();
        annotations[TLS_CA_KEY] = json!("bundle.pem");
        let ops = patch_for(&pod(annotations, app_container()))
            .expect("valid")
            .expect("patched");
        assert_eq!(
            env_value(sidecar(&ops), "FLEXIQ_ATTACH_TLS_CA")[0]["value"],
            "/etc/flexiq/attach-ca/bundle.pem"
        );
    }

    #[test]
    fn a_client_secret_mounts_the_certificate_and_key() {
        let mut annotations = tls_opted_in();
        annotations[TLS_CLIENT_SECRET] = json!("executor-cert");
        let ops = patch_for(&pod(annotations, app_container()))
            .expect("valid")
            .expect("patched");
        let sidecar = sidecar(&ops);
        assert_eq!(
            sidecar["volumeMounts"],
            json!([
                { "name": TLS_CA_VOLUME, "mountPath": TLS_CA_DIR, "readOnly": true },
                { "name": TLS_CLIENT_VOLUME, "mountPath": TLS_CLIENT_DIR, "readOnly": true },
            ])
        );
        assert_eq!(
            env_value(sidecar, "FLEXIQ_ATTACH_TLS_CERT")[0]["value"],
            "/etc/flexiq/attach-client/tls.crt"
        );
        assert_eq!(
            env_value(sidecar, "FLEXIQ_ATTACH_TLS_KEY")[0]["value"],
            "/etc/flexiq/attach-client/tls.key"
        );
        assert_eq!(
            added_volumes(&ops),
            vec![
                json!({ "name": TLS_CA_VOLUME, "secret": { "secretName": "attach-ca" } }),
                json!({ "name": TLS_CLIENT_VOLUME, "secret": { "secretName": "executor-cert" } }),
            ]
        );
    }

    #[test]
    fn a_pod_without_volumes_gets_the_array_created() {
        let ops = patch_for(&pod(tls_opted_in(), app_container()))
            .expect("valid")
            .expect("patched");
        assert!(ops.iter().any(|op| op["path"] == "/spec/volumes"));
        assert!(!ops.iter().any(|op| op["path"] == "/spec/volumes/-"));
    }

    #[test]
    fn a_pod_with_volumes_gets_them_appended() {
        let mut pod = pod(tls_opted_in(), app_container());
        pod["spec"]["volumes"] = json!([{ "name": "data", "emptyDir": {} }]);
        let ops = patch_for(&pod).expect("valid").expect("patched");
        assert!(!ops.iter().any(|op| op["path"] == "/spec/volumes"));
        assert_eq!(added_volumes(&ops).len(), 1);
    }

    #[test]
    fn a_volume_name_clash_is_rejected() {
        let mut pod = pod(tls_opted_in(), app_container());
        pod["spec"]["volumes"] = json!([{ "name": TLS_CA_VOLUME, "emptyDir": {} }]);
        let error = patch_for(&pod).expect_err("must reject");
        assert!(error.to_string().contains(TLS_CA_VOLUME), "{error}");
    }

    #[test]
    fn an_inherited_tls_path_does_not_win_over_the_annotation() {
        let containers = json!([{
            "name": "app",
            "image": "myapp:1.4.2",
            "env": [{ "name": "FLEXIQ_ATTACH_TLS_CA", "value": "/app/ca.pem" }],
        }]);
        let ops = patch_for(&pod(tls_opted_in(), containers))
            .expect("valid")
            .expect("patched");
        let paths = env_value(sidecar(&ops), "FLEXIQ_ATTACH_TLS_CA");
        assert_eq!(paths.len(), 1, "no duplicate key may survive");
        assert_eq!(paths[0]["value"], "/etc/flexiq/attach-ca/ca.crt");
    }

    #[test]
    fn an_inherited_tls_path_survives_without_the_annotation() {
        // A CA baked into the image is still a working setup.
        let mut annotations = opted_in();
        annotations[ATTACH] = json!("tls://flexiq-scheduler:7777");
        let containers = json!([{
            "name": "app",
            "image": "myapp:1.4.2",
            "env": [{ "name": "FLEXIQ_ATTACH_TLS_CA", "value": "/app/ca.pem" }],
        }]);
        let ops = patch_for(&pod(annotations, containers))
            .expect("valid")
            .expect("patched");
        assert_eq!(
            env_value(sidecar(&ops), "FLEXIQ_ATTACH_TLS_CA")[0]["value"],
            "/app/ca.pem"
        );
        assert!(added_volumes(&ops).is_empty());
    }

    #[test]
    fn tls_material_beside_a_plaintext_address_denies_the_pod() {
        let mut annotations = opted_in();
        annotations[TLS_CLIENT_SECRET] = json!("executor-cert");
        let error = patch_for(&pod(annotations, app_container())).expect_err("must reject");
        assert!(error.to_string().contains("tls://"), "{error}");
    }
}
