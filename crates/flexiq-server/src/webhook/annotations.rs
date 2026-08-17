//! The annotation contract a pod opts in with.
//!
//! Everything the injector needs comes from annotations rather than a chart
//! value, because the thing being described is the *pod*: which of its
//! containers carries the app, what command runs an executor against it, how
//! many slots that app can afford. A cluster-wide default could not know any of
//! it.
//!
//! Parsing is deliberately strict. A pod that asks for injection and gets it
//! wrong is rejected with a message naming the annotation, because the
//! alternative — admitting it with a silently skipped sidecar — looks exactly
//! like a working deployment until no job ever runs.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};

/// Opt-in switch. Absent or anything but a true value means "leave this pod
/// alone".
pub const INJECT: &str = "flexiq.dev/inject";
/// Scheduler address the executor dials.
pub const ATTACH: &str = "flexiq.dev/attach";
/// Argv that runs an executor inside the app image.
pub const COMMAND: &str = "flexiq.dev/command";
/// Jobs the executor runs at once.
pub const SLOTS: &str = "flexiq.dev/slots";
/// Container whose image and environment the sidecar copies.
pub const CONTAINER: &str = "flexiq.dev/container";
/// Secret holding the attach token.
pub const TOKEN_SECRET: &str = "flexiq.dev/token-secret";
/// Key within that secret.
pub const TOKEN_KEY: &str = "flexiq.dev/token-key";
/// Volume to mount for a `unix:` attach address.
pub const SOCKET_VOLUME: &str = "flexiq.dev/socket-volume";
/// Whether to copy the source container's `env` and `envFrom`.
pub const INHERIT_ENV: &str = "flexiq.dev/inherit-env";

/// Default key read from the token secret.
const DEFAULT_TOKEN_KEY: &str = "token";

/// What a pod's annotations asked the injector to add.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InjectionSpec {
    /// Scheduler address, verbatim from the annotation.
    pub attach: String,
    /// Executor argv.
    pub command: Vec<String>,
    /// Concurrent jobs.
    pub slots: u32,
    /// Container to copy the image (and optionally the environment) from.
    /// `None` means the first container in the pod.
    pub source_container: Option<String>,
    /// Secret name and key holding the attach token, when one is configured.
    pub token: Option<TokenRef>,
    /// Volume carrying the Unix socket, for a `unix:` attach.
    pub socket_volume: Option<String>,
    /// Whether the sidecar inherits the source container's environment.
    pub inherit_env: bool,
}

/// A `secretKeyRef` for the attach token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenRef {
    /// Secret name.
    pub name: String,
    /// Key within it.
    pub key: String,
}

/// Read the injection request out of `annotations`, or `None` when the pod did
/// not opt in.
pub fn parse(annotations: &BTreeMap<String, String>) -> Result<Option<InjectionSpec>> {
    if !opted_in(annotations) {
        return Ok(None);
    }

    let attach = required(annotations, ATTACH)?;
    let command = parse_command(&required(annotations, COMMAND)?)?;
    let socket_volume = optional(annotations, SOCKET_VOLUME);

    // A `unix:` address is a path inside the pod, and the sidecar only sees it
    // if the volume holding it is mounted. Catching that here turns a sidecar
    // that crash-loops on ENOENT into a rejected pod with a reason.
    if attach.starts_with("unix:") && socket_volume.is_none() {
        bail!(
            "{ATTACH}={attach} is a Unix socket, so {SOCKET_VOLUME} must name the \
             volume that carries it — the sidecar cannot reach a path it has not \
             mounted"
        );
    }

    Ok(Some(InjectionSpec {
        attach,
        command,
        slots: parse_slots(annotations)?,
        source_container: optional(annotations, CONTAINER),
        token: parse_token(annotations),
        socket_volume,
        inherit_env: !matches!(
            boolean(annotations, INHERIT_ENV).as_deref(),
            Some("false") | Some("0") | Some("no") | Some("off")
        ),
    }))
}

/// Whether `flexiq.dev/inject` reads as true.
fn opted_in(annotations: &BTreeMap<String, String>) -> bool {
    matches!(
        boolean(annotations, INJECT).as_deref(),
        Some("true") | Some("1") | Some("yes") | Some("on")
    )
}

/// A boolean-ish annotation, lowercased.
///
/// `"True"` is what a templating tool emits from a real boolean, and reading it
/// as "not opted in" would skip the sidecar silently — the exact failure this
/// module exists to avoid.
fn boolean(annotations: &BTreeMap<String, String>, key: &str) -> Option<String> {
    optional(annotations, key).map(|value| value.to_ascii_lowercase())
}

/// A set annotation, trimmed, with an empty value reading as unset — pod
/// templating tools emit `""` freely for a value that was not supplied.
fn optional(annotations: &BTreeMap<String, String>, key: &str) -> Option<String> {
    annotations
        .get(key)
        .map(|raw| raw.trim().to_string())
        .filter(|trimmed| !trimmed.is_empty())
}

fn required(annotations: &BTreeMap<String, String>, key: &str) -> Result<String> {
    optional(annotations, key)
        .with_context(|| format!("{key} is required when {INJECT} is set on a pod"))
}

/// Parse the executor argv, as either a JSON array or a whitespace-split
/// string.
///
/// JSON is the form that survives an argument containing spaces; the bare form
/// exists because most commands have none and quoting JSON inside YAML
/// annotations is miserable.
fn parse_command(raw: &str) -> Result<Vec<String>> {
    let argv = if raw.starts_with('[') {
        serde_json::from_str::<Vec<String>>(raw).with_context(|| {
            format!("{COMMAND} looks like JSON but is not an array of strings: {raw}")
        })?
    } else {
        raw.split_whitespace().map(str::to_string).collect()
    };

    if argv.is_empty() {
        bail!("{COMMAND} is empty — it must be the argv that runs an executor");
    }
    Ok(argv)
}

fn parse_slots(annotations: &BTreeMap<String, String>) -> Result<u32> {
    let Some(raw) = optional(annotations, SLOTS) else {
        return Ok(1);
    };
    let slots: u32 = raw
        .parse()
        .with_context(|| format!("{SLOTS} must be a positive integer, got '{raw}'"))?;
    if slots == 0 {
        bail!("{SLOTS} must be at least 1");
    }
    Ok(slots)
}

/// The token reference, if a secret was named. A key without a secret is
/// ignored rather than an error: it describes nothing on its own.
fn parse_token(annotations: &BTreeMap<String, String>) -> Option<TokenRef> {
    let name = optional(annotations, TOKEN_SECRET)?;
    let key = optional(annotations, TOKEN_KEY).unwrap_or_else(|| DEFAULT_TOKEN_KEY.to_string());
    Some(TokenRef { name, key })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn annotations(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn minimal() -> Vec<(&'static str, &'static str)> {
        vec![
            (INJECT, "true"),
            (ATTACH, "flexiq-scheduler:7777"),
            (COMMAND, "flexiq executor --app myapp:queue"),
        ]
    }

    #[test]
    fn a_pod_without_the_switch_is_left_alone() {
        let spec = parse(&annotations(&[(ATTACH, "flexiq-scheduler:7777")])).expect("valid");
        assert!(spec.is_none());
    }

    #[test]
    fn an_explicit_false_is_left_alone() {
        let mut pairs = minimal();
        pairs[0] = (INJECT, "false");
        assert!(parse(&annotations(&pairs)).expect("valid").is_none());
    }

    #[test]
    fn the_minimal_opt_in_parses_with_defaults() {
        let spec = parse(&annotations(&minimal()))
            .expect("valid")
            .expect("opted in");
        assert_eq!(spec.attach, "flexiq-scheduler:7777");
        assert_eq!(
            spec.command,
            vec!["flexiq", "executor", "--app", "myapp:queue"]
        );
        assert_eq!(spec.slots, 1);
        assert_eq!(spec.source_container, None);
        assert_eq!(spec.token, None);
        assert!(spec.inherit_env);
    }

    #[test]
    fn a_json_command_keeps_arguments_containing_spaces() {
        let mut pairs = minimal();
        pairs[2] = (COMMAND, r#"["java","-cp","app.jar","Cli","executor"]"#);
        let spec = parse(&annotations(&pairs))
            .expect("valid")
            .expect("opted in");
        assert_eq!(
            spec.command,
            vec!["java", "-cp", "app.jar", "Cli", "executor"]
        );
    }

    #[test]
    fn malformed_json_names_the_annotation() {
        let mut pairs = minimal();
        pairs[2] = (COMMAND, r#"["flexiq", 4]"#);
        let error = parse(&annotations(&pairs)).expect_err("must reject");
        assert!(error.to_string().contains(COMMAND));
    }

    #[test]
    fn opting_in_without_an_address_is_rejected() {
        let error = parse(&annotations(&[
            (INJECT, "true"),
            (COMMAND, "flexiq executor --app myapp:queue"),
        ]))
        .expect_err("must reject");
        assert!(error.to_string().contains(ATTACH));
    }

    #[test]
    fn opting_in_without_a_command_is_rejected() {
        let error = parse(&annotations(&[
            (INJECT, "true"),
            (ATTACH, "flexiq-scheduler:7777"),
        ]))
        .expect_err("must reject");
        assert!(error.to_string().contains(COMMAND));
    }

    #[test]
    fn a_unix_address_without_a_volume_is_rejected() {
        let mut pairs = minimal();
        pairs[1] = (ATTACH, "unix:/run/flexiq/attach.sock");
        let error = parse(&annotations(&pairs)).expect_err("must reject");
        assert!(error.to_string().contains(SOCKET_VOLUME));
    }

    #[test]
    fn a_unix_address_with_a_volume_parses() {
        let mut pairs = minimal();
        pairs[1] = (ATTACH, "unix:/run/flexiq/attach.sock");
        pairs.push((SOCKET_VOLUME, "attach"));
        let spec = parse(&annotations(&pairs))
            .expect("valid")
            .expect("opted in");
        assert_eq!(spec.socket_volume.as_deref(), Some("attach"));
    }

    #[test]
    fn zero_slots_is_rejected() {
        let mut pairs = minimal();
        pairs.push((SLOTS, "0"));
        let error = parse(&annotations(&pairs)).expect_err("must reject");
        assert!(error.to_string().contains(SLOTS));
    }

    #[test]
    fn a_token_key_defaults_when_only_the_secret_is_named() {
        let mut pairs = minimal();
        pairs.push((TOKEN_SECRET, "flexiq"));
        let spec = parse(&annotations(&pairs))
            .expect("valid")
            .expect("opted in");
        assert_eq!(
            spec.token,
            Some(TokenRef {
                name: "flexiq".to_string(),
                key: DEFAULT_TOKEN_KEY.to_string(),
            })
        );
    }

    #[test]
    fn a_key_without_a_secret_describes_nothing_and_is_ignored() {
        let mut pairs = minimal();
        pairs.push((TOKEN_KEY, "attach-token"));
        let spec = parse(&annotations(&pairs))
            .expect("valid")
            .expect("opted in");
        assert_eq!(spec.token, None);
    }

    #[test]
    fn inheriting_the_environment_can_be_turned_off() {
        let mut pairs = minimal();
        pairs.push((INHERIT_ENV, "false"));
        let spec = parse(&annotations(&pairs))
            .expect("valid")
            .expect("opted in");
        assert!(!spec.inherit_env);
    }

    #[test]
    fn a_capitalised_boolean_still_opts_in() {
        // What a templating tool emits from a real YAML boolean. Reading it as
        // "not opted in" would skip the sidecar without a word.
        for value in ["True", "TRUE", "Yes", "On"] {
            let mut pairs = minimal();
            pairs[0] = (INJECT, value);
            assert!(
                parse(&annotations(&pairs)).expect("valid").is_some(),
                "{value} must opt in"
            );
        }
    }

    #[test]
    fn a_capitalised_false_still_disables_inheritance() {
        let mut pairs = minimal();
        pairs.push((INHERIT_ENV, "False"));
        let spec = parse(&annotations(&pairs))
            .expect("valid")
            .expect("opted in");
        assert!(!spec.inherit_env);
    }

    #[test]
    fn a_blank_value_reads_as_unset() {
        let mut pairs = minimal();
        pairs.push((CONTAINER, "   "));
        let spec = parse(&annotations(&pairs))
            .expect("valid")
            .expect("opted in");
        assert_eq!(spec.source_container, None);
    }
}
