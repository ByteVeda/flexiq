//! Step identity: how a name plus an occurrence — or a name plus an explicit
//! key — becomes the string a memo lookup matches on.

use crate::error::{QueueError, Result};

/// Longest a step name may be. A name is written by hand at the call site; a
/// limit this generous only ever catches a name built from data by mistake.
const MAX_NAME_BYTES: usize = 128;
/// Longest an explicit key may be. Keys *are* built from data — an order id, a
/// tenant — so they get more room than a name.
const MAX_KEY_BYTES: usize = 256;

/// The separator between a name and its occurrence counter.
const OCCURRENCE_SEPARATOR: char = '#';
/// The separator between a name and an explicit key.
const KEY_SEPARATOR: char = ':';

/// Derives the identity of one step within a job.
///
/// Two forms, and they count independently. `derive` spends an occurrence of
/// the name; `explicit` does not, so adding a keyed call cannot shift the key
/// of a later unkeyed one — a divergence caused by an edit that changed nothing
/// about the unkeyed steps.
pub struct StepKey;

impl StepKey {
    /// `name#occurrence` — the default identity, where `occurrence` is how many
    /// times this name has already been requested in this attempt.
    ///
    /// Stable only while the surrounding code requests the same names in the
    /// same order, which is exactly what the divergence check verifies. A loop
    /// over anything whose order is not guaranteed wants
    /// [`explicit`](Self::explicit) instead.
    pub fn derive(name: &str, occurrence: u32) -> Result<String> {
        validate_name(name)?;
        Ok(format!("{name}{OCCURRENCE_SEPARATOR}{occurrence}"))
    }

    /// `name:key` — identity pinned to the data rather than to the position.
    ///
    /// A key is only ever compared, never parsed back, so it may contain
    /// anything the caller likes, including the separators.
    pub fn explicit(name: &str, key: &str) -> Result<String> {
        validate_name(name)?;
        validate_key(name, key)?;
        Ok(format!("{name}{KEY_SEPARATOR}{key}"))
    }
}

/// A name must be writable as itself in a key, so it may not contain either
/// separator: `charge#1` as a name would collide with the second occurrence of
/// `charge`.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(QueueError::Config("a step name must not be empty".into()));
    }
    if name.len() > MAX_NAME_BYTES {
        return Err(QueueError::Config(format!(
            "step name '{}' is {} bytes, over the {MAX_NAME_BYTES} byte limit",
            abbreviate(name),
            name.len()
        )));
    }
    if let Some(separator) = [OCCURRENCE_SEPARATOR, KEY_SEPARATOR]
        .into_iter()
        .find(|c| name.contains(*c))
    {
        return Err(QueueError::Config(format!(
            "step name '{}' contains '{separator}', which separates a name from its key",
            abbreviate(name)
        )));
    }
    Ok(())
}

fn validate_key(name: &str, key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(QueueError::Config(format!(
            "step '{}' was given an empty key; omit the key to number it by occurrence",
            abbreviate(name)
        )));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(QueueError::Config(format!(
            "key '{}' of step '{}' is {} bytes, over the {MAX_KEY_BYTES} byte limit",
            abbreviate(key),
            abbreviate(name),
            key.len()
        )));
    }
    Ok(())
}

/// Bound what an error message quotes back. The value that failed is often the
/// reason it failed — a name built from a payload — and pasting all of it into
/// a log line helps nobody.
pub(super) fn abbreviate(value: &str) -> String {
    const MAX_CHARS: usize = 48;

    match value.char_indices().nth(MAX_CHARS) {
        Some((cut, _)) => format!("{}…", &value[..cut]),
        None => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unkeyed_step_is_named_by_its_occurrence() {
        assert_eq!(StepKey::derive("charge", 0).unwrap(), "charge#0");
        assert_eq!(StepKey::derive("charge", 12).unwrap(), "charge#12");
    }

    #[test]
    fn a_keyed_step_is_named_by_its_data() {
        assert_eq!(StepKey::explicit("fetch", "1234").unwrap(), "fetch:1234");
    }

    #[test]
    fn the_two_forms_cannot_collide() {
        // A name may hold neither separator, so no explicit key can be spelled
        // the way an occurrence is, and vice versa.
        assert_ne!(
            StepKey::derive("fetch", 0).unwrap(),
            StepKey::explicit("fetch", "0").unwrap()
        );
    }

    #[test]
    fn a_key_may_contain_anything() {
        for key in ["a#b", "a:b", "  ", "ünïcode", &"x".repeat(256)] {
            assert!(StepKey::explicit("fetch", key).is_ok(), "{key}");
        }
    }

    #[test]
    fn an_ambiguous_name_is_refused_before_any_io() {
        for name in ["", "charge#1", "charge:1", &"n".repeat(129)] {
            let err = StepKey::derive(name, 0).unwrap_err();
            assert!(
                matches!(err, QueueError::Config(_)),
                "name {name:?} gave {err}"
            );
        }
    }

    #[test]
    fn an_unusable_key_is_refused() {
        for key in ["", &"k".repeat(257)] {
            let err = StepKey::explicit("fetch", key).unwrap_err();
            assert!(matches!(err, QueueError::Config(_)), "key {key:?}");
        }
    }

    #[test]
    fn an_error_quotes_back_a_bounded_prefix() {
        let err = StepKey::derive(&"n".repeat(200), 0)
            .unwrap_err()
            .to_string();
        assert!(err.contains('…'), "{err}");
        assert!(err.contains("200 bytes"), "{err}");
        assert!(
            err.len() < 200,
            "an error must not paste the whole name: {err}"
        );
    }

    #[test]
    fn abbreviating_never_splits_a_character() {
        // A cut mid-codepoint would panic; every prefix length must land on a
        // character boundary.
        let wide = "é".repeat(80);
        assert!(abbreviate(&wide).starts_with("ééé"));
    }
}
