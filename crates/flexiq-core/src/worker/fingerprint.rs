//! Registry fingerprint: one short, comparable value for "what tasks does this
//! peer know how to run".
//!
//! Autodiscovery makes a worker's registry *implicit* — it is whatever the
//! import walk found — and an unregistered task name is a fatal, non-retryable
//! failure. A worker that discovered eleven of twelve modules dead-letters
//! every job for the twelfth and says nothing. The fingerprint is what lets two
//! peers that are supposed to be interchangeable notice that they are not.
//!
//! # Algorithm
//!
//! 64-bit FNV-1a over the sorted, de-duplicated names, each **length-prefixed**,
//! rendered as sixteen lowercase hex digits:
//!
//! ```text
//! h = 0xcbf29ce484222325
//! for name in sorted_unique(names):                # sorted by UTF-8 bytes
//!     for byte in be_u64(len(utf8(name))) + utf8(name):
//!         h = (h ^ byte) * 0x100000001b3           # mod 2^64
//! ```
//!
//! Every choice here exists to be re-implementable in an SDK's standard library
//! alone — the same constraint that keeps the frame headers JSON:
//!
//! - **Not cryptographic.** A collision costs a missed warning, never a wrong
//!   dispatch, and requiring SHA-2 would put a crypto dependency in the path of
//!   anyone writing an executor.
//! - **Sorted by UTF-8 bytes**, stated rather than implied: JavaScript's
//!   `Array.prototype.sort` and Java's `String.compareTo` order by UTF-16 code
//!   units, which disagrees with byte order above the BMP.
//! - **De-duplicated**, so registering a name twice cannot change the answer.
//! - **Length-prefixed, not separated.** Any separator can also occur *inside*
//!   a task name: with a trailing `\n`, `["a\nb"]` and `["a", "b"]` both hash the
//!   bytes `a\nb\n`, so two different registries share a fingerprint and the
//!   divergence this module exists to catch is silently suppressed. A
//!   fixed-width length makes the encoding injective, and eight big-endian
//!   bytes are something every standard library can produce.
//!
//! An empty registry has no fingerprint. "Registered nothing" and "does not
//! report one" are both nothing to compare against, and giving the empty set a
//! value would make a deliberately inert executor look divergent from every
//! real one.

use std::collections::BTreeSet;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;

/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// Fingerprint of a task registry, or `None` when it holds nothing.
///
/// Order and duplicates in `names` do not affect the result — see the module
/// docs for the exact algorithm, which an SDK writing its own executor has to
/// match.
pub fn registry_fingerprint<I, S>(names: I) -> Option<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    // `BTreeSet<String>` does both jobs the algorithm needs: `Ord for String`
    // is byte-wise over the UTF-8 encoding, which is the order the spec pins,
    // and a set is the de-duplication.
    let unique: BTreeSet<String> = names
        .into_iter()
        .map(|name| name.as_ref().to_string())
        .collect();
    if unique.is_empty() {
        return None;
    }

    let mut hash = FNV_OFFSET_BASIS;
    for name in &unique {
        let bytes = name.as_bytes();
        // Length-prefixed rather than separated: see the module docs. A
        // separator that can also occur inside a name makes the encoding
        // ambiguous, and the collision that follows is a missed warning.
        for byte in (bytes.len() as u64)
            .to_be_bytes()
            .into_iter()
            .chain(bytes.iter().copied())
        {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    Some(format!("{hash:016x}"))
}

#[cfg(test)]
mod tests {
    use super::registry_fingerprint;

    #[test]
    fn an_empty_registry_has_no_fingerprint() {
        assert_eq!(registry_fingerprint(Vec::<String>::new()), None);
    }

    #[test]
    fn order_does_not_change_the_fingerprint() {
        assert_eq!(
            registry_fingerprint(["b", "a", "c"]),
            registry_fingerprint(["c", "b", "a"]),
        );
    }

    #[test]
    fn duplicates_do_not_change_the_fingerprint() {
        assert_eq!(
            registry_fingerprint(["a", "b"]),
            registry_fingerprint(["a", "b", "a"]),
        );
    }

    #[test]
    fn a_different_registry_gets_a_different_fingerprint() {
        assert_ne!(registry_fingerprint(["a"]), registry_fingerprint(["b"]));
        assert_ne!(
            registry_fingerprint(["a", "b"]),
            registry_fingerprint(["a", "b", "c"]),
        );
    }

    /// The framing is why these differ. Hashed back to back both would give the
    /// bytes `abc`, and two fleets running different registries would look
    /// identical.
    #[test]
    fn concatenation_alone_would_collide() {
        assert_ne!(
            registry_fingerprint(["ab", "c"]),
            registry_fingerprint(["a", "bc"]),
        );
    }

    /// The reason the encoding is length-prefixed rather than separated. Any
    /// separator can also occur *inside* a task name, and a separated encoding
    /// then hashes identical bytes for two different registries — the exact
    /// divergence this module exists to catch, silently suppressed.
    #[test]
    fn a_name_containing_a_newline_does_not_collide_with_two_names() {
        assert_ne!(
            registry_fingerprint(["a\nb"]),
            registry_fingerprint(["a", "b"]),
        );
    }

    /// Pins the algorithm, not just its properties. An SDK re-implementing the
    /// fingerprint has to reproduce this value, and a refactor that quietly
    /// changed the hash would split a mixed fleet into two "divergent" halves
    /// that in fact run the same tasks.
    #[test]
    fn the_fingerprint_of_a_known_registry_is_pinned() {
        assert_eq!(
            registry_fingerprint(["invoices.send", "reports.build"]).as_deref(),
            Some("fafd30ef8ebcb7de"),
        );
    }

    #[test]
    fn a_single_name_is_pinned() {
        assert_eq!(
            registry_fingerprint(["a"]).as_deref(),
            Some("e6017d3a248deb69")
        );
    }

    /// The table in `BINDING_CONTRACT.md`, asserted rather than described.
    ///
    /// The cases are the ones a re-implementation gets wrong: the framing
    /// pairs, and the newline that a separated encoding would collide. Every
    /// SDK asserts the same strings, so a drift here is caught in this crate
    /// instead of as a fleet that looks split while running the same code.
    #[test]
    fn the_contract_vectors_are_pinned() {
        for (names, want) in [
            (vec!["ab", "c"], "fe4b6261eea66aa8"),
            (vec!["a", "bc"], "e6b0607a88120c30"),
            (vec!["a\nb"], "068365c3a2f19d9f"),
            (vec!["a", "b"], "9dbd0e0e67e641dc"),
            (
                vec!["reports.build", "invoices.send", "reports.build"],
                "fafd30ef8ebcb7de",
            ),
            // The only vector that separates UTF-8 byte order from UTF-16 code
            // units, which is the ordering rule most at risk of being got wrong
            // — every ASCII vector above passes under either. See
            // `sorting_above_the_bmp_follows_utf_8_not_utf_16`.
            (vec!["\u{E000}", "\u{10000}"], "370802f2ebd8a642"),
        ] {
            assert_eq!(
                registry_fingerprint(&names).as_deref(),
                Some(want),
                "fingerprint of {names:?}"
            );
        }
    }

    /// The ordering rule, on the only inputs that can catch it breaking.
    ///
    /// `U+E000` is a single UTF-16 unit `0xE000`; `U+10000` is the surrogate
    /// pair `0xD800 0xDC00`. Java's `String.compareTo` and JavaScript's
    /// `Array.prototype.sort` compare those units and put `U+10000` first,
    /// while UTF-8 bytes put `U+E000` first — so an SDK that sorted natively
    /// would agree with every ASCII vector in the contract and still hash a
    /// different value for a fleet using astral task names.
    ///
    /// Rust cannot get this wrong (`Ord for String` is byte-wise), so what this
    /// pins is the *contract*: the constant below is what a re-implementation
    /// has to reproduce.
    #[test]
    fn sorting_above_the_bmp_follows_utf_8_not_utf_16() {
        let canonical = registry_fingerprint(["\u{E000}", "\u{10000}"]);
        assert_eq!(canonical.as_deref(), Some("370802f2ebd8a642"));

        // Input order must not matter here either — otherwise the vector would
        // pin the caller's ordering rather than the algorithm's.
        assert_eq!(registry_fingerprint(["\u{10000}", "\u{E000}"]), canonical,);

        // What a UTF-16-ordered implementation would produce. Asserted as the
        // wrong answer so the two are never quietly reconciled.
        assert_ne!(canonical.as_deref(), Some("7653f22bef39d8ea"));
    }
}
