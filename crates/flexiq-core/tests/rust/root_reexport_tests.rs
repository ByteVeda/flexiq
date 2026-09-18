//! The crate root is the blessed import path, so every public record has to be
//! on it. Twice now the list in `lib.rs` has been declared complete while it
//! was not (#921, then #949, both corrected by #948), because nothing compares
//! it against `records.rs` — a record added there is simply forgotten. This
//! reads both files as text and diffs the two sets, so the next omission fails
//! here instead of being found by a consumer writing out the module path.

/// Top-level `pub struct` / `pub enum` names in `records.rs`. Column-0 only:
/// rustfmt indents anything nested, which keeps the file's `#[cfg(test)]`
/// module out without having to parse Rust.
fn declared_records() -> Vec<String> {
    let src = include_str!("../../src/storage/records.rs");
    let mut names: Vec<String> = src
        .lines()
        .filter_map(|line| {
            let rest = line
                .strip_prefix("pub struct ")
                .or_else(|| line.strip_prefix("pub enum "))?;
            // Stop at whatever follows the ident: `<'a>`, `{`, `(` or `;`.
            let end = rest
                .find(|c: char| !c.is_alphanumeric() && c != '_')
                .unwrap_or(rest.len());
            Some(rest[..end].to_string())
        })
        .collect();
    names.sort();
    names
}

/// Names inside the root's `pub use storage::records::{…};` block.
fn reexported_records() -> Vec<String> {
    let src = include_str!("../../src/lib.rs");
    let (_, after) = src
        .split_once("pub use storage::records::{")
        .expect("lib.rs re-exports storage::records as a braced group");
    let (block, _) = after
        .split_once("};")
        .expect("the storage::records re-export group is closed");
    let mut names: Vec<String> = block
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect();
    names.sort();
    names
}

#[test]
fn every_public_record_is_re_exported_from_the_root() {
    let declared = declared_records();
    let re_exported = reexported_records();

    let missing: Vec<&String> = declared
        .iter()
        .filter(|name| !re_exported.contains(name))
        .collect();
    assert!(
        missing.is_empty(),
        "records missing from the `pub use storage::records::{{…}}` block in \
         crates/flexiq-core/src/lib.rs: {missing:?}"
    );

    // The other direction: a record renamed or made private leaves a stale name
    // behind, which would not compile — but a `pub use` of a *different* module's
    // type smuggled into this group would, and it belongs in its own block.
    let stale: Vec<&String> = re_exported
        .iter()
        .filter(|name| !declared.contains(name))
        .collect();
    assert!(
        stale.is_empty(),
        "names in the storage::records re-export group that `records.rs` does \
         not declare: {stale:?}"
    );
}
