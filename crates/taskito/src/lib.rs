#![doc = include_str!("../README.md")]

// The whole crate is a re-export: `taskito::X` and `taskito_core::X` are the
// same item, so there is exactly one definition and one set of docs to keep
// current. Anything added to the core root appears here without an edit.
pub use taskito_core::*;

// Also re-exported under its own name, so code that already spells out
// `taskito_core::` keeps compiling when it depends only on this crate.
pub use taskito_core;
