//! Auto-discovered schema migrations.
//!
//! `build.rs` scans this directory at compile time and generates the module
//! declarations and the `all()` registry — drop a new `mXXXX_*.rs` file here
//! and it is picked up automatically, no manual edits. Identity is each
//! migration's own `version()` (never the filename); the migrator applies them
//! in ascending `version()` order.

use crate::storage::migrate::Migration;

include!(concat!(env!("OUT_DIR"), "/migrations_generated.rs"));
