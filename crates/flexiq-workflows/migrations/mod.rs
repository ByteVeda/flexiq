//! Auto-discovered workflow schema migrations.
//!
//! `build.rs` scans this directory at compile time and generates the module
//! declarations and the `all()` registry — drop a new `mXXXX_*.rs` file here
//! and it is picked up automatically. Recorded in a dedicated
//! `workflow_schema_migrations` ledger so versions never collide with the core
//! migrations on a shared database.

use flexiq_core::storage::migrate::Migration;

include!(concat!(env!("OUT_DIR"), "/migrations_generated.rs"));
