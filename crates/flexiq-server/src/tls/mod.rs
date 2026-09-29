//! TLS on the listeners that carry a credential: which files, how they load,
//! and how a rotated pair replaces the one in force without a restart.

pub mod config;
pub mod load;
pub mod watch;

pub use config::TlsFiles;
pub use load::ServerTls;
