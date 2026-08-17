//! Command-line arguments.

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "flexiq-tui",
    version,
    about = "Headless terminal UI for a FlexiQ queue",
    long_about = "Browse and operate a FlexiQ queue from the terminal — no browser, \
no dashboard server. Reads the queue's storage directly, so it works against a queue \
driven by any SDK.\n\n\
NOTE: opening a SQLite database runs pending schema migrations and a one-time archive \
sweep. This is not a passive read-only viewer."
)]
pub struct Cli {
    /// Connection URL. Examples:
    ///   sqlite:///abs/path/app.db | ./app.db | :memory:
    ///   postgres://user:pw@host/db   (needs --features postgres)
    ///   redis://host:6379            (needs --features redis)
    #[arg(long)]
    pub db: String,

    /// Auto-refresh interval for the active view, in milliseconds.
    #[arg(long, default_value_t = 2000)]
    pub refresh: u64,
}
