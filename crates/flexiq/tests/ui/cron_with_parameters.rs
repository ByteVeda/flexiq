//! A periodic fire has no caller, so a scheduled task cannot take arguments.
//! Left to runtime this decodes as missing arguments on every single fire.

#[flexiq::task(cron = "0 0 3 * * *")]
fn nightly(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
