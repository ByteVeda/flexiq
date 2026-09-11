//! The shell's pool runs every handler on a blocking thread, so an async task
//! is refused with the two ways out rather than expanded into something that
//! never awaits.

#[flexiq::task]
async fn charge(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
