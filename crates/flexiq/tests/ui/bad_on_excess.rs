//! `on_excess` takes one of two spellings, and "discard" is neither.

#[flexiq::task(on_excess = "discard")]
fn charge(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
