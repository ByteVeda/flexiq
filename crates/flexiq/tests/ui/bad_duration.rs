//! A duration the parser cannot read should say what it accepts.

#[flexiq::task(timeout = "30 fortnights")]
fn charge(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
