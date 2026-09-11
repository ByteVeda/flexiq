//! A misspelled attribute is the likeliest mistake, so the refusal lists every
//! one that exists rather than only rejecting this one.

#[flexiq::task(retries = 5)]
fn charge(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
