//! A rate below one never releases a job: the backends compare against
//! `tokens < 1.0`, so the task would simply never dispatch and nothing would
//! say why. `NaN` loses the same comparison.

#[flexiq::task(rate_limit = "0/s")]
fn charge(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
