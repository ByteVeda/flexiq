//! The expansion emits a safe `run` and calls it from `run_encoded` with
//! decoded arguments, so an `unsafe fn` would lose the contract its callers are
//! meant to uphold — silently, since the generated code compiles.

#[flexiq::task]
unsafe fn charge(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

fn main() {}
