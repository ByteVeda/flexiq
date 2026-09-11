//! What `#[flexiq::task]` refuses, and what it says.
//!
//! A macro's error messages are its user interface, and they are the one part
//! of it no ordinary test exercises: every other test here compiles. These
//! cases assert the refusals still name the problem rather than pointing at a
//! generated token.

/// `trybuild` shells out to cargo per case, so this is deliberately one test
/// over a directory rather than several.
#[test]
fn rejections_name_the_problem() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
