//! Build script: runs `napi_build::setup()` to emit the link configuration the
//! `#[napi]` macros need to produce a loadable Node addon.

fn main() {
    napi_build::setup();
}
