# flexiq-macros

The `#[task]` attribute for the [FlexiQ](https://crates.io/crates/flexiq) Rust SDK.

This crate exists only to hold that macro: a proc-macro crate can export nothing
else, so the attribute has to live apart from the shell that uses it.

**Depend on [`flexiq`](https://crates.io/crates/flexiq), not on this.** The macro
is re-exported as `flexiq::task`, and its expansion names `::flexiq::` paths —
so it does not compile without that crate present anyway.

```toml
[dependencies]
flexiq = "2"
```

```rust,ignore
#[flexiq::task(max_retries = 5, timeout = "30s", queue = "billing")]
fn charge(order_id: String, cents: i64) -> flexiq::Outcome<i64> {
    Ok(cents)
}
```

The full attribute list and what each one reaches are documented on the macro
itself.

## License

MIT.
