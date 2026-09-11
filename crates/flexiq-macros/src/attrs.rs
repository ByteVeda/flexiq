//! Reading `#[task(...)]`.

// `Parser` is imported anonymously for `parse_terminated`'s `parse2`, which is
// a trait method on the parser function rather than on the token stream.
use syn::parse::Parser as _;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{Error, Expr, ExprLit, Lit, Meta, Result, Token};

use crate::duration;

/// Every attribute `#[task]` accepts, in the order the error message lists them.
const ACCEPTED: &[&str] = &[
    "name",
    "queue",
    "priority",
    "max_retries",
    "retry_backoff_ms",
    "retry_max_delay_ms",
    "timeout",
    "expires",
    "result_ttl",
    "idempotent",
    "on_excess",
    "max_concurrent",
    "max_in_flight_per_task",
    "rate_limit",
    "retry_budget",
    "cron",
    "timezone",
];

/// The two spellings `OnExcess::parse` accepts.
const ON_EXCESS: &[&str] = &["defer", "drop"];

/// What a task was declared with. Everything is optional; an absent value means
/// the default that [`flexiq_core::TaskConfig`] or `EnqueueOptions` already has,
/// so the macro never has to restate one.
#[derive(Default)]
pub struct TaskAttrs {
    pub name: Option<String>,
    pub queue: Option<String>,
    pub priority: Option<i32>,
    pub max_retries: Option<i32>,
    pub retry_backoff_ms: Option<i64>,
    pub retry_max_delay_ms: Option<i64>,
    pub timeout_ms: Option<i64>,
    pub expires_in_ms: Option<i64>,
    pub result_ttl_ms: Option<i64>,
    pub idempotent: bool,
    pub on_excess: Option<String>,
    pub max_concurrent: Option<i32>,
    pub max_in_flight_per_task: Option<usize>,
    pub rate_limit: Option<String>,
    pub retry_budget: Option<String>,
    pub cron: Option<String>,
    pub timezone: Option<String>,
}

impl TaskAttrs {
    /// Parse the comma-separated list inside `#[task(...)]`.
    pub fn parse(tokens: proc_macro2::TokenStream) -> Result<Self> {
        let mut attrs = Self::default();
        if tokens.is_empty() {
            return Ok(attrs);
        }

        let metas = Punctuated::<Meta, Token![,]>::parse_terminated
            .parse2(tokens)
            .map_err(|e| Error::new(e.span(), format!("{e}; {}", accepted_list())))?;

        for meta in metas {
            attrs.apply(meta)?;
        }
        Ok(attrs)
    }

    /// Fold one `key = value`, or one bare flag, into the set.
    fn apply(&mut self, meta: Meta) -> Result<()> {
        let path = meta.path().clone();
        let key = path
            .get_ident()
            .map(|ident| ident.to_string())
            .ok_or_else(|| Error::new(path.span(), unknown(&quote_path(&path))))?;

        // `idempotent` is the only flag, and it is also accepted as
        // `idempotent = true` so a caller generating attributes does not have
        // to special-case it.
        if let Meta::Path(_) = meta {
            return match key.as_str() {
                "idempotent" => {
                    self.idempotent = true;
                    Ok(())
                }
                _ => Err(Error::new(
                    path.span(),
                    format!("`{key}` needs a value, as in `{key} = ...`"),
                )),
            };
        }

        let lit = literal(&meta)?;
        match key.as_str() {
            "name" => self.name = Some(text(&lit)?),
            "queue" => self.queue = Some(text(&lit)?),
            "priority" => self.priority = Some(integer(&lit)?),
            "max_retries" => self.max_retries = Some(integer(&lit)?),
            "retry_backoff_ms" => self.retry_backoff_ms = Some(duration::millis(&lit)?),
            "retry_max_delay_ms" => self.retry_max_delay_ms = Some(duration::millis(&lit)?),
            "timeout" => self.timeout_ms = Some(duration::millis(&lit)?),
            "expires" => self.expires_in_ms = Some(duration::millis(&lit)?),
            "result_ttl" => self.result_ttl_ms = Some(duration::millis(&lit)?),
            "idempotent" => self.idempotent = boolean(&lit)?,
            "on_excess" => self.on_excess = Some(one_of(&lit, ON_EXCESS, "shedding policy")?),
            "max_concurrent" => self.max_concurrent = Some(integer(&lit)?),
            "max_in_flight_per_task" => self.max_in_flight_per_task = Some(integer(&lit)?),
            "rate_limit" => self.rate_limit = Some(rate(&lit)?),
            "retry_budget" => self.retry_budget = Some(rate(&lit)?),
            "cron" => self.cron = Some(text(&lit)?),
            "timezone" => self.timezone = Some(text(&lit)?),
            _ => return Err(Error::new(path.span(), unknown(&key))),
        }
        Ok(())
    }
}

/// `key` was not one of [`ACCEPTED`].
fn unknown(key: &str) -> String {
    format!("`{key}` is not a task attribute. {}", accepted_list())
}

/// The accepted-attribute list, rendered once.
fn accepted_list() -> String {
    format!("Accepted: {}", ACCEPTED.join(", "))
}

/// A path rendered for an error message.
fn quote_path(path: &syn::Path) -> String {
    path.segments
        .iter()
        .map(|s| s.ident.to_string())
        .collect::<Vec<_>>()
        .join("::")
}

/// The literal on the right of `key = value`.
fn literal(meta: &Meta) -> Result<Lit> {
    match meta {
        Meta::NameValue(nv) => match &nv.value {
            Expr::Lit(ExprLit { lit, .. }) => Ok(lit.clone()),
            other => Err(Error::new(
                other.span(),
                "a task attribute's value has to be a literal",
            )),
        },
        other => Err(Error::new(
            other.span(),
            "expected `key = value` or a bare `idempotent`",
        )),
    }
}

fn text(lit: &Lit) -> Result<String> {
    match lit {
        Lit::Str(value) => Ok(value.value()),
        other => Err(Error::new(other.span(), "expected a string")),
    }
}

fn boolean(lit: &Lit) -> Result<bool> {
    match lit {
        Lit::Bool(value) => Ok(value.value()),
        other => Err(Error::new(other.span(), "expected `true` or `false`")),
    }
}

fn integer<T>(lit: &Lit) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match lit {
        Lit::Int(value) => value.base10_parse::<T>(),
        other => Err(Error::new(other.span(), "expected an integer")),
    }
}

/// One of a closed set of spellings.
fn one_of(lit: &Lit, allowed: &[&str], what: &str) -> Result<String> {
    let value = text(lit)?;
    if allowed.contains(&value.as_str()) {
        return Ok(value);
    }
    Err(Error::new(
        lit.span(),
        format!(
            "`{value}` is not a {what}. Use one of: {}",
            allowed.join(", ")
        ),
    ))
}

/// A rate, `"<count>/<unit>"`.
///
/// Validated here so a typo is a compile error rather than a panic when the
/// worker starts. The value still travels as the string core parses, so this
/// crate is not a second definition of the format.
fn rate(lit: &Lit) -> Result<String> {
    let value = text(lit)?;
    let mut parts = value.split('/');
    let count = parts.next().unwrap_or_default().trim();
    let unit = parts.next().unwrap_or_default().trim();
    let extra = parts.next();

    // At least one, and finite. A bucket built from zero, a negative or a NaN
    // never hands out a token — the backends compare against `tokens < 1.0`, a
    // comparison NaN loses — so the task would simply never dispatch, with
    // nothing anywhere saying why.
    let counted = count
        .parse::<f64>()
        .is_ok_and(|n| n.is_finite() && n >= 1.0);
    let united = matches!(unit, "s" | "m" | "h" | "second" | "minute" | "hour");
    if counted && united && extra.is_none() {
        return Ok(value);
    }
    Err(Error::new(
        lit.span(),
        format!(
            "`{value}` is not a rate. Write it as a count of at least 1 over a unit, as in \
             100/s, 60/m or 1000/h — a rate below one never releases a job"
        ),
    ))
}
