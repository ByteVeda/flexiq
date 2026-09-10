//! Turning one function into a task.

use proc_macro2::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Error, FnArg, Ident, ItemFn, Pat, Result, Type};

use crate::attrs::TaskAttrs;

/// One declared parameter: the name to bind and the type to decode into.
struct Param {
    ident: Ident,
    ty: Type,
}

/// Expand `#[task]` over `item`.
pub fn task(attrs: TaskAttrs, item: ItemFn) -> Result<TokenStream> {
    reject_unsupported(&item)?;

    let params = params(&item)?;
    let idents: Vec<&Ident> = params.iter().map(|p| &p.ident).collect();
    let types: Vec<&Type> = params.iter().map(|p| &p.ty).collect();

    let vis = &item.vis;
    let ident = &item.sig.ident;
    let name = attrs.name.clone().unwrap_or_else(|| ident.to_string());
    let inputs = &item.sig.inputs;
    let output = &item.sig.output;
    let body = &item.block;
    let doc = docs(&item);

    let config = config(&attrs);
    let defaults = defaults(&attrs);
    let decode = decode(&idents, &types, &name);

    // `run` keeps the caller's own signature so the body compiles unchanged and
    // stays directly callable from a unit test. `call` mirrors it, so a wrong
    // argument to an enqueue is the same compile error as a wrong argument to
    // the function.
    Ok(quote! {
        #[allow(non_camel_case_types)]
        #doc
        #vis struct #ident;

        impl #ident {
            /// Build a call to this task, ready to enqueue.
            #vis fn call(#inputs) -> ::flexiq::TaskCall<#ident> {
                let args = ::std::vec![
                    #(
                        ::flexiq::__private::to_wire(&#idents)
                            .expect("a task argument that does not encode"),
                    )*
                ];
                ::flexiq::TaskCall::from_args(::flexiq::__private::encode_args(&args))
            }

            /// Run the task body directly, without a queue.
            #vis fn run(#inputs) #output #body
        }

        impl ::flexiq::Task for #ident {
            const NAME: &'static str = #name;

            fn config() -> ::flexiq::TaskConfig {
                #config
            }

            fn defaults() -> ::flexiq::EnqueueOptions {
                #defaults
            }

            fn run_encoded(
                job: &::flexiq::Job,
            ) -> ::flexiq::Outcome<::std::option::Option<::std::vec::Vec<u8>>> {
                let _ = job;
                #decode
                let value = Self::run(#(#idents),*)?;
                let encoded = ::flexiq::__private::to_wire(&value).map_err(|e| {
                    ::flexiq::Abort::Fail(::flexiq::TaskError::fatal(::std::format!(
                        "task `{}` returned a value that does not encode: {}",
                        #name,
                        e
                    )))
                })?;
                ::std::result::Result::Ok(::std::option::Option::Some(
                    ::flexiq::__private::encode_result(&encoded),
                ))
            }
        }
    })
}

/// Decode the payload into the declared parameters.
///
/// A task with no parameters skips this entirely: an empty argument array is
/// not a unit value, and asking a deserializer to read one as `()` fails on a
/// payload that is perfectly correct.
fn decode(idents: &[&Ident], types: &[&Type], name: &str) -> TokenStream {
    if idents.is_empty() {
        return quote! {};
    }
    quote! {
        let ( #(#idents,)* ): ( #(#types,)* ) =
            ::flexiq::__private::decode_args(&job.payload).map_err(|e| {
                ::flexiq::Abort::Fail(::flexiq::TaskError::fatal(::std::format!(
                    "task `{}` could not read its arguments: {}",
                    #name,
                    e
                )))
            })?;
    }
}

/// The scheduler's half of the attributes.
fn config(attrs: &TaskAttrs) -> TokenStream {
    let mut policy = Vec::new();
    if let Some(value) = attrs.max_retries {
        policy.push(quote! { config.retry_policy.max_retries = #value; });
    }
    if let Some(value) = attrs.retry_backoff_ms {
        policy.push(quote! { config.retry_policy.base_delay_ms = #value; });
    }
    if let Some(value) = attrs.retry_max_delay_ms {
        policy.push(quote! { config.retry_policy.max_delay_ms = #value; });
    }
    if let Some(value) = &attrs.on_excess {
        policy.push(quote! {
            config.on_excess = ::flexiq::scheduler::shed::OnExcess::parse(#value)
                .expect("a shedding policy the macro already validated");
        });
    }
    if let Some(value) = attrs.max_concurrent {
        policy.push(quote! { config.max_concurrent = ::std::option::Option::Some(#value); });
    }
    if let Some(value) = attrs.max_in_flight_per_task {
        policy
            .push(quote! { config.max_in_flight_per_task = ::std::option::Option::Some(#value); });
    }
    if let Some(value) = &attrs.rate_limit {
        policy.push(quote! {
            config.rate_limit = ::std::option::Option::Some(
                ::flexiq::RateLimitConfig::parse(#value)
                    .expect("a rate the macro already validated"),
            );
        });
    }
    if let Some(value) = &attrs.retry_budget {
        policy.push(quote! {
            config.retry_budget = ::std::option::Option::Some(
                ::flexiq::RateLimitConfig::parse(#value)
                    .expect("a rate the macro already validated"),
            );
        });
    }

    quote! {
        #[allow(unused_mut)]
        let mut config = ::flexiq::TaskConfig::default();
        #(#policy)*
        config
    }
}

/// The enqueue half of the attributes.
fn defaults(attrs: &TaskAttrs) -> TokenStream {
    let mut calls = Vec::new();
    if let Some(value) = &attrs.queue {
        calls.push(quote! { .queue(#value) });
    }
    if let Some(value) = attrs.priority {
        calls.push(quote! { .priority(#value) });
    }
    if let Some(value) = attrs.max_retries {
        calls.push(quote! { .max_retries(#value) });
    }
    if let Some(value) = attrs.timeout_ms {
        calls.push(quote! { .timeout_ms(#value) });
    }
    if let Some(value) = attrs.expires_in_ms {
        calls.push(quote! { .expires_in_ms(#value) });
    }
    if let Some(value) = attrs.result_ttl_ms {
        calls.push(quote! { .result_ttl_ms(#value) });
    }
    if attrs.idempotent {
        calls.push(quote! { .idempotent() });
    }

    quote! { ::flexiq::EnqueueOptions::default() #(#calls)* }
}

/// The declared parameters, or an error naming the one that cannot be bound.
fn params(item: &ItemFn) -> Result<Vec<Param>> {
    item.sig
        .inputs
        .iter()
        .map(|arg| match arg {
            FnArg::Receiver(receiver) => Err(Error::new(
                receiver.span(),
                "a task is a free function: it cannot take `self`",
            )),
            FnArg::Typed(typed) => match &*typed.pat {
                Pat::Ident(pat) => Ok(Param {
                    ident: pat.ident.clone(),
                    ty: (*typed.ty).clone(),
                }),
                other => Err(Error::new(
                    other.span(),
                    "a task's parameters have to be plain names: a payload is decoded \
                     into them one by one, so there is nothing to destructure yet",
                )),
            },
        })
        .collect()
}

/// The shapes this macro does not expand.
fn reject_unsupported(item: &ItemFn) -> Result<()> {
    if let Some(token) = item.sig.asyncness {
        return Err(Error::new(
            token.span(),
            "an async task is not supported yet: the shell's pool runs every handler on a \
             blocking thread. Track the work with an async runtime inside the body, or use \
             `flexiq_core::Worker::register_async` directly",
        ));
    }
    if !item.sig.generics.params.is_empty() {
        return Err(Error::new(
            item.sig.generics.span(),
            "a task cannot be generic: one registration is one task name, and a generic \
             function is a family of them",
        ));
    }
    if let Some(variadic) = &item.sig.variadic {
        return Err(Error::new(variadic.span(), "a task cannot be variadic"));
    }
    Ok(())
}

/// The function's own doc comments, moved onto the type that replaces it.
///
/// Without this a documented task becomes an undocumented public type, which
/// `#![deny(missing_docs)]` in the caller's crate would reject — and the
/// caller's documentation would vanish from a name they still use.
fn docs(item: &ItemFn) -> TokenStream {
    let attrs: Vec<_> = item
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .collect();

    if attrs.is_empty() {
        let generated = format!("The `{}` task.", item.sig.ident);
        return quote! { #[doc = #generated] };
    }
    quote! { #(#attrs)* }
}
