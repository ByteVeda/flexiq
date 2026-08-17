//! Build core task/queue configuration from JS option inputs.

use napi::bindgen_prelude::Result;
use taskito_core::resilience::circuit_breaker::CircuitBreakerConfig;
use taskito_core::resilience::rate_limiter::RateLimitConfig;
use taskito_core::resilience::retry::RetryPolicy;
use taskito_core::scheduler::shed::OnExcess;
use taskito_core::scheduler::{QueueConfig, TaskConfig};

use crate::config::{CircuitBreakerInput, QueueConfigInput, TaskConfigInput};
use crate::error::invalid_arg;

const DEFAULT_RETRY_BASE_MS: i64 = 1_000;
const DEFAULT_RETRY_MAX_MS: i64 = 300_000;
const DEFAULT_MAX_RETRIES: i32 = 3;
const DEFAULT_HALF_OPEN_PROBES: i32 = 5;
const DEFAULT_HALF_OPEN_SUCCESS_RATE: f64 = 0.8;

/// Parse an optional rate spec, failing fast on a malformed value rather than
/// silently disabling the cap (a misconfigured limit is a config error).
///
/// Names the option and what carries it — several options share this grammar,
/// and both tasks and queues configure one, so the error cannot say which
/// config is wrong without both. `scope` is the kind ("task", "queue"), `name`
/// its name.
fn parse_rate_spec(
    field: &str,
    scope: &str,
    name: &str,
    spec: Option<&str>,
) -> Result<Option<RateLimitConfig>> {
    match spec {
        Some(s) => RateLimitConfig::parse(s).map(Some).ok_or_else(|| {
            invalid_arg(format!(
                "invalid {field} '{s}' on {scope} '{name}' (expected e.g. '100/m')"
            ))
        }),
        None => Ok(None),
    }
}

/// Parse an optional `onExcess` spec. Unset means the default, `defer`; an
/// unrecognized value is a config error, not a silent fall back to keeping the
/// job — a caller who asked to shed and got deferral would never notice.
fn parse_on_excess(name: &str, spec: Option<&str>) -> Result<OnExcess> {
    match spec {
        Some(s) => OnExcess::parse(s).ok_or_else(|| {
            invalid_arg(format!(
                "invalid onExcess '{s}' on task '{name}' (expected 'defer' or 'drop')"
            ))
        }),
        None => Ok(OnExcess::default()),
    }
}

/// Build a [`TaskConfig`] (retry policy, rate limit, concurrency cap) from JS input.
pub fn task_config(input: &TaskConfigInput) -> Result<TaskConfig> {
    Ok(TaskConfig {
        retry_policy: RetryPolicy {
            max_retries: input.max_retries.unwrap_or(DEFAULT_MAX_RETRIES),
            base_delay_ms: input.retry_base_delay_ms.unwrap_or(DEFAULT_RETRY_BASE_MS),
            max_delay_ms: input.retry_max_delay_ms.unwrap_or(DEFAULT_RETRY_MAX_MS),
            custom_delays_ms: None,
        },
        rate_limit: parse_rate_spec(
            "rateLimit",
            "task",
            &input.name,
            input.rate_limit.as_deref(),
        )?,
        on_excess: parse_on_excess(&input.name, input.on_excess.as_deref())?,
        circuit_breaker: input
            .circuit_breaker
            .as_ref()
            .map(circuit_breaker_config)
            .transpose()?,
        // Same "100/m" grammar as rate_limit, so one parser serves both.
        retry_budget: parse_rate_spec(
            "retryBudget",
            "task",
            &input.name,
            input.retry_budget.as_deref(),
        )?,
        max_concurrent: input.max_concurrent,
        max_in_flight_per_task: input.max_in_flight_per_task.map(|n| n.max(1) as usize),
    })
}

/// Build a [`CircuitBreakerConfig`] from JS input, validating bounds and filling
/// half-open defaults. Rejects non-positive/out-of-range values that would make
/// the breaker misbehave.
fn circuit_breaker_config(input: &CircuitBreakerInput) -> Result<CircuitBreakerConfig> {
    if input.threshold <= 0 {
        return Err(invalid_arg("circuitBreaker.threshold must be > 0"));
    }
    if input.window_ms <= 0 {
        return Err(invalid_arg("circuitBreaker.windowMs must be > 0"));
    }
    if input.cooldown_ms <= 0 {
        return Err(invalid_arg("circuitBreaker.cooldownMs must be > 0"));
    }
    let half_open_max_probes = input
        .half_open_max_probes
        .unwrap_or(DEFAULT_HALF_OPEN_PROBES);
    if half_open_max_probes <= 0 {
        return Err(invalid_arg("circuitBreaker.halfOpenMaxProbes must be > 0"));
    }
    let half_open_success_rate = input
        .half_open_success_rate
        .unwrap_or(DEFAULT_HALF_OPEN_SUCCESS_RATE);
    if !(0.0..=1.0).contains(&half_open_success_rate) {
        return Err(invalid_arg(
            "circuitBreaker.halfOpenSuccessRate must be in 0.0..=1.0",
        ));
    }
    Ok(CircuitBreakerConfig {
        threshold: input.threshold,
        window_ms: input.window_ms,
        cooldown_ms: input.cooldown_ms,
        half_open_max_probes,
        half_open_success_rate,
    })
}

/// Build a [`QueueConfig`] (rate limit, concurrency cap) from JS input.
pub fn queue_config(input: &QueueConfigInput) -> Result<QueueConfig> {
    Ok(QueueConfig {
        rate_limit: parse_rate_spec(
            "rateLimit",
            "queue",
            &input.name,
            input.rate_limit.as_deref(),
        )?,
        max_concurrent: input.max_concurrent,
    })
}

/// Build the optional per-queue CoDel config: both bounds must be present and
/// positive. Kept separate from [`queue_config`] since the core registers CoDel
/// through its own path, not on `QueueConfig`.
pub fn queue_codel(
    input: &QueueConfigInput,
) -> Option<taskito_core::scheduler::codel::CodelConfig> {
    match (input.codel_target_ms, input.codel_interval_ms) {
        (Some(target_ms), Some(interval_ms)) if target_ms > 0 && interval_ms > 0 => {
            Some(taskito_core::scheduler::codel::CodelConfig {
                target_ms,
                interval_ms,
            })
        }
        _ => None,
    }
}
