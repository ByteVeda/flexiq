/// Per-task circuit breaker with half-open probing.
pub mod circuit_breaker;
/// Dead-letter queue manager.
pub mod dlq;
/// Storage-backed token-bucket rate limiter.
pub mod rate_limiter;
/// Retry policies (exponential backoff, custom delays).
pub mod retry;
