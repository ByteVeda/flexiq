package org.byteveda.flexiq.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a task handler. A compile-time annotation processor
 * generates, per enclosing class {@code Foo}, a {@code FooTasks} companion that
 * holds a typed {@link org.byteveda.flexiq.task.Task} constant per handler plus
 * a {@code bind(Worker.Builder, Foo)} method — no runtime reflection.
 *
 * <p>The handler method must take exactly one parameter (the payload) and may
 * return a result or {@code void}. The generated task name is {@link #value()},
 * or the method name when {@code value} is empty.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface TaskHandler {
    /** Task name; defaults to the method name when empty. */
    String value() default "";

    /** Target queue; the default queue when empty. */
    String queue() default "";

    /** Max retries; core default when negative. */
    int maxRetries() default -1;

    /** Timeout in milliseconds; core default when negative. */
    long timeoutMs() default -1;

    /** Priority; 0 (default) is left unset. */
    int priority() default 0;

    /** Auto-derive an idempotency {@code uniqueKey} from the payload on every enqueue. */
    boolean idempotent() default false;

    /**
     * Rate-limit spec like {@code "100/m"} ({@code s}, {@code m} and {@code h}
     * suffixes); empty (default) leaves the task unthrottled. A malformed spec
     * fails the worker's start rather than running unthrottled.
     */
    String rateLimit() default "";

    /**
     * What a saturated rate limit does to this task's jobs: {@code "defer"}
     * (the default) reschedules it, {@code "drop"} sheds it to the dead-letter
     * queue. Empty means {@code "defer"}; anything else fails the worker's
     * start rather than silently keeping the job.
     */
    String onExcess() default "";

    /**
     * Cap on how fast this task may <em>retry</em>, across all of its jobs — a
     * spec like {@code "100/m"}; empty (default) leaves retries uncapped. Once
     * spent, failures dead-letter instead of retrying.
     */
    String retryBudget() default "";

    /**
     * Cap on concurrently-running jobs of this task across the cluster; 0
     * (default) leaves it uncapped.
     */
    int maxConcurrent() default 0;

    /**
     * Cap on this task's share of one worker's dispatch slots, so a slow task
     * cannot occupy the whole pool; 0 (default) lets it use the whole pool.
     */
    int maxInFlightPerTask() default 0;

    /** Circuit-breaker failure threshold; 0 (default) leaves the breaker off. */
    int circuitBreakerThreshold() default 0;

    /** Rolling window, in seconds, over which failures count toward the threshold. */
    long circuitBreakerWindowSeconds() default 60;

    /** How long, in seconds, the breaker stays open before admitting half-open probes. */
    long circuitBreakerCooldownSeconds() default 300;

    /** Probe runs admitted while half-open. */
    int circuitBreakerHalfOpenProbes() default 5;

    /** Probe success rate (0.0–1.0) required to re-close the breaker. */
    double circuitBreakerHalfOpenSuccessRate() default 0.8;
}
