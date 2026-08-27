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
    /**
     * Task name; defaults to the method name when empty.
     *
     * @return the task name, or empty to use the method name
     */
    String value() default "";

    /**
     * Target queue; the default queue when empty.
     *
     * @return the queue name, or empty for the default queue
     */
    String queue() default "";

    /**
     * Max retries; core default when negative.
     *
     * @return the retry ceiling, or a negative value to leave the core's default
     */
    int maxRetries() default -1;

    /**
     * Timeout in milliseconds; core default when negative.
     *
     * @return the timeout in milliseconds, or a negative value to leave the core's default
     */
    long timeoutMs() default -1;

    /**
     * Priority; 0 (default) is left unset.
     *
     * @return the priority, or 0 to leave it unset
     */
    int priority() default 0;

    /**
     * Auto-derive an idempotency {@code uniqueKey} from the payload on every enqueue.
     *
     * @return {@code true} to key every enqueue on its payload, so a repeat is dropped
     */
    boolean idempotent() default false;

    /**
     * Rate-limit spec like {@code "100/m"} ({@code s}, {@code m} and {@code h}
     * suffixes); empty (default) leaves the task unthrottled. A malformed spec
     * fails the worker's start rather than running unthrottled.
     *
     * @return the spec, or empty to leave the task unthrottled
     */
    String rateLimit() default "";

    /**
     * What a saturated rate limit does to this task's jobs: {@code "defer"}
     * (the default) reschedules it, {@code "drop"} sheds it to the dead-letter
     * queue. Empty means {@code "defer"}; anything else fails the worker's
     * start rather than silently keeping the job.
     *
     * @return {@code "defer"}, {@code "drop"}, or empty for {@code "defer"}
     */
    String onExcess() default "";

    /**
     * Cap on how fast this task may <em>retry</em>, across all of its jobs — a
     * spec like {@code "100/m"}; empty (default) leaves retries uncapped. Once
     * spent, failures dead-letter instead of retrying.
     *
     * @return the spec, or empty to leave retries uncapped
     */
    String retryBudget() default "";

    /**
     * Cap on concurrently-running jobs of this task across the cluster; 0
     * (default) leaves it uncapped.
     *
     * @return the cluster-wide ceiling, or 0 to leave it uncapped
     */
    int maxConcurrent() default 0;

    /**
     * Cap on this task's share of one worker's dispatch slots, so a slow task
     * cannot occupy the whole pool; 0 (default) lets it use the whole pool.
     *
     * @return the per-worker ceiling, or 0 to allow the whole pool
     */
    int maxInFlightPerTask() default 0;

    /**
     * Circuit-breaker failure threshold; 0 (default) leaves the breaker off.
     *
     * @return how many failures in the window trip the breaker, or 0 to leave it off
     */
    int circuitBreakerThreshold() default 0;

    /**
     * Rolling window, in seconds, over which failures count toward the threshold.
     *
     * @return the window, in seconds
     */
    long circuitBreakerWindowSeconds() default 60;

    /**
     * How long, in seconds, the breaker stays open before admitting half-open probes.
     *
     * @return the cooldown, in seconds
     */
    long circuitBreakerCooldownSeconds() default 300;

    /**
     * Probe runs admitted while half-open.
     *
     * @return how many probes are let through before the breaker decides
     */
    int circuitBreakerHalfOpenProbes() default 5;

    /**
     * Probe success rate (0.0–1.0) required to re-close the breaker.
     *
     * @return the share of probes that must succeed, from 0.0 to 1.0
     */
    double circuitBreakerHalfOpenSuccessRate() default 0.8;
}
