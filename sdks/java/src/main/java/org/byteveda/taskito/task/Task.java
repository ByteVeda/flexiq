package org.byteveda.taskito.task;

import com.fasterxml.jackson.core.type.TypeReference;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;

/**
 * Typed task descriptor: a name, its payload type, and default enqueue options.
 *
 * <p>For generic payloads (e.g. {@code Map<String, Object>}) use the
 * {@link TypeReference} factory, which {@code Class} tokens cannot express.
 * The fluent option methods each return a new descriptor (the type is immutable).
 */
public final class Task<T> {
    private final String name;
    private final Type payloadType;
    private final EnqueueOptions options;
    private final @Nullable RetryPolicy retryPolicy;
    private final List<String> codecs;
    private final boolean idempotent;
    private final @Nullable CircuitBreakerConfig circuitBreaker;
    private final @Nullable String rateLimit;
    private final @Nullable String retryBudget;
    private final @Nullable Integer maxConcurrent;
    private final @Nullable Integer maxInFlightPerTask;
    private final @Nullable Predicate<Throwable> retryOn;

    private Task(
            String name,
            Type payloadType,
            EnqueueOptions options,
            @Nullable RetryPolicy retryPolicy,
            List<String> codecs,
            boolean idempotent,
            @Nullable CircuitBreakerConfig circuitBreaker,
            @Nullable String rateLimit,
            @Nullable String retryBudget,
            @Nullable Integer maxConcurrent,
            @Nullable Integer maxInFlightPerTask,
            @Nullable Predicate<Throwable> retryOn) {
        this.name = Objects.requireNonNull(name, "task name must not be null");
        if (name.trim().isEmpty()) {
            throw new IllegalArgumentException("task name must not be blank");
        }
        this.payloadType = Objects.requireNonNull(payloadType, "payloadType must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
        this.retryPolicy = retryPolicy;
        this.codecs = List.copyOf(codecs);
        this.idempotent = idempotent;
        this.circuitBreaker = circuitBreaker;
        this.rateLimit = rateLimit;
        this.retryBudget = retryBudget;
        this.maxConcurrent = maxConcurrent;
        this.maxInFlightPerTask = maxInFlightPerTask;
        this.retryOn = retryOn;
    }

    /** A task whose payload deserializes to {@code payloadType}. */
    public static <T> Task<T> of(String name, Class<T> payloadType) {
        return new Task<>(
                name, payloadType, EnqueueOptions.none(), null, List.of(), false, null, null, null, null, null, null);
    }

    /** A task whose payload deserializes to a generic type, e.g. {@code new TypeReference<List<Foo>>(){}}. */
    public static <T> Task<T> of(String name, TypeReference<T> payloadType) {
        return new Task<>(
                name,
                payloadType.getType(),
                EnqueueOptions.none(),
                null,
                List.of(),
                false,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** A copy of this task with the given default options. */
    public Task<T> withOptions(EnqueueOptions options) {
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task whose retries use {@code retryPolicy}'s backoff curve.
     * Registered with the worker on {@code start()}; the retry budget still comes
     * from {@link #maxRetries}.
     */
    public Task<T> retryPolicy(RetryPolicy retryPolicy) {
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task that retries a failure only when {@code retryOn} accepts
     * the thrown exception. Returning {@code false} dead-letters the job at once,
     * whatever retry budget is left — for permanent failures (a malformed payload,
     * a 4xx) that no amount of retrying fixes. Unset retries every exception, and
     * so does a predicate that itself throws.
     *
     * <p>Evaluated by the worker that ran the handler, so unlike the backoff curve
     * it never reaches the scheduler. It sees every exception raised while running
     * the task, not only the handler's: {@code before}/{@code after} middleware,
     * payload decoding and result serialization can fail too, so a whitelist
     * predicate dead-letters those as well. A timeout is detected outside the
     * handler and always consumes a retry.
     *
     * <p>A handler that throws {@link org.byteveda.taskito.errors.RetryableException}
     * or {@link org.byteveda.taskito.errors.NonRetryableException} decides for itself
     * — those bypass this predicate.
     */
    public Task<T> retryOn(Predicate<Throwable> retryOn) {
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task whose payload is passed through the named {@link
     * org.byteveda.taskito.serialization.PayloadCodec}s (in order on enqueue,
     * reversed on the worker). Each name must be registered via
     * {@code Taskito.builder().codec(name, codec)} on producers and workers.
     */
    public Task<T> codecs(String... codecs) {
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                Arrays.asList(codecs),
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task that auto-derives a {@code uniqueKey} from the payload on every
     * enqueue, so a duplicate enqueue is a no-op while the first job is pending or running.
     * A per-enqueue {@link EnqueueOptions.Builder#idempotent(boolean)} overrides this default.
     */
    public Task<T> idempotent(boolean idempotent) {
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task guarded by {@code circuitBreaker}. The worker registers it on
     * {@code start()}; once the breaker opens, the scheduler stops dispatching this task until
     * it recovers.
     */
    public Task<T> circuitBreaker(CircuitBreakerConfig circuitBreaker) {
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task throttled to {@code rateLimit}, a spec like {@code "100/m"}
     * ({@code s}, {@code m} and {@code h} suffixes). The worker registers it on
     * {@code start()} and rejects a malformed spec rather than running unthrottled.
     */
    public Task<T> rateLimit(String rateLimit) {
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task allowed at most {@code maxConcurrent} jobs running at once
     * across the cluster. The scheduler counts running jobs before dispatch, so this
     * costs a database read. {@code null} or {@code 0} means no cap — matching the
     * annotation's sentinel, and because a literal cap of zero would stop the task
     * from ever dispatching.
     */
    public Task<T> maxConcurrent(@Nullable Integer maxConcurrent) {
        maxConcurrent = uncappedIfZero(maxConcurrent);
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task whose <em>retries</em> are capped at {@code retryBudget},
     * a spec like {@code "100/m"} — across all of its jobs, not per job. Once spent,
     * failures dead-letter instead of retrying, so a broken dependency cannot become
     * a retry storm. Distinct from {@link #maxRetries}, which bounds one job rather
     * than the rate.
     */
    public Task<T> retryBudget(String retryBudget) {
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /**
     * A copy of this task allowed at most {@code maxInFlightPerTask} of one worker's
     * dispatch slots, so a slow task cannot occupy the whole pool and starve the
     * others. In-process and free, unlike {@link #maxConcurrent}, which is
     * cluster-wide and costs a database read. {@code null} or {@code 0} lets it use
     * the whole pool, matching the annotation's sentinel.
     */
    public Task<T> maxInFlightPerTask(@Nullable Integer maxInFlightPerTask) {
        maxInFlightPerTask = uncappedIfZero(maxInFlightPerTask);
        return new Task<>(
                name,
                payloadType,
                options,
                retryPolicy,
                codecs,
                idempotent,
                circuitBreaker,
                rateLimit,
                retryBudget,
                maxConcurrent,
                maxInFlightPerTask,
                retryOn);
    }

    /** A cap of zero is the annotation's "unset" sentinel, never a literal zero. */
    private static @Nullable Integer uncappedIfZero(@Nullable Integer cap) {
        return cap != null && cap == 0 ? null : cap;
    }

    public Task<T> queue(String queue) {
        return withOptions(options.toBuilder().queue(queue).build());
    }

    public Task<T> priority(int priority) {
        return withOptions(options.toBuilder().priority(priority).build());
    }

    public Task<T> maxRetries(int maxRetries) {
        return withOptions(options.toBuilder().maxRetries(maxRetries).build());
    }

    /** Alias of {@link #maxRetries} in the guide's vocabulary. */
    public Task<T> retries(int retries) {
        return maxRetries(retries);
    }

    public Task<T> timeoutMs(long timeoutMs) {
        return withOptions(options.toBuilder().timeoutMs(timeoutMs).build());
    }

    public Task<T> timeout(Duration timeout) {
        return timeoutMs(timeout.toMillis());
    }

    public Task<T> delayMs(long delayMs) {
        return withOptions(options.toBuilder().delayMs(delayMs).build());
    }

    public Task<T> delay(Duration delay) {
        return delayMs(delay.toMillis());
    }

    public String name() {
        return name;
    }

    /** The payload type — a {@code Class} or a generic {@code Type} from a {@link TypeReference}. */
    public Type payloadType() {
        return payloadType;
    }

    public EnqueueOptions options() {
        return options;
    }

    /** The retry-backoff curve for this task, or {@code null} for the core defaults. */
    public @Nullable RetryPolicy retryPolicy() {
        return retryPolicy;
    }

    /** Names of the payload codecs applied to this task (empty if none). */
    public List<String> codecNames() {
        return codecs;
    }

    /** Whether this task auto-derives an idempotency {@code uniqueKey} by default. */
    public boolean idempotent() {
        return idempotent;
    }

    /** This task's circuit-breaker configuration, or {@code null} when none is set. */
    public @Nullable CircuitBreakerConfig circuitBreaker() {
        return circuitBreaker;
    }

    /** This task's rate-limit spec (e.g. {@code "100/m"}), or {@code null} when unthrottled. */
    public @Nullable String rateLimit() {
        return rateLimit;
    }

    /** This task's retry-rate cap (e.g. {@code "100/m"}), or {@code null} when uncapped. */
    public @Nullable String retryBudget() {
        return retryBudget;
    }

    /** Cap on this task's concurrently-running jobs, or {@code null} when uncapped. */
    public @Nullable Integer maxConcurrent() {
        return maxConcurrent;
    }

    /** Cap on this task's share of one worker's dispatch slots, or {@code null} when uncapped. */
    public @Nullable Integer maxInFlightPerTask() {
        return maxInFlightPerTask;
    }

    /** Predicate deciding whether a thrown exception is retryable, or {@code null} to retry all. */
    public @Nullable Predicate<Throwable> retryOn() {
        return retryOn;
    }
}
