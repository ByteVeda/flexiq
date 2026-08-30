package org.byteveda.flexiq;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import org.byteveda.flexiq.dashboard.DashboardServer;
import org.byteveda.flexiq.errors.ConfigurationException;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.FlexiQEvent;
import org.byteveda.flexiq.interception.InterceptionAnalysis;
import org.byteveda.flexiq.interception.Interceptor;
import org.byteveda.flexiq.internal.JniQueueBackend;
import org.byteveda.flexiq.locks.Lock;
import org.byteveda.flexiq.locks.LockInfo;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.model.CircuitBreakerState;
import org.byteveda.flexiq.model.DeadJob;
import org.byteveda.flexiq.model.DispatchOrder;
import org.byteveda.flexiq.model.EffectiveRetention;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.model.JobDag;
import org.byteveda.flexiq.model.JobError;
import org.byteveda.flexiq.model.JobFilter;
import org.byteveda.flexiq.model.MigrationReport;
import org.byteveda.flexiq.model.Page;
import org.byteveda.flexiq.model.PeriodicInfo;
import org.byteveda.flexiq.model.QueueStats;
import org.byteveda.flexiq.model.ReplayEntry;
import org.byteveda.flexiq.model.RetentionPreview;
import org.byteveda.flexiq.model.StorageBackend;
import org.byteveda.flexiq.model.Subscription;
import org.byteveda.flexiq.model.TaskLog;
import org.byteveda.flexiq.model.TaskLogLevel;
import org.byteveda.flexiq.model.TaskMetric;
import org.byteveda.flexiq.model.Topic;
import org.byteveda.flexiq.model.TopicLogStat;
import org.byteveda.flexiq.model.TopicMessage;
import org.byteveda.flexiq.model.TopicStat;
import org.byteveda.flexiq.model.WorkerInfo;
import org.byteveda.flexiq.model.WorkflowRunInfo;
import org.byteveda.flexiq.predicates.EnqueueGate;
import org.byteveda.flexiq.predicates.Predicate;
import org.byteveda.flexiq.predicates.PredicateStats;
import org.byteveda.flexiq.pubsub.LogConsumerOptions;
import org.byteveda.flexiq.pubsub.PublishOptions;
import org.byteveda.flexiq.pubsub.SubscriptionOptions;
import org.byteveda.flexiq.resources.PoolConfig;
import org.byteveda.flexiq.resources.ResourceContext;
import org.byteveda.flexiq.resources.ResourceDefinition;
import org.byteveda.flexiq.resources.ResourceScope;
import org.byteveda.flexiq.resources.ResourceStat;
import org.byteveda.flexiq.scheduling.PeriodicTask;
import org.byteveda.flexiq.serialization.CodecSerializer;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.serialization.PayloadCodec;
import org.byteveda.flexiq.serialization.Serializer;
import org.byteveda.flexiq.spi.ConditionalSettings;
import org.byteveda.flexiq.spi.QueueBackend;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.worker.Retention;
import org.byteveda.flexiq.worker.Worker;
import org.byteveda.flexiq.workflows.Workflow;
import org.byteveda.flexiq.workflows.WorkflowRun;
import org.byteveda.flexiq.workflows.WorkflowState;
import org.byteveda.flexiq.workflows.WorkflowStatus;
import org.jspecify.annotations.Nullable;

/**
 * The FlexiQ client: a handle to a storage backend through which you enqueue,
 * inspect, and administer jobs across every named queue. Obtain one from
 * {@link #builder()}. Operations scoped to a single named queue (pause/resume)
 * live on the {@link Queue} handle returned by {@link #queue(String)}.
 */
public interface FlexiQ extends AutoCloseable, ConditionalSettings {

    /**
     * Begin configuring a client.
     *
     * @return the builder, defaulting to SQLite
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A handle to one named queue, e.g. {@code flexiq.queue("emails").pause()}.
     *
     * @param name the record's name
     * @return the handle
     */
    Queue queue(String name);

    /**
     * Register cross-cutting middleware (enqueue + worker hooks); returns {@code this}.
     *
     * @param middleware the hook to add to the chain
     * @return {@code this}, for chaining
     */
    FlexiQ use(Middleware middleware);

    /**
     * Subscribe to queue-level events — enqueues, predicate rejections, queue
     * pause/resume, workflow submission/cancellation — plus every event of
     * workers built via {@link #worker()} (lifecycle, job outcomes, workflow
     * progress). The listener narrows the {@link FlexiQEvent} to its concrete
     * type; returns {@code this}.
     *
     * @param name the record's name
     * @param listener called on the emitting thread
     * @return {@code this}, for chaining
     * @throws UnsupportedOperationException when this implementation has no event hub
     */
    default FlexiQ onEvent(EventName name, Consumer<FlexiQEvent> listener) {
        throw new UnsupportedOperationException("this FlexiQ implementation does not support event subscriptions");
    }

    // ── Resources (worker-side dependency injection) ─────────────────

    /**
     * Register a worker-scoped resource resolved in handlers via {@code Resources.use(name)}.
     *
     * @param name the record's name
     * @param factory builds the resource, possibly using others via the context
     * @param <T> the resource type
     * @return {@code this}, for chaining
     */
    <T> FlexiQ resource(String name, Function<ResourceContext, T> factory);

    /**
     * Register a resource with an explicit {@link ResourceScope}.
     *
     * @param name the record's name
     * @param scope how long one instance lives
     * @param factory builds the resource, possibly using others via the context
     * @param <T> the resource type
     * @return {@code this}, for chaining
     */
    <T> FlexiQ resource(String name, ResourceScope scope, Function<ResourceContext, T> factory);

    /**
     * Register a resource with a scope and a disposer run when the scope ends.
     *
     * @param name the record's name
     * @param scope how long one instance lives
     * @param factory builds the resource, possibly using others via the context
     * @param dispose cleanup run when the scope ends
     * @param <T> the resource type
     * @return {@code this}, for chaining
     */
    <T> FlexiQ resource(String name, ResourceScope scope, Function<ResourceContext, T> factory, Consumer<T> dispose);

    /**
     * Register a {@link ResourceScope#POOLED} resource: a bounded pool of
     * instances shared across tasks. Each task checks one instance out for its
     * duration and returns it at task end; {@code pool} bounds capacity and
     * {@code dispose} runs when the pool retires an instance (worker shutdown or
     * {@link PoolConfig#maxLifetime()} expiry).
     *
     * @param name the record's name
     * @param pool bounded-pool sizing
     * @param factory builds the resource, possibly using others via the context
     * @param dispose cleanup run when the scope ends
     * @param <T> the resource type
     * @return {@code this}, for chaining
     */
    <T> FlexiQ resource(String name, PoolConfig pool, Function<ResourceContext, T> factory, Consumer<T> dispose);

    /**
     * Register a pre-built {@link ResourceDefinition} — the escape hatch for knobs
     * the typed overloads above don't cover, such as
     * {@link ResourceDefinition#withReloadable(boolean)}.
     *
     * @param name resource name resolved in handlers via {@code Resources.use(name)}
     * @param definition how to build, scope, and dispose it
     * @return this instance, for chaining
     */
    FlexiQ resource(String name, ResourceDefinition definition);

    /**
     * Per-resource counters (created / disposed / active).
     *
     * @return created, disposed and live counts per registered name
     */
    Map<String, ResourceStat> resourceMetrics();

    /**
     * Hot-reload every resource registered with
     * {@link ResourceDefinition#withReloadable(boolean)} — the programmatic SIGHUP.
     * See {@link #reloadResources(Collection)}.
     *
     * @return per-resource reload success
     */
    Map<String, Boolean> reloadResources();

    /**
     * Hot-reload worker resources: dispose what is cached and rebuild, so the next
     * use sees a fresh instance. Returns {@code name -> success} — an unregistered
     * name reports {@code false} rather than throwing.
     *
     * <p>Resources are reloaded dependency-first, so a dependent rebuilds against the
     * fresh dependency. Only running workers hold instances, so the result is empty
     * when none is running; with several workers a name counts as reloaded only when
     * it reloaded on all of them.
     *
     * @param names the resources to reload, or {@code null} for the reloadable sweep
     * @return per-resource reload success
     */
    Map<String, Boolean> reloadResources(Collection<String> names);

    /**
     * Gate enqueues of {@code taskName} with {@code predicate}: when it rejects,
     * {@link #enqueue} throws and no job is created. Multiple predicates on one
     * task must all pass. Returns {@code this}.
     *
     * @param taskName the task's registered name
     * @param predicate the gate deciding whether an enqueue proceeds
     * @return {@code this}, for chaining
     */
    FlexiQ predicate(String taskName, Predicate predicate);

    /**
     * Gate enqueues of {@code taskName} with a richer {@link EnqueueGate} whose
     * {@link org.byteveda.flexiq.predicates.EnqueueDecision} can allow, skip,
     * defer, or reject. Gates run in registration order and the first non-allow
     * decision wins. Returns {@code this}.
     *
     * @param taskName the task's registered name
     * @param gate the gate deciding what happens to an enqueue
     * @return {@code this}, for chaining
     */
    FlexiQ gate(String taskName, EnqueueGate gate);

    /**
     * What this process's gates decided: one count per gated enqueue, keyed by
     * decision. Enqueues of ungated tasks are not counted.
     *
     * @return a point-in-time snapshot of the counters
     */
    PredicateStats predicateStats();

    /**
     * Register an interceptor that may convert, redirect, or reject each enqueue
     * before it is serialized (see {@link Interceptor}). Returns {@code this}.
     *
     * @param interceptor the hook that inspects each enqueue before serialization
     * @return {@code this}, for chaining
     */
    FlexiQ intercept(Interceptor interceptor);

    /**
     * Dry-run the registered interceptors over {@code (taskName, payload)}: report
     * what they would do without enqueuing anything. A chain that would reject comes
     * back as {@link InterceptionAnalysis#rejected()} instead of throwing.
     *
     * @param taskName the task the enqueue would target
     * @param payload the payload the enqueue would carry
     * @return what the chain would produce, and how far it got
     */
    InterceptionAnalysis analyzeArguments(String taskName, Object payload);

    /**
     * Set an opt-in admission cap on {@code queue}'s pending backlog. Once the
     * queue holds {@code cap} pending jobs, {@link #enqueue} throws
     * {@link org.byteveda.flexiq.errors.QueueFullException}. Enforced
     * producer-side (a non-atomic count-then-insert), so it applies even with no
     * worker running. Returns {@code this}.
     *
     * @param queue the queue name
     * @param cap the admission ceiling on pending jobs
     * @return {@code this}, for chaining
     */
    FlexiQ maxPending(String queue, int cap);

    /**
     * Enable opt-in CoDel load shedding on {@code queue}. Under sustained
     * overload — a job's wait past its eligibility staying above {@code targetMs}
     * for a full {@code intervalMs} — a running worker sheds the stalest jobs to
     * the dead-letter queue (reason prefixed {@code codel:}) instead of running
     * them stale. A transient spike is never shed. Takes effect for workers
     * started after this call. Returns {@code this}.
     *
     * @param queue the queue name
     * @param targetMs the queueing delay CoDel aims to keep below
     * @param intervalMs how often CoDel re-evaluates that target
     * @return {@code this}, for chaining
     */
    FlexiQ codel(String queue, long targetMs, long intervalMs);

    /**
     * Set a queue's same-priority dispatch order. {@link DispatchOrder#LIFO} runs
     * newest-first under overload (a freshness lever); {@link DispatchOrder#FIFO}
     * (default) is the fair oldest-first ordering. Priority always dominates. Honored on
     * SQLite/Postgres; the Redis backend is FIFO-only. Takes effect for workers
     * started after this call. Returns {@code this}.
     *
     * @param queue the queue name
     * @param order the tie-break among same-priority jobs
     * @return {@code this}, for chaining
     */
    FlexiQ dispatchOrder(String queue, DispatchOrder order);

    /**
     * Set a queue's dispatch order from its wire form ({@code "fifo"}/{@code "lifo"}).
     *
     * @param queue the queue name
     * @param order the tie-break among same-priority jobs
     * @return {@code this}, for chaining
     * @deprecated use {@link #dispatchOrder(String, DispatchOrder)}, which rejects a typo
     *     at compile time.
     */
    @Deprecated
    FlexiQ dispatchOrder(String queue, String order);

    // ── Producer ────────────────────────────────────────────────────

    /**
     * Enqueue a typed payload using the task's default options; returns the job id.
     *
     * @param task the task descriptor, carrying its own defaults
     * @param payload the argument the handler is called with
     * @param <T> the payload type
     * @return the new job's id
     */
    <T> String enqueue(Task<T> task, T payload);

    /**
     * Enqueue one job of {@code task}.
     *
     * @param task the task descriptor, carrying its own defaults
     * @param payload the argument the handler is called with
     * @param options the delivery settings for this enqueue
     * @param <T> the payload type
     * @return the new job's id
     */
    <T> String enqueue(Task<T> task, T payload, EnqueueOptions options);

    /**
     * Enqueue by task name with an arbitrary payload and default options.
     *
     * @param taskName the task's registered name
     * @param payload the argument the handler is called with
     * @return the new job's id
     */
    String enqueue(String taskName, @Nullable Object payload);

    /**
     * Like {@link #enqueue(Task, Object)} but gate-aware: returns the job id, or
     * an empty {@code Optional} when a gate skips the enqueue. A gate
     * {@code Reject} still throws.
     *
     * @param task the task descriptor, carrying its own defaults
     * @param payload the argument the handler is called with
     * @param <T> the payload type
     * @return the new job's id, or empty when a gate skipped the enqueue
     */
    <T> Optional<String> tryEnqueue(Task<T> task, T payload);

    /**
     * Enqueue, returning empty when a gate skipped it rather than throwing.
     *
     * @param task the task descriptor, carrying its own defaults
     * @param payload the argument the handler is called with
     * @param options the delivery settings for this enqueue
     * @param <T> the payload type
     * @return the new job's id, or empty when a gate skipped the enqueue
     */
    <T> Optional<String> tryEnqueue(Task<T> task, T payload, EnqueueOptions options);

    /**
     * Gate-aware {@link #enqueue(String, Object)}; empty when a gate skips the enqueue.
     *
     * @param taskName the task's registered name
     * @param payload the argument the handler is called with
     * @return the new job's id, or empty when a gate skipped the enqueue
     */
    Optional<String> tryEnqueue(String taskName, Object payload);

    /**
     * Enqueue a batch in one storage call; returns ids in input order.
     *
     * @param task the task descriptor, carrying its own defaults
     * @param payloads one payload per job
     * @param <T> the payload type
     * @return the job ids, in input order
     */
    <T> List<String> enqueueMany(Task<T> task, List<T> payloads);

    /**
     * Enqueue one job per payload, in a single call.
     *
     * @param task the task descriptor, carrying its own defaults
     * @param payloads one payload per job
     * @param options the delivery settings for this enqueue
     * @param <T> the payload type
     * @return the job ids, in input order
     */
    <T> List<String> enqueueMany(Task<T> task, List<T> payloads, EnqueueOptions options);

    /**
     * Alias of {@link #enqueueMany(Task, List)} in the guide's vocabulary.
     *
     * @param task the task descriptor, carrying its own defaults
     * @param payloads one payload per job
     * @param <T> the payload type
     * @return the job ids, in input order
     */
    <T> List<String> enqueueAll(Task<T> task, List<T> payloads);

    /**
     * One job's current state.
     *
     * @param jobId the job's id
     * @return the job, or empty when no such job exists
     */
    Optional<Job> getJob(String jobId);

    /**
     * Block until the job reaches a terminal state (tests only); throws on timeout.
     *
     * @param jobId the job's id
     * @param timeout how long to wait before giving up
     * @return the job in its terminal state
     */
    Optional<Job> awaitJob(String jobId, Duration timeout);

    /**
     * The job's raw serialized result, if complete.
     *
     * @param jobId the job's id
     * @return the result, or empty when the job is absent or has not completed
     */
    Optional<byte[]> getResult(String jobId);

    /**
     * The job's result deserialized to {@code type}, if complete.
     *
     * @param jobId the job's id
     * @param type the class to decode the stored result into
     * @param <R> the result type
     * @return the result, or empty when the job is absent or has not completed
     */
    <R> Optional<R> getResult(String jobId, Class<R> type);

    /**
     * Cancel a job outright, so it never runs again.
     *
     * @param jobId the job's id
     * @return whether a job was cancelled
     */
    boolean cancel(String jobId);

    /**
     * Ask a running job to stop, for a handler that polls {@link #isCancelRequested}.
     *
     * @param jobId the job's id
     * @return whether the request was recorded
     */
    boolean requestCancel(String jobId);

    /**
     * Whether a cooperative cancel has been requested for this job.
     *
     * @param jobId the job's id
     * @return whether the handler should wind down
     */
    boolean isCancelRequested(String jobId);

    /**
     * Record how far a running job has got.
     *
     * @param jobId the job's id
     * @param progress the percentage the handler is reporting
     */
    void setProgress(String jobId, int progress);

    // ── Inspection ──────────────────────────────────────────────────

    /**
     * Job counts by status across every queue.
     *
     * @return the counts
     */
    QueueStats stats();

    /**
     * Job counts by status for one queue.
     *
     * @param queue the queue name
     * @return the counts
     */
    QueueStats statsByQueue(String queue);

    /**
     * Count pending jobs on {@code queue} — the primitive behind the {@code maxPending} cap.
     *
     * @param queue the queue name
     * @return how many jobs are waiting to run on it
     */
    long countPendingByQueue(String queue);

    /**
     * Job counts by status, one entry per queue.
     *
     * @return the counts, keyed by queue name
     */
    Map<String, QueueStats> statsAllQueues();

    /**
     * A page of jobs matching a filter.
     *
     * @param filter which jobs to return, and how many
     * @return the matching jobs
     */
    List<Job> listJobs(JobFilter filter);

    /**
     * Keyset-paginated {@link #listJobs}, ordered by created time. Pass a page's
     * {@code nextCursor} back as {@code after}; {@code null} starts at the first
     * page, and a {@code null} {@code nextCursor} means the last one.
     *
     * <p>Stays O(page) at any depth, unlike an offset walk. On Redis the status
     * indexes are not seekable, so the keyset is applied in memory — correct, but
     * O(matching rows) rather than O(page).
     *
     * @param filter same predicates as {@link #listJobs}; its {@code offset} is ignored
     * @param after cursor from a previous page, or {@code null} for the first
     * @return the page, carrying the cursor for the next one
     */
    Page<Job> listJobsAfter(JobFilter filter, String after);

    /**
     * Keyset-paginated archived-job listing, ordered by completed time. See
     * {@link #listJobsAfter} for the cursor contract.
     *
     * @param limit page size
     * @param after cursor from a previous page, or {@code null} for the first
     * @return the page, carrying the cursor for the next one
     */
    Page<Job> listArchivedAfter(long limit, String after);

    /**
     * A job's per-attempt error history.
     *
     * @param jobId the job's id
     * @return the attempts, oldest first
     */
    List<JobError> jobErrors(String jobId);

    /**
     * Per-execution metrics within the last {@code sinceMs}; null task = all.
     *
     * @param taskName the task's registered name
     * @param sinceMs a Unix-millisecond floor
     * @return the matching metrics
     */
    List<TaskMetric> metrics(@Nullable String taskName, long sinceMs);

    /**
     * Every worker that is heartbeating.
     *
     * @return the workers
     */
    List<WorkerInfo> listWorkers();

    /**
     * Every configured task's circuit-breaker state.
     *
     * @return one entry per task with a breaker
     */
    List<CircuitBreakerState> listCircuitBreakers();

    // ── Admin ───────────────────────────────────────────────────────

    /**
     * A page of the dead-letter queue.
     *
     * @param limit the page size
     * @param offset how many rows to skip
     * @return the entries
     */
    List<DeadJob> listDead(long limit, long offset);

    /**
     * Dead-letter entries for a single task, newest first.
     *
     * @param taskName the task's registered name
     * @param limit the page size
     * @param offset how many rows to skip
     * @return the entries
     */
    List<DeadJob> listDeadByTask(String taskName, long limit, long offset);

    /**
     * Delete every dead-letter entry for a task; returns the number removed.
     *
     * @param taskName the task's registered name
     * @return how many were removed
     */
    long purgeDeadByTask(String taskName);

    /**
     * Re-enqueue a dead-letter entry; returns the new job id.
     *
     * @param deadId the dead-letter row's id
     * @return the new job's id
     */
    String retryDead(String deadId);

    /**
     * Alias of {@link #retryDead(String)} in the guide's vocabulary.
     *
     * @param deadId the dead-letter row's id
     * @return the new job's id
     */
    String retry(String deadId);

    /**
     * Re-enqueue a copy of a job (recording it in the replay history); returns the new job id.
     *
     * @param jobId the job's id
     * @return the new job's id
     */
    String replayJob(String jobId);

    /**
     * A job's replay history.
     *
     * @param jobId the job's id
     * @return the replays minted from that job
     */
    List<ReplayEntry> getReplayHistory(String jobId);

    /**
     * The dependency DAG reachable from a job (nodes plus {@code from → to} edges).
     *
     * @param jobId the job's id
     * @return the graph
     */
    JobDag jobDag(String jobId);

    /**
     * Discard a dead-letter entry without re-enqueuing it.
     *
     * @param deadId the dead-letter row's id
     * @return whether an entry was removed
     */
    boolean deleteDead(String deadId);

    /**
     * Force a stuck running job back to pending so a healthy worker re-runs it.
     *
     * <p>Releases the job's execution claim atomically and preserves its retry
     * budget. Only use it when the owning worker is confirmed dead or hung: if the
     * old attempt is actually still running, it may finish later and the job runs
     * twice.
     *
     * @param jobId the stuck job's id
     * @return false when the job does not exist or is not running
     */
    boolean requeueJob(String jobId);

    /**
     * Delete dead-letter entries older than a cutoff.
     *
     * @param olderThanMs a Unix-millisecond cutoff; rows older than it are removed
     * @return how many were removed
     */
    long purgeDead(long olderThanMs);

    /**
     * Delete completed jobs older than a cutoff.
     *
     * @param olderThanMs a Unix-millisecond cutoff; rows older than it are removed
     * @return how many were removed
     */
    long purgeCompleted(long olderThanMs);

    /**
     * The names of every currently paused queue.
     *
     * @return the paused queue names
     */
    List<String> listPausedQueues();

    /**
     * Write a settings document, overwriting whatever was there.
     *
     * @param key the settings document's key
     * @param value the content to store
     */
    void setSetting(String key, String value);

    /**
     * Remove a settings document.
     *
     * @param key the settings document's key
     * @return whether a row existed
     */
    boolean deleteSetting(String key);

    /**
     * Every settings document.
     *
     * @return the documents, keyed by key
     */
    Map<String, String> listSettings();

    /**
     * Applies any pending schema changes and reports what ran.
     *
     * <p>Idempotent, and the only path that applies DDL when the client was
     * built with {@code autoMigrate(false)}. Native-only, like the contract
     * floor below.
     *
     * @return what the migration did
     */
    default MigrationReport migrate() {
        throw new UnsupportedOperationException("migrate requires the native backend");
    }

    /**
     * The lowest contract level a process may speak and still open this
     * storage. The contract level is the revision of the shared storage and
     * wire contract an SDK build implements; a build below the floor refuses to
     * open rather than misreading rows its contract never described.
     *
     * <p>The level belongs to the native build, so an implementation backed by
     * something else does not have one — hence the default, which keeps an
     * existing implementation of this interface compiling.
     *
     * @return the contract level this storage refuses to open below
     */
    default int minContract() {
        throw new UnsupportedOperationException("the contract floor requires the native backend");
    }

    /**
     * Whether this backend has a durable-step store, i.e. whether
     * {@code JobContext.current().step()} can commit anything.
     *
     * <p>A capability probe, not the gate: a step session refuses on its own if
     * the store is missing. This is for an application that wants the answer
     * without dispatching a job first.
     *
     * @return {@code false} for a backend that has no step store, including any
     *     implementation of this interface that predates them
     */
    default boolean supportsSteps() {
        return false;
    }

    /**
     * Raises or lowers the contract floor.
     *
     * <p>Raise it only once every process in the deployment has been upgraded —
     * older ones stop opening immediately. A level this build does not itself
     * speak is rejected, since writing it would lock the caller out too.
     * Native-only, for the same reason as {@link #minContract()}.
     *
     * @param level the severity
     */
    default void setMinContract(int level) {
        throw new UnsupportedOperationException("the contract floor requires the native backend");
    }

    /**
     * The retention windows a worker is applying to this queue's namespace, or
     * empty when no worker has swept yet — distinct from retention being
     * disabled, which reports with {@code enabled = false}.
     *
     * @return the published policy, or empty if unreported
     */
    Optional<EffectiveRetention> effectiveRetention();

    /**
     * Preview what a retention purge would delete right now, without deleting
     * anything, following the policy the elected cleaner reported for this
     * namespace — recommended defaults only when no cleaner has swept yet. The
     * counts are a point-in-time snapshot; nothing is deleted.
     *
     * @return the per-table counts a purge would remove
     */
    RetentionPreview dryRunRetention();

    /**
     * Preview what a retention purge would delete under candidate windows,
     * without deleting anything — so a window can be sized before it is set,
     * with no worker reconfiguration. A {@code null} argument previews the
     * reported policy, as in {@link #dryRunRetention()}.
     *
     * @param retention the candidate windows to preview
     * @return the per-table counts a purge would remove
     */
    RetentionPreview dryRunRetention(@Nullable Retention retention);

    // ── Middleware toggles ──────────────────────────────────────────

    /**
     * Stop running {@code middlewareName} for {@code taskName}. Takes effect on
     * the next job — workers read the list per invocation, so no restart is
     * needed. The name is the middleware's fully-qualified class name.
     *
     * @param taskName task to disable it for
     * @param middlewareName fully-qualified class name of the middleware
     */
    void disableMiddleware(String taskName, String middlewareName);

    /**
     * Undo {@link #disableMiddleware}. A no-op when it was not disabled.
     *
     * @param taskName task to re-enable it for
     * @param middlewareName fully-qualified class name of the middleware
     */
    void enableMiddleware(String taskName, String middlewareName);

    /**
     * Middleware names currently disabled for {@code taskName}; empty when none
     * are.
     *
     * @param taskName task to read the list for
     * @return the disabled middleware names
     */
    List<String> listDisabledMiddleware(String taskName);

    // ── Logs ────────────────────────────────────────────────────────

    /**
     * Append one structured log line for a job.
     *
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param level the severity
     * @param message the line itself
     */
    void writeTaskLog(String jobId, String taskName, TaskLogLevel level, String message);

    /**
     * Append one structured log line for a job.
     *
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param level the severity
     * @param message the line itself
     * @param extra structured context as pre-encoded JSON, or {@code null}
     */
    void writeTaskLog(String jobId, String taskName, TaskLogLevel level, String message, @Nullable String extra);

    /**
     * Write a task log at a wire-form level.
     *
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param level the severity
     * @param message the line itself
     * @deprecated use {@link #writeTaskLog(String, String, TaskLogLevel, String)}.
     */
    @Deprecated
    void writeTaskLog(String jobId, String taskName, String level, String message);

    /**
     * Write a task log at a wire-form level, with an extra JSON blob.
     *
     * @param jobId the job's id
     * @param taskName the task's registered name
     * @param level the severity
     * @param message the line itself
     * @param extra structured context as pre-encoded JSON, or {@code null}
     * @deprecated use {@link #writeTaskLog(String, String, TaskLogLevel, String, String)}.
     */
    @Deprecated
    void writeTaskLog(String jobId, String taskName, String level, String message, @Nullable String extra);

    /**
     * Every log line one job emitted.
     *
     * @param jobId the job's id
     * @return the lines, in emission order
     */
    List<TaskLog> getTaskLogs(String jobId);

    /**
     * Logs for a job with id after {@code afterId} (UUIDv7-ordered cursor); null = all.
     *
     * @param jobId the job's id
     * @param afterId the last id already read
     * @return the lines after that cursor
     */
    List<TaskLog> getTaskLogsAfter(String jobId, String afterId);

    /**
     * Logs across jobs filtered by task/level, at or after {@code sinceMs}, capped at
     * {@code limit}. {@code level} is the wire form ({@link TaskLogLevel#wire()}), not the
     * enum: a filter is open by nature — {@code null} means no filter, and an unrecognized
     * value must return nothing rather than throw, since it typically arrives from a query
     * string.
     *
     * @param taskName the task's registered name
     * @param level the severity
     * @param sinceMs a Unix-millisecond floor
     * @param limit the page size
     * @return the matching lines
     */
    List<TaskLog> queryTaskLogs(@Nullable String taskName, @Nullable String level, long sinceMs, long limit);

    // ── Locks ───────────────────────────────────────────────────────

    /**
     * A distributed lock {@code name} with the given TTL; call {@link Lock#acquire()}.
     *
     * @param name the record's name
     * @param ttlMs how long the lock survives without an extend
     * @return the lock, unheld until acquired
     */
    Lock lock(String name, long ttlMs);

    /**
     * A distributed lock {@code name} with a default 30s TTL.
     *
     * @param name the record's name
     * @return the lock, unheld until acquired
     */
    Lock lock(String name);

    /**
     * Acquire {@code name}, run {@code body} if obtained, then release; returns whether it ran.
     *
     * @param name the record's name
     * @param ttlMs how long the lock survives without an extend
     * @param body run while the lock is held
     * @return whether the lock was taken and {@code body} ran
     */
    boolean withLock(String name, long ttlMs, Runnable body);

    /**
     * Who holds a lock right now.
     *
     * @param name the record's name
     * @return the holder, or empty when the lock is free
     */
    Optional<LockInfo> lockInfo(String name);

    /**
     * Alias of {@link #lockInfo(String)} in the guide's vocabulary.
     *
     * @param name the record's name
     * @return the holder, or empty when the lock is free
     */
    Optional<LockInfo> getLockInfo(String name);

    // ── Periodic ────────────────────────────────────────────────────

    /**
     * Register (or replace) a cron task; returns the next fire time (Unix ms).
     *
     * @param task the task descriptor, carrying its own defaults
     * @return the next fire time, in Unix milliseconds
     */
    long registerPeriodic(PeriodicTask task);

    /**
     * Every registered periodic task, enabled or paused.
     *
     * @return the schedules
     */
    List<PeriodicInfo> listPeriodic();

    /**
     * Unschedule a periodic task; false if none had that name.
     *
     * @param name the record's name
     * @return whether a schedule was removed
     */
    boolean deletePeriodic(String name);

    /**
     * Stop a periodic task from firing without removing it; false if none had that name.
     *
     * @param name the record's name
     * @return whether a schedule was changed
     */
    boolean pausePeriodic(String name);

    /**
     * Resume a paused periodic task; false if none had that name.
     *
     * @param name the record's name
     * @return whether a schedule was changed
     */
    boolean resumePeriodic(String name);

    // ── Pub/Sub ─────────────────────────────────────────────────────

    /**
     * Subscribe {@code task} to {@code topic} as an independent, durable
     * subscriber named after the task. Every {@link #publish} to the topic then
     * enqueues one ordinary job of this task; register a handler for it on the
     * worker as usual. Returns {@code this}.
     *
     * @param topic the topic's name
     * @param task the task descriptor, carrying its own defaults
     * @param <T> the task's payload type
     * @return {@code this}, for chaining
     */
    <T> FlexiQ subscribe(String topic, Task<T> task);

    /**
     * As {@link #subscribe(String, Task)} with explicit {@link SubscriptionOptions}.
     * A durable subscription registers immediately; an ephemeral one
     * ({@code durable(false)}) binds to a worker and registers when that worker
     * starts, disappearing once it stops heartbeating.
     *
     * @param topic the topic's name
     * @param task the task descriptor, carrying its own defaults
     * @param options the delivery settings for this enqueue
     * @param <T> the task's payload type
     * @return {@code this}, for chaining
     */
    <T> FlexiQ subscribe(String topic, Task<T> task, SubscriptionOptions options);

    /**
     * Publish a message to {@code topic}: one job per active subscription, each
     * carrying the same serialized payload. Returns the created jobs — empty
     * when the topic has no active subscribers (a valid no-op). Each delivery's
     * notes carry {@code topic} and {@code subscription} for filtering.
     *
     * @param topic the topic's name
     * @param payload the argument the handler is called with
     * @return the delivery jobs the publish created
     */
    List<Job> publish(String topic, Object payload);

    /**
     * As {@link #publish(String, Object)} with {@link PublishOptions}. An
     * {@code idempotencyKey} dedupes per subscriber: republishing the same key
     * yields no new deliveries, and a subscription added later still gets its
     * own copy.
     *
     * @param topic the topic's name
     * @param payload the argument the handler is called with
     * @param options the delivery settings for this enqueue
     * @return the delivery jobs the publish created
     */
    List<Job> publish(String topic, Object payload, PublishOptions options);

    /**
     * Remove a subscription; false if none matched.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @return whether a subscription was removed
     */
    boolean unsubscribe(String topic, String name);

    /**
     * Stop deliveries without unregistering; false if none matched.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @return whether a subscription was changed
     */
    boolean pauseSubscription(String topic, String name);

    /**
     * Resume a paused subscription; false if none matched.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @return whether a subscription was changed
     */
    boolean resumeSubscription(String topic, String name);

    /**
     * Every registered subscription (active or paused), across all topics.
     *
     * @return the subscriptions
     */
    List<Subscription> listSubscriptions();

    /**
     * One topic's active subscriptions.
     *
     * @param topic the topic's name
     * @return the subscriptions
     */
    List<Subscription> listSubscriptions(String topic);

    /**
     * Backlog snapshot per subscription, across all topics. Every registered
     * subscription appears — paused and ephemeral ones included — even with nothing
     * queued, so the full subscriber list comes from one call. Counts are computed
     * live off indexed columns, so this is safe to poll.
     *
     * @return the snapshots
     */
    List<TopicStat> topicStats();

    /**
     * As {@link #topicStats()}, filtered to one topic; a {@code null} topic means no filter.
     *
     * @param topic the topic's name
     * @return the snapshots
     */
    List<TopicStat> topicStats(String topic);

    /**
     * Distinct topics that currently have at least one subscription.
     *
     * @return the topic names
     */
    List<String> listTopics();

    /**
     * Register a durable <b>log</b> subscription: a named cursor over {@code topic}.
     * Unlike {@link #subscribe(String, Task)} it has no handler — the topic's
     * publishes are stored once each and this consumer pulls them with
     * {@link #readTopic(String, String)}, advancing with {@link #ackTopic}. Writes
     * immediately, so register it before the publishes it should see. (On Redis the
     * log is backed by a Stream, but that is transparent.)
     *
     * @param topic the topic's name
     * @param name the record's name
     */
    void subscribeLog(String topic, String name);

    /**
     * As {@link #readTopic(String, String, int)} with a default limit of 100.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @return the messages, oldest first
     */
    List<TopicMessage> readTopic(String topic, String name);

    /**
     * Pull up to {@code limit} messages after a log subscription's cursor, oldest
     * first and exclusive of it. Empty when caught up. Decode each {@code payload}
     * with the queue's serializer. At-least-once: process, then {@link #ackTopic}
     * the last {@code id}.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @param limit the page size
     * @return the messages, oldest first
     */
    List<TopicMessage> readTopic(String topic, String name, int limit);

    /**
     * Advance a log subscription's cursor to {@code cursor} (a message id). A
     * high-water mark: acking an id acks everything up to it. Monotonic — acking an
     * older id is a no-op. Returns false when nothing moved.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @param cursor the id of the last message handled
     * @return whether the cursor moved
     */
    boolean ackTopic(String topic, String name, String cursor);

    /**
     * As {@link #leaseTopic(String, String, int, Duration)} with a default limit of 100 and a 30s visibility.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @return the leased messages
     */
    List<TopicMessage> leaseTopic(String topic, String name);

    /**
     * Lease up to {@code limit} messages for <b>per-message</b> consumption. Unlike
     * {@link #readTopic}'s cursor, each message is leased for {@code visibility} and
     * tracked individually: {@link #ackMessage} it when done, or {@link #nackMessage}
     * to redeliver it now. A lease that expires un-acked is redelivered, so one poison
     * message no longer blocks its siblings. In-flight (leased, un-expired) messages
     * are skipped; oldest first.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @param limit the page size
     * @param visibility how long each lease holds before the message redelivers
     * @return the leased messages
     */
    List<TopicMessage> leaseTopic(String topic, String name, int limit, Duration visibility);

    /**
     * Ack one leased message — the delivery is done and never redelivered. Returns
     * false when there was no un-acked delivery to ack.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @param messageId the leased message's id
     * @return whether a delivery was acked
     */
    boolean ackMessage(String topic, String name, String messageId);

    /**
     * Nack one leased message — make it available for redelivery now, rather than
     * waiting out the visibility timeout. Returns false when there was no un-acked
     * delivery to nack.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @param messageId the leased message's id
     * @return whether a delivery was nacked
     */
    boolean nackMessage(String topic, String name, String messageId);

    /**
     * Lag snapshot per log subscription (cursor position and un-acked backlog).
     *
     * @return the snapshots
     */
    List<TopicLogStat> topicLogStats();

    /**
     * Declare a <b>log</b> topic so its publishes are retained even with no
     * subscriber, removing the late-join boundary: without a declaration a log
     * message is stored only when a log subscription already exists at publish
     * time. Idempotent — re-declaring keeps the topic. Equivalent to
     * {@link #declareTopic(String, Duration)} with no retention bound (messages
     * kept until consumed).
     *
     * @param name the topic name
     * @return this instance, for chaining
     */
    FlexiQ declareTopic(String name);

    /**
     * Declare a <b>log</b> topic with a retention bound. Each stored message
     * expires {@code retention} after it was published, so the retention sweep can
     * reclaim a sub-less backlog. A {@code null} retention keeps messages until a
     * subscriber consumes them. Idempotent — re-declaring updates the window.
     *
     * @param name the topic name
     * @param retention how long a sub-less message is retained, or {@code null} for unbounded
     * @return this instance, for chaining
     */
    FlexiQ declareTopic(String name, @Nullable Duration retention);

    /**
     * Every declared topic in the registry.
     *
     * @return the declared topics
     */
    List<Topic> listDeclaredTopics();

    /**
     * Register a <b>managed</b> consumer of log {@code topic}: a durable log
     * subscription plus, once a worker runs, a daemon thread that pulls each stored
     * message, decodes it to {@code payloadType}, invokes {@code handler}, and
     * advances the cursor — the {@link #readTopic(String, String, int)}/
     * {@link #ackTopic(String, String, String)} loop callers otherwise hand-write.
     * Registers immediately, so declare it before the publishes it should see;
     * a producer-only process still retains the topic's publishes. Returns {@code this}.
     *
     * @param topic the topic's name
     * @param name the record's name
     * @param payloadType the type each message decodes to
     * @param handler what runs per decoded message
     * @param <T> the message payload type
     * @return {@code this}, for chaining
     */
    <T> FlexiQ logConsumer(String topic, String name, Class<T> payloadType, Consumer<T> handler);

    /**
     * As {@link #logConsumer(String, String, Class, Consumer)} with explicit
     * {@link LogConsumerOptions} (poll interval, batch size, error policy).
     *
     * @param topic the topic's name
     * @param name the record's name
     * @param payloadType the type each message decodes to
     * @param handler what runs per decoded message
     * @param options the delivery settings for this enqueue
     * @param <T> the message payload type
     * @return {@code this}, for chaining
     */
    <T> FlexiQ logConsumer(
            String topic, String name, Class<T> payloadType, Consumer<T> handler, LogConsumerOptions options);

    // ── Workflows ───────────────────────────────────────────────────

    /**
     * Submit a workflow DAG; returns a handle to the run.
     *
     * @param workflow the definition to submit
     * @return the run handle
     */
    WorkflowRun submitWorkflow(Workflow workflow);

    /**
     * Submit a workflow, supplying per-step payloads keyed by step name. A step's
     * effective payload is {@code payloads.get(name)} when present, else the
     * payload baked into the step. Pairs with the structural
     * {@code Workflow.stepAfter(name, task, deps...)} form.
     *
     * @param workflow the definition to submit
     * @param payloads one payload per job
     * @return the run handle
     */
    WorkflowRun submitWorkflow(Workflow workflow, Map<String, Object> payloads);

    /**
     * Current status of a workflow run, or empty if it no longer exists.
     *
     * @param runId the workflow run's id
     * @return the run and its nodes, or empty when no such run exists
     */
    Optional<WorkflowStatus> workflowStatus(String runId);

    /**
     * Cancel a workflow run: skip its pending nodes and mark it cancelled.
     *
     * @param runId the workflow run's id
     */
    void cancelWorkflow(String runId);

    /**
     * Workflow run summaries, filtered by definition name and/or state, paged. Nulls mean no
     * filter. {@code state} is the wire form ({@link WorkflowState#wire()}) rather than the enum,
     * so that a bare {@code null} filter stays unambiguous; unlike the log level, an
     * unrecognized state is rejected by the core.
     *
     * @param definitionName narrow to one definition, or {@code null} for every one
     * @param state narrow to one state, or {@code null} for every state
     * @param limit the page size
     * @param offset how many rows to skip
     * @return the matching runs
     */
    List<WorkflowRunInfo> listWorkflowRuns(
            @Nullable String definitionName, @Nullable String state, long limit, long offset);

    /**
     * A single workflow run summary, or empty if the run no longer exists.
     *
     * @param runId the workflow run's id
     * @return the run, or empty when no such run exists
     */
    Optional<WorkflowRunInfo> getWorkflowRun(String runId);

    /**
     * Sub-workflow runs spawned by a run.
     *
     * @param runId the workflow run's id
     * @return the child runs
     */
    List<WorkflowRunInfo> getWorkflowChildren(String runId);

    /**
     * The serialized DAG JSON backing a run, or empty if the run/definition is gone.
     *
     * @param runId the workflow run's id
     * @return the graph as JSON, or empty when no such run exists
     */
    Optional<String> getWorkflowDag(String runId);

    // ── Worker ──────────────────────────────────────────────────────

    /**
     * Begin building a worker over this client.
     *
     * @return the builder
     */
    Worker.Builder worker();

    /**
     * Stop every worker started from this client — the programmatic equivalent of
     * SIGINT/SIGTERM. Each one stops dispatching, drains its in-flight handlers, and
     * disposes its worker-scoped resources, exactly as {@link Worker#close()} does.
     * A no-op when no worker is running, and safe alongside a direct
     * {@link Worker#close()} — closing twice does nothing the second time.
     *
     * <p>The client itself stays usable; {@link #close()} releases the storage handle.
     */
    void shutdown();

    // ── Dashboard ───────────────────────────────────────────────────

    /**
     * Start the dashboard HTTP server over this client on {@code port}
     * (0 = ephemeral). Serves openly — no authentication.
     *
     * @param port the port to bind, or 0 for an ephemeral one
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    default DashboardServer dashboard(int port) throws IOException {
        return DashboardServer.start(this, port);
    }

    /**
     * As {@link #dashboard(int)}; {@code authEnabled=true} enables the session login flow.
     *
     * @param port the port to bind, or 0 for an ephemeral one
     * @param authEnabled {@code true} for password users, sessions and RBAC
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    default DashboardServer dashboard(int port, boolean authEnabled) throws IOException {
        return DashboardServer.start(this, port, authEnabled);
    }

    /**
     * As {@link #dashboard(int)} but gating {@code /api/*} behind a shared {@code token}.
     *
     * @param port the port to bind, or 0 for an ephemeral one
     * @param token the shared bearer token every request must carry
     * @return the running server, to be closed when the process stops
     * @throws IOException if the port cannot be bound
     */
    default DashboardServer dashboard(int port, String token) throws IOException {
        return DashboardServer.start(this, port, token);
    }

    /**
     * Close the client and release its native handle. Idempotent.
     */
    @Override
    void close();

    /** Configures and opens a {@link FlexiQ} client. */
    final class Builder {
        /** A builder defaulting to a brokerless SQLite store under {@code .flexiq/}. */
        public Builder() {}

        private static final ObjectMapper JSON = new ObjectMapper();
        // Mirrors the Python/Node SDKs: a brokerless SQLite store under .flexiq/.
        private static final String DEFAULT_SQLITE_DB = ".flexiq/flexiq.db";

        private final Map<String, Object> options = new LinkedHashMap<>();
        private Serializer serializer = new JsonSerializer();
        private final List<PayloadCodec> codecs = new ArrayList<>();
        private final Map<String, PayloadCodec> namedCodecs = new LinkedHashMap<>();
        /** Per-hook middleware budget; {@code null} leaves the worker's own default. */
        private @Nullable Duration middlewareTimeout;

        /**
         * The storage to open over, by its wire name.
         *
         * @param backend the storage to open over
         * @return {@code this}, for chaining
         */
        public Builder backend(String backend) {
            // Normalize at the boundary so callers may pass "SQLite"/"REDIS"; the
            // default-DSN branch and the native layer then see a canonical name.
            options.put("backend", backend == null ? null : backend.toLowerCase(Locale.ROOT));
            return this;
        }

        /**
         * Type-safe variant of {@link #backend(String)}. Prefer this over the string overload.
         *
         * @param backend the storage to open over
         * @return {@code this}, for chaining
         */
        public Builder backend(StorageBackend backend) {
            options.put("backend", backend == null ? null : backend.wire());
            return this;
        }

        /**
         * Connection string: a file path for SQLite, a URL for Postgres/Redis.
         *
         * @param dsn the connection string
         * @return {@code this}, for chaining
         */
        public Builder url(String dsn) {
            options.put("dsn", dsn);
            return this;
        }

        /**
         * Shortcut for {@code backend("sqlite")} using the default {@code .flexiq/flexiq.db}.
         *
         * @return {@code this}, for chaining
         */
        public Builder sqlite() {
            return backend("sqlite");
        }

        /**
         * Shortcut for {@code backend("sqlite").url(path)}.
         *
         * @param path where the SQLite file lives
         * @return {@code this}, for chaining
         */
        public Builder sqlite(String path) {
            return backend("sqlite").url(path);
        }

        /**
         * Shortcut for {@code backend("postgres").url(url)}.
         *
         * @param url the connection string
         * @return {@code this}, for chaining
         */
        public Builder postgres(String url) {
            return backend("postgres").url(url);
        }

        /**
         * Shortcut for {@code backend("redis").url(url)}.
         *
         * @param url the connection string
         * @return {@code this}, for chaining
         */
        public Builder redis(String url) {
            return backend("redis").url(url);
        }

        /**
         * How many connections the backend keeps open.
         *
         * @param poolSize how many connections the backend keeps open
         * @return {@code this}, for chaining
         */
        public Builder poolSize(int poolSize) {
            options.put("poolSize", poolSize);
            return this;
        }

        /**
         * Put the PostgreSQL tables in a schema other than the default.
         *
         * @param schema the PostgreSQL schema the tables live in
         * @return {@code this}, for chaining
         */
        public Builder schema(String schema) {
            options.put("schema", schema);
            return this;
        }

        /**
         * Prefix every Redis key, so two deployments can share one Redis.
         *
         * @param prefix the key prefix Redis rows are stored under
         * @return {@code this}, for chaining
         */
        public Builder prefix(String prefix) {
            options.put("prefix", prefix);
            return this;
        }

        /**
         * Read and write under a namespace other than the default.
         *
         * @param namespace the deployment namespace this client reads and writes under
         * @return {@code this}, for chaining
         */
        public Builder namespace(String namespace) {
            options.put("namespace", namespace);
            return this;
        }

        /**
         * Whether opening applies pending schema changes. {@code true} (the
         * default) keeps the existing behavior; {@code false} gates every
         * schema change behind {@link FlexiQ#migrate()}, for a deployment
         * whose database credentials do not permit DDL at runtime. Until
         * migrate has run, queries fail — the tables do not exist yet.
         *
         * @param autoMigrate {@code false} to require an explicit {@link #migrate()}
         * @return {@code this}, for chaining
         */
        public Builder autoMigrate(boolean autoMigrate) {
            options.put("autoMigrate", autoMigrate);
            return this;
        }

        /**
         * What encodes payloads and decodes results. Must match the worker's.
         *
         * @param serializer what encodes payloads and decodes results
         * @return {@code this}, for chaining
         */
        public Builder serializer(Serializer serializer) {
            this.serializer = serializer;
            return this;
        }

        /**
         * Apply payload codecs (compress/encrypt/sign) around the serializer, in
         * order on the way out and reversed on the way in. The same chain must be
         * configured on producers and workers. Returns {@code this}.
         *
         * @param codecs the codecs to register, keyed by their own names
         * @return {@code this}, for chaining
         */
        public Builder codec(PayloadCodec... codecs) {
            this.codecs.addAll(Arrays.asList(codecs));
            return this;
        }

        /**
         * Register a named codec for per-task selection (e.g. via {@code Task.codecs(...)}
         * or the {@code @Encrypted}/{@code @Compressed} annotations). The same names
         * must be registered on producers and workers. Returns {@code this}.
         *
         * @param name the record's name
         * @param codec the codec to register
         * @return {@code this}, for chaining
         */
        public Builder codec(String name, PayloadCodec codec) {
            this.namedCodecs.put(name, codec);
            return this;
        }

        /**
         * How long one middleware hook may take before the worker interrupts it.
         *
         * <p>A task's own timeout bounds its handler and nothing else, so a
         * {@code before}, {@code after}, {@code onError} or {@code onSleep} that
         * blocks holds the attempt open past that limit. Past this budget the
         * hook's thread is interrupted, the overrun is logged against the
         * middleware that caused it, and the chain carries on — failing an
         * attempt over its instrumentation is the failure mode the hooks exist
         * to avoid. Defaults to 5 seconds.
         *
         * @param timeout the per-hook budget; {@link Duration#ZERO} disables the bound
         * @return {@code this}, for chaining
         */
        public Builder middlewareTimeout(Duration timeout) {
            this.middlewareTimeout = Objects.requireNonNull(timeout, "middlewareTimeout");
            return this;
        }

        /** The serializer wrapped in the configured codec chain (if any). */
        private Serializer effectiveSerializer() {
            return codecs.isEmpty() ? serializer : new CodecSerializer(serializer, codecs);
        }

        /**
         * Open over an explicit backend, e.g. an in-memory fake in tests.
         *
         * @param backend the storage to open over
         * @return the client
         */
        public FlexiQ open(QueueBackend backend) {
            return new DefaultFlexiQ(backend, effectiveSerializer(), namedCodecs, middlewareTimeout);
        }

        /**
         * Open the native backend described by the configured options.
         *
         * @return the client
         */
        public FlexiQ open() {
            String backend = (String) options.getOrDefault("backend", "sqlite");
            if ("sqlite".equals(backend)) {
                String dsn = (String) options.computeIfAbsent("dsn", key -> DEFAULT_SQLITE_DB);
                ensureSqliteParentDir(dsn);
            } else if (!options.containsKey("dsn")) {
                throw new ConfigurationException("url (dsn) is required");
            }
            return new DefaultFlexiQ(
                    JniQueueBackend.open(encodeOptions()), effectiveSerializer(), namedCodecs, middlewareTimeout);
        }

        /** Create the SQLite file's parent directory (skip in-memory databases). */
        private static void ensureSqliteParentDir(String dsn) {
            if (dsn.equals(":memory:") || dsn.startsWith("file::memory:")) {
                return;
            }
            Path parent = Paths.get(dsn).getParent();
            if (parent == null) {
                return;
            }
            try {
                Files.createDirectories(parent);
            } catch (IOException e) {
                throw new ConfigurationException("failed to create sqlite directory " + parent, e);
            }
        }

        private String encodeOptions() {
            try {
                return JSON.writeValueAsString(options);
            } catch (Exception e) {
                throw new ConfigurationException("failed to encode open options", e);
            }
        }
    }
}
