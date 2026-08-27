package org.byteveda.flexiq.dashboard.api;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.support.Http;
import org.byteveda.flexiq.model.JobFilter;
import org.byteveda.flexiq.model.JobStatus;
import org.jspecify.annotations.Nullable;

/**
 * Read and action handlers for jobs, queues, dead-letters, metrics, and
 * workers. Each returns the snake_case JSON body (via {@link Contract}) or
 * {@code null} to signal 404.
 */
public final class CoreHandlers {
    private static final long DEFAULT_LIMIT = 50;

    private final FlexiQ queue;

    /**
     * Handlers reading and acting on one queue.
     *
     * @param queue what every route below reads from and acts on
     */
    public CoreHandlers(FlexiQ queue) {
        this.queue = queue;
    }

    /**
     * Job counts by status, across every queue.
     *
     * @return the counts
     */
    public Object stats() {
        return Contract.stats(queue.stats());
    }

    /**
     * Job counts by status, one entry per queue.
     *
     * @return the counts, keyed by queue name
     */
    public Object statsByQueue() {
        Map<String, Object> out = new LinkedHashMap<>();
        queue.statsAllQueues().forEach((name, stats) -> out.put(name, Contract.stats(stats)));
        return out;
    }

    /**
     * Which queues are currently paused.
     *
     * @return the paused queue names
     */
    public Object queuesPaused() {
        return queue.listPausedQueues();
    }

    /**
     * A page of jobs.
     *
     * @param query {@code status}, {@code queue}, {@code task}, {@code limit} and
     *     {@code offset}, each optional
     * @return the matching jobs
     */
    public Object listJobs(Map<String, String> query) {
        JobFilter.Builder filter = JobFilter.builder();
        if (query.containsKey("status")) {
            filter.status(JobStatus.fromWire(query.get("status")));
        }
        if (query.containsKey("queue")) {
            filter.queue(query.get("queue"));
        }
        if (query.containsKey("task")) {
            filter.task(query.get("task"));
        }
        if (query.containsKey("limit")) {
            filter.limit(Http.intParam(query, "limit", 0));
        }
        if (query.containsKey("offset")) {
            filter.offset(Http.intParam(query, "offset", 0));
        }
        return queue.listJobs(filter.build()).stream().map(Contract::job).collect(Collectors.toList());
    }

    /**
     * One job.
     *
     * @param id the job's id
     * @return the job, or {@code null} for a 404
     */
    public @Nullable Object job(String id) {
        return queue.getJob(id).map(Contract::job).orElse(null);
    }

    /**
     * A page of the dead-letter queue.
     *
     * @param query {@code limit} and {@code offset}, both optional
     * @return the dead jobs
     */
    public Object listDead(Map<String, String> query) {
        long limit = Http.longParam(query, "limit", DEFAULT_LIMIT);
        long offset = Http.longParam(query, "offset", 0);
        return queue.listDead(limit, offset).stream().map(Contract::dead).collect(Collectors.toList());
    }

    /**
     * Every worker that is heartbeating.
     *
     * @return the workers
     */
    public Object listWorkers() {
        return queue.listWorkers().stream().map(Contract::worker).collect(Collectors.toList());
    }

    /**
     * Cancel a job.
     *
     * @param id the job's id
     * @return whether it was cancelled, under {@code cancelled}
     */
    public Object cancel(String id) {
        return Map.of("cancelled", queue.cancel(id));
    }

    /**
     * Re-enqueue a dead-lettered job.
     *
     * @param id the dead job's id
     * @return the new job's id, under {@code id}
     */
    public Object retryDead(String id) {
        return Map.of("id", queue.retryDead(id));
    }

    /**
     * Stop dispatching from a queue.
     *
     * @param name the queue's name
     * @return {@code {"ok": true}}
     */
    public Object pause(String name) {
        queue.queue(name).pause();
        return Map.of("ok", true);
    }

    /**
     * Resume dispatching from a queue.
     *
     * @param name the queue's name
     * @return {@code {"ok": true}}
     */
    public Object resume(String name) {
        queue.queue(name).resume();
        return Map.of("ok", true);
    }

    /**
     * Logs across jobs filtered by task/level; {@code since} is a lookback in seconds.
     *
     * @param query {@code task}, {@code level}, {@code since} (default 3600) and
     *     {@code limit} (default 100), each optional
     * @return the matching log lines
     */
    public Object logs(Map<String, String> query) {
        long sinceSeconds = Http.longParam(query, "since", 3600);
        long sinceMs = System.currentTimeMillis() - sinceSeconds * 1000;
        long limit = Http.longParam(query, "limit", 100);
        return queue.queryTaskLogs(query.get("task"), query.get("level"), sinceMs, limit).stream()
                .map(Contract::taskLog)
                .collect(Collectors.toList());
    }

    /**
     * One job's log lines.
     *
     * @param id the job's id
     * @return the lines, in the order the worker emitted them
     */
    public Object jobLogs(String id) {
        return queue.getTaskLogs(id).stream().map(Contract::taskLog).collect(Collectors.toList());
    }

    /**
     * Run a job's payload again as a fresh job.
     *
     * @param id the job to replay
     * @return the new job's id, under {@code replay_job_id}
     */
    public Object replayJob(String id) {
        return Map.of("replay_job_id", queue.replayJob(id));
    }

    /**
     * Every replay minted from one job.
     *
     * @param id the original job's id
     * @return the replay entries
     */
    public Object replayHistory(String id) {
        return queue.getReplayHistory(id).stream().map(Contract::replayEntry).collect(Collectors.toList());
    }

    /**
     * The step graph a job recorded, for the job detail view.
     *
     * @param id the job's id
     * @return the graph
     */
    public Object jobDag(String id) {
        return Contract.jobDag(queue.jobDag(id));
    }
}
