package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A registered worker (heartbeat + identity). Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WorkerInfo {
    /** The worker's id, minted when it registered. */
    public final String workerId;

    /** JSON array of the queues it dispatches from. */
    public final String queues;

    /** The lifecycle state its last heartbeat reported. */
    public final String status;

    /** When it last heartbeated, in Unix milliseconds; staleness is how a dead worker is spotted. */
    public final long lastHeartbeat;

    /** When it registered, in Unix milliseconds, or {@code null}. */
    public final Long startedAt;

    /** The host it runs on. */
    public final String hostname;

    /** Its process id on that host, or {@code null}. */
    public final Integer pid;

    /** How it runs handlers — a thread pool, a process pool, and so on. */
    public final String poolType;

    /** How many jobs it can run at once. */
    public final int threads;

    /** JSON array of the routing tags it advertised. */
    public final String tags;
    /** JSON array of resource names the worker advertised at registration; may be null. */
    public final String resources;
    /** JSON object of per-resource health written by the worker's heartbeat; may be null. */
    public final String resourceHealth;

    /** SDK that registered the worker (e.g. {@code java}); null from a shell predating this. */
    public final String sdk;
    /** Release of that SDK; null from a shell predating this. */
    public final String sdkVersion;

    /**
     * Fingerprint of the tasks this worker has handlers for, so the one worker in a fleet that
     * registered a different set is visible without going host by host. Null from a shell
     * predating this, and from a worker with nothing registered.
     */
    public final String registryFingerprint;

    /**
     * Decoded from the core's JSON worker registry row.
     *
     * @param workerId the worker's id, minted when it registered
     * @param queues JSON array of the queues it dispatches from
     * @param status the lifecycle state its last heartbeat reported
     * @param lastHeartbeat when it last heartbeated, in Unix milliseconds; staleness is how a dead worker is spotted
     * @param startedAt when it registered, in Unix milliseconds, or {@code null}
     * @param hostname the host it runs on
     * @param pid its process id on that host, or {@code null}
     * @param poolType how it runs handlers — a thread pool, a process pool, and so on
     * @param threads how many jobs it can run at once
     * @param tags JSON array of the routing tags it advertised
     * @param resources JSON array of resource names the worker advertised at registration; may be null
     * @param resourceHealth JSON object of per-resource health written by the worker's heartbeat; may be null
     * @param sdk SDK that registered the worker (e.g. {@code java}); null from a shell predating this
     * @param sdkVersion release of that SDK; null from a shell predating this
     * @param registryFingerprint fingerprint of the tasks this worker has handlers for, so the one worker in a fleet
     *     that registered a different set is visible without going host by host. Null from a
     *     shell predating this, and from a worker with nothing registered
     */
    @JsonCreator
    public WorkerInfo(
            @JsonProperty("workerId") String workerId,
            @JsonProperty("queues") String queues,
            @JsonProperty("status") String status,
            @JsonProperty("lastHeartbeat") long lastHeartbeat,
            @JsonProperty("startedAt") Long startedAt,
            @JsonProperty("hostname") String hostname,
            @JsonProperty("pid") Integer pid,
            @JsonProperty("poolType") String poolType,
            @JsonProperty("threads") int threads,
            @JsonProperty("tags") String tags,
            @JsonProperty("resources") String resources,
            @JsonProperty("resourceHealth") String resourceHealth,
            @JsonProperty("sdk") String sdk,
            @JsonProperty("sdkVersion") String sdkVersion,
            @JsonProperty("registryFingerprint") String registryFingerprint) {
        this.workerId = workerId;
        this.queues = queues;
        this.status = status;
        this.lastHeartbeat = lastHeartbeat;
        this.startedAt = startedAt;
        this.hostname = hostname;
        this.pid = pid;
        this.poolType = poolType;
        this.threads = threads;
        this.tags = tags;
        this.resources = resources;
        this.resourceHealth = resourceHealth;
        this.sdk = sdk;
        this.sdkVersion = sdkVersion;
        this.registryFingerprint = registryFingerprint;
    }
}
