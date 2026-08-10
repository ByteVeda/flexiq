package org.byteveda.taskito.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** A registered worker (heartbeat + identity). Timestamps are Unix milliseconds. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class WorkerInfo {
    public final String workerId;
    public final String queues;
    public final String status;
    public final long lastHeartbeat;
    public final Long startedAt;
    public final String hostname;
    public final Integer pid;
    public final String poolType;
    public final int threads;
    public final String tags;
    /** JSON array of resource names the worker advertised at registration; may be null. */
    public final String resources;
    /** JSON object of per-resource health written by the worker's heartbeat; may be null. */
    public final String resourceHealth;

    /** SDK that registered the worker (e.g. {@code java}); null from a shell predating this. */
    public final String sdk;
    /** Release of that SDK; null from a shell predating this. */
    public final String sdkVersion;

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
            @JsonProperty("sdkVersion") String sdkVersion) {
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
    }
}
