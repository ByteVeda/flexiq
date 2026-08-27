package org.byteveda.flexiq.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.errors.SerializationException;
import org.jspecify.annotations.Nullable;

/**
 * Mesh scheduling options for {@link Worker.Builder#mesh(MeshOptions)}. Workers
 * sharing a cluster gossip over SWIM (UDP) for peer discovery and steal work
 * from busy peers (TCP); the database stays the source of truth, so mesh is a
 * pure throughput optimization layered under the normal worker.
 *
 * <p>Build with {@link #builder()}. The only field most clusters set is the
 * seed list (and a distinct {@link Builder#port(int)} per host when co-located):
 *
 * <pre>{@code
 * Worker w = flexiq.worker()
 *     .handle(task, handler)
 *     .mesh(MeshOptions.builder().port(7946).seed("10.0.0.2:7946").build())
 *     .start();
 * }</pre>
 */
public final class MeshOptions {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final int gossipPort;
    private final List<String> seeds;
    private final String bindAddr;
    private final @Nullable String advertiseAddr;
    private final boolean enableStealing;
    private final double affinityWeight;
    private final int localBufferCapacity;
    private final int maxStealBatch;
    private final int stealThreshold;
    private final int virtualNodes;
    private final int stealRateLimit;
    private final @Nullable String encryptionKey;

    private MeshOptions(Builder b) {
        this.gossipPort = b.gossipPort;
        this.seeds = List.copyOf(b.seeds);
        this.bindAddr = b.bindAddr;
        this.advertiseAddr = b.advertiseAddr;
        this.enableStealing = b.enableStealing;
        this.affinityWeight = b.affinityWeight;
        this.localBufferCapacity = b.localBufferCapacity;
        this.maxStealBatch = b.maxStealBatch;
        this.stealThreshold = b.stealThreshold;
        this.virtualNodes = b.virtualNodes;
        this.stealRateLimit = b.stealRateLimit;
        this.encryptionKey = b.encryptionKey;
    }

    /**
     * A builder pre-loaded with the core's mesh defaults.
     *
     * @return the builder; usually only the seed list needs setting
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * The {@code MeshConfig} JSON the native worker reads. Every non-optional
     * field is emitted (the core config has no serde defaults), with the SWIM
     * protocol timings left at their core defaults.
     */
    String toConfigJson() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("gossip_port", gossipPort);
        config.put("steal_port", gossipPort + 1);
        config.put("bind_addr", bindAddr);
        config.put("seeds", seeds);
        config.put("protocol_period_ms", 500);
        config.put("indirect_ping_count", 3);
        config.put("suspicion_multiplier", 4);
        config.put("virtual_nodes", virtualNodes);
        config.put("local_buffer_capacity", localBufferCapacity);
        config.put("max_steal_batch", maxStealBatch);
        config.put("steal_threshold", stealThreshold);
        config.put("affinity_weight", affinityWeight);
        config.put("enable_stealing", enableStealing);
        config.put("steal_rate_limit", stealRateLimit);
        if (advertiseAddr != null) {
            config.put("advertise_addr", advertiseAddr);
        }
        if (encryptionKey != null) {
            config.put("encryption_key", encryptionKey);
        }
        try {
            return JSON.writeValueAsString(config);
        } catch (Exception e) {
            throw new SerializationException("failed to encode mesh config", e);
        }
    }

    /** Mirrors {@code flexiq_mesh::MeshConfig} defaults; only the seed list is usually set. */
    public static final class Builder {
        /** A builder holding the core's defaults; reach it through {@link MeshOptions#builder()}. */
        public Builder() {}

        private int gossipPort = 7946;
        private final List<String> seeds = new ArrayList<>();
        private String bindAddr = "0.0.0.0";
        private @Nullable String advertiseAddr;
        private boolean enableStealing = true;
        private double affinityWeight = 0.7;
        private int localBufferCapacity = 64;
        private int maxStealBatch = 4;
        private int stealThreshold = 2;
        private int virtualNodes = 150;
        private int stealRateLimit = 10;
        private @Nullable String encryptionKey;

        /**
         * Gossip (UDP) port; the work-stealing (TCP) port is {@code port + 1}.
         *
         * @param port the gossip port, within {@code 1..=65534}
         * @return {@code this}, for chaining
         */
        public Builder port(int port) {
            if (port < 1 || port > 65534) {
                throw new IllegalArgumentException("mesh port must be in 1..=65534 (steal port is port+1)");
            }
            this.gossipPort = port;
            return this;
        }

        /**
         * Add a seed peer ({@code host:port}) used for initial cluster join.
         *
         * @param hostPort a peer to contact on join, as {@code host:port}
         * @return {@code this}, for chaining
         */
        public Builder seed(String hostPort) {
            this.seeds.add(hostPort);
            return this;
        }

        /**
         * Add several seed peers at once.
         *
         * @param seeds peers to contact on join, each as {@code host:port}
         * @return {@code this}, for chaining
         */
        public Builder seeds(List<String> seeds) {
            this.seeds.addAll(seeds);
            return this;
        }

        /**
         * Listen address for gossip and steal (default {@code 0.0.0.0}).
         *
         * @param bindAddr the local address to listen on
         * @return {@code this}, for chaining
         */
        public Builder bindAddr(String bindAddr) {
            this.bindAddr = bindAddr;
            return this;
        }

        /**
         * Address advertised to peers; required when {@code bindAddr} is {@code 0.0.0.0} across hosts.
         *
         * @param advertiseAddr the address peers should reach this worker on
         * @return {@code this}, for chaining
         */
        public Builder advertiseAddr(String advertiseAddr) {
            this.advertiseAddr = advertiseAddr;
            return this;
        }

        /**
         * Whether an idle worker may pull queued jobs off a busier peer. On by default.
         *
         * @param enableStealing {@code false} to leave every job with its hashed owner
         * @return {@code this}, for chaining
         */
        public Builder enableStealing(boolean enableStealing) {
            this.enableStealing = enableStealing;
            return this;
        }

        /**
         * 0.0 ignores affinity, 1.0 is strict affinity (default 0.7).
         *
         * @param affinityWeight how strongly a job is pinned to its hashed owner
         * @return {@code this}, for chaining
         */
        public Builder affinityWeight(double affinityWeight) {
            this.affinityWeight = affinityWeight;
            return this;
        }

        /**
         * Max jobs prefetched into the local deque (default 64).
         *
         * @param localBufferCapacity how many jobs to hold locally ahead of dispatch
         * @return {@code this}, for chaining
         */
        public Builder localBufferCapacity(int localBufferCapacity) {
            this.localBufferCapacity = localBufferCapacity;
            return this;
        }

        /**
         * How many jobs one steal moves at a time.
         *
         * @param maxStealBatch the batch ceiling
         * @return {@code this}, for chaining
         */
        public Builder maxStealBatch(int maxStealBatch) {
            this.maxStealBatch = maxStealBatch;
            return this;
        }

        /**
         * How far behind a peer must be before its jobs are eligible to be stolen.
         *
         * @param stealThreshold the backlog difference that justifies a steal
         * @return {@code this}, for chaining
         */
        public Builder stealThreshold(int stealThreshold) {
            this.stealThreshold = stealThreshold;
            return this;
        }

        /**
         * Replicas per worker on the hash ring; more evens the distribution out.
         *
         * @param virtualNodes the replica count
         * @return {@code this}, for chaining
         */
        public Builder virtualNodes(int virtualNodes) {
            this.virtualNodes = virtualNodes;
            return this;
        }

        /**
         * Max steal requests served per peer per second; 0 is unlimited (default 10).
         *
         * @param stealRateLimit the per-peer ceiling, or 0 for unlimited
         * @return {@code this}, for chaining
         */
        public Builder stealRateLimit(int stealRateLimit) {
            this.stealRateLimit = stealRateLimit;
            return this;
        }

        /**
         * Base64 (32-byte) key XOR-applied to gossip datagrams; deters casual sniffing only.
         *
         * @param encryptionKey the base64 key every peer must share
         * @return {@code this}, for chaining
         */
        public Builder encryptionKey(String encryptionKey) {
            this.encryptionKey = encryptionKey;
            return this;
        }

        /**
         * Freeze the settings collected so far.
         *
         * @return the immutable options
         */
        public MeshOptions build() {
            return new MeshOptions(this);
        }
    }
}
