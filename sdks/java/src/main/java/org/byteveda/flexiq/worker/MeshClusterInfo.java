package org.byteveda.flexiq.worker;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A snapshot of a mesh worker's view of its cluster, from
 * {@link Worker#meshClusterInfo()}. Capacities and loads are summed across alive
 * peers (excluding this node); buffered counts are job-deque lengths.
 *
 * @param peerCount alive peers discovered via gossip (excludes self)
 * @param totalCapacity summed advertised capacity of those peers
 * @param totalLoad summed in-flight jobs across those peers
 * @param totalBuffered summed local-deque length across those peers
 * @param localBufferLen this worker's local-deque length
 * @param adaptivePrefetch this worker's current prefetch budget
 */
public record MeshClusterInfo(
        @JsonProperty("peer_count") int peerCount,
        @JsonProperty("total_capacity") int totalCapacity,
        @JsonProperty("total_load") int totalLoad,
        @JsonProperty("total_buffered") int totalBuffered,
        @JsonProperty("local_buffer_len") int localBufferLen,
        @JsonProperty("adaptive_prefetch") int adaptivePrefetch) {}
