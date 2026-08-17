package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A declared topic in the registry. Declaring a log topic makes its publishes
 * retained even with no subscriber (removing the late-join boundary), bounded by
 * an optional retention window.
 *
 * @param name the topic name
 * @param mode delivery mode — {@code "log"} today (the only declarable mode)
 * @param retentionMs retention window in milliseconds, or {@code null} to keep
 *     messages until a subscriber consumes them
 * @param createdAt Unix-millisecond declaration time
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Topic(
        @JsonProperty("name") String name,
        @JsonProperty("mode") String mode,
        @JsonProperty("retentionMs") Long retentionMs,
        @JsonProperty("createdAt") long createdAt) {}
