package org.byteveda.flexiq.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jspecify.annotations.Nullable;

/**
 * Retention windows set from a dashboard for one namespace. {@link #source} is
 * the toggle: the windows are kept while it is {@link RetentionSource#CODE}, so
 * switching back restores them. The elected cleaner reads it on every sweep.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class RetentionOverride {

    /** Which windows the cleaner applies. */
    public final RetentionSource source;

    /** The dashboard's windows, in milliseconds; {@code null} keeps a table forever. */
    public final EffectiveRetention.Windows windows;

    /** When it was last saved, in Unix milliseconds; {@code null} when never saved. */
    public final @Nullable Long updatedAt;

    /**
     * Decoded from the stored document.
     *
     * @param source which windows the cleaner applies
     * @param windows the dashboard's windows, or {@code null} for none
     * @param updatedAt when it was last saved, or {@code null} when never saved
     */
    @JsonCreator
    public RetentionOverride(
            @JsonProperty("source") @Nullable RetentionSource source,
            @JsonProperty("windows") EffectiveRetention.@Nullable Windows windows,
            @JsonProperty("updated_at") @Nullable Long updatedAt) {
        this.source = source == null ? RetentionSource.CODE : source;
        this.windows = windows == null ? EffectiveRetention.Windows.EMPTY : windows;
        this.updatedAt = updatedAt;
    }
}
