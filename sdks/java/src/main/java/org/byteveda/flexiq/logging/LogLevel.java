package org.byteveda.flexiq.logging;

import java.util.Locale;
import org.jspecify.annotations.Nullable;

/** Severity, from most to least verbose. {@link #SILENT} disables all output. */
public enum LogLevel {
    /** Internal detail, useful when diagnosing the SDK itself. */
    DEBUG(10),
    /** Normal lifecycle progress. */
    INFO(20),
    /** Something recovered from, but worth knowing about. */
    WARN(30),
    /** Something that failed. */
    ERROR(40),
    /** As a threshold, drops everything; no message is emitted at this level. */
    SILENT(Integer.MAX_VALUE);

    private final int severity;

    LogLevel(int severity) {
        this.severity = severity;
    }

    /** Whether a message at this level clears the {@code threshold}. */
    boolean passes(LogLevel threshold) {
        return severity >= threshold.severity;
    }

    /** Case-insensitive parse; {@code null} when {@code raw} is not a level name. */
    static @Nullable LogLevel parseOrNull(String raw) {
        if (raw == null) {
            return null;
        }
        try {
            return valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
