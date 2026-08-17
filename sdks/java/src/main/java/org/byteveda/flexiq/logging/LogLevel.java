package org.byteveda.flexiq.logging;

import java.util.Locale;
import org.jspecify.annotations.Nullable;

/** Severity, from most to least verbose. {@link #SILENT} disables all output. */
public enum LogLevel {
    DEBUG(10),
    INFO(20),
    WARN(30),
    ERROR(40),
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
