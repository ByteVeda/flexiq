package org.byteveda.flexiq.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * A namespaced leveled logger ({@code [flexiq:worker]}). Obtain via
 * {@link #create}; the level threshold and sink are global, so
 * {@link #setLevel}/{@link #setSink} take effect everywhere immediately.
 * Messages below the threshold are dropped (suppliers are never invoked).
 */
public final class FlexiQLogger {
    private static volatile LogLevel level = envLevel();
    private static volatile LogSink sink = FlexiQLogger::writeToStderr;

    private final String tag;

    private FlexiQLogger(@Nullable String namespace) {
        this.tag = namespace == null ? "flexiq" : "flexiq:" + namespace;
    }

    /**
     * The root {@code [flexiq]} logger. Prefer {@link #create} for a namespaced one.
     *
     * @return a logger tagging its lines {@code [flexiq]}
     */
    public static FlexiQLogger root() {
        return new FlexiQLogger(null);
    }

    /**
     * A namespaced logger: {@code create("worker")} tags lines {@code [flexiq:worker]}.
     *
     * @param namespace the subsystem the lines come from
     * @return a logger tagging its lines with that namespace
     */
    public static FlexiQLogger create(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        return new FlexiQLogger(namespace);
    }

    /**
     * Set the global threshold; messages below it are dropped (and never built).
     *
     * @param newLevel the threshold, applying to every logger immediately
     */
    public static void setLevel(LogLevel newLevel) {
        level = Objects.requireNonNull(newLevel, "level");
    }

    /**
     * Replace the output sink (default: stderr). Useful for capture in tests.
     *
     * @param newSink where formatted lines go, for every logger immediately
     */
    public static void setSink(LogSink newSink) {
        sink = Objects.requireNonNull(newSink, "sink");
    }

    /**
     * Log at {@code DEBUG}.
     *
     * @param message the line; built by the caller either way, so prefer
     *     {@link #debug(Supplier)} when building it costs something
     */
    public void debug(String message) {
        emit(LogLevel.DEBUG, message, null);
    }

    /**
     * Log at {@code INFO}.
     *
     * @param message the line
     */
    public void info(String message) {
        emit(LogLevel.INFO, message, null);
    }

    /**
     * Log at {@code WARN}.
     *
     * @param message the line
     */
    public void warn(String message) {
        emit(LogLevel.WARN, message, null);
    }

    /**
     * Log at {@code WARN} with a stack trace.
     *
     * @param message the line
     * @param cause appended as a trace under the line
     */
    public void warn(String message, Throwable cause) {
        emit(LogLevel.WARN, message, cause);
    }

    /**
     * Log at {@code ERROR}.
     *
     * @param message the line
     */
    public void error(String message) {
        emit(LogLevel.ERROR, message, null);
    }

    /**
     * Log at {@code ERROR} with a stack trace.
     *
     * @param message the line
     * @param cause appended as a trace under the line
     */
    public void error(String message, Throwable cause) {
        emit(LogLevel.ERROR, message, cause);
    }

    /**
     * Lazy variant: {@code message} is built only when the level passes.
     *
     * @param message builds the line, invoked only if {@code DEBUG} clears the threshold
     */
    public void debug(Supplier<String> message) {
        if (LogLevel.DEBUG.passes(level)) {
            emit(LogLevel.DEBUG, message.get(), null);
        }
    }

    private void emit(LogLevel messageLevel, String message, @Nullable Throwable cause) {
        if (!messageLevel.passes(level)) {
            return;
        }
        StringBuilder line = new StringBuilder()
                .append(Instant.now())
                .append(" [")
                .append(tag)
                .append("] ")
                .append(messageLevel)
                .append(' ')
                .append(message);
        if (cause != null) {
            line.append(System.lineSeparator()).append(stackTraceOf(cause));
        }
        sink.accept(messageLevel, line.toString());
    }

    private static String stackTraceOf(Throwable cause) {
        StringWriter buffer = new StringWriter();
        cause.printStackTrace(new PrintWriter(buffer));
        return buffer.toString().stripTrailing();
    }

    private static LogLevel envLevel() {
        LogLevel parsed = LogLevel.parseOrNull(System.getenv("FLEXIQ_LOG_LEVEL"));
        return parsed == null ? LogLevel.WARN : parsed;
    }

    private static void writeToStderr(LogLevel ignored, String line) {
        System.err.println(line);
    }
}
