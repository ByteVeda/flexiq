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

    /** The root {@code [flexiq]} logger. Prefer {@link #create} for a namespaced one. */
    public static FlexiQLogger root() {
        return new FlexiQLogger(null);
    }

    /** A namespaced logger: {@code create("worker")} tags lines {@code [flexiq:worker]}. */
    public static FlexiQLogger create(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        return new FlexiQLogger(namespace);
    }

    /** Set the global threshold; messages below it are dropped (and never built). */
    public static void setLevel(LogLevel newLevel) {
        level = Objects.requireNonNull(newLevel, "level");
    }

    /** Replace the output sink (default: stderr). Useful for capture in tests. */
    public static void setSink(LogSink newSink) {
        sink = Objects.requireNonNull(newSink, "sink");
    }

    public void debug(String message) {
        emit(LogLevel.DEBUG, message, null);
    }

    public void info(String message) {
        emit(LogLevel.INFO, message, null);
    }

    public void warn(String message) {
        emit(LogLevel.WARN, message, null);
    }

    public void warn(String message, Throwable cause) {
        emit(LogLevel.WARN, message, cause);
    }

    public void error(String message) {
        emit(LogLevel.ERROR, message, null);
    }

    public void error(String message, Throwable cause) {
        emit(LogLevel.ERROR, message, cause);
    }

    /** Lazy variant: {@code message} is built only when the level passes. */
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
