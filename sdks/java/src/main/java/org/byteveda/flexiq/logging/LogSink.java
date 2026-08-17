package org.byteveda.flexiq.logging;

/** Receives every formatted line that clears the level threshold. */
@FunctionalInterface
public interface LogSink {
    void accept(LogLevel level, String line);
}
