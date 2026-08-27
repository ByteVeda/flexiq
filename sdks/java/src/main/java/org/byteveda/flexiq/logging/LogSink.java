package org.byteveda.flexiq.logging;

/** Receives every formatted line that clears the level threshold. */
@FunctionalInterface
public interface LogSink {
    /**
     * Take one formatted line.
     *
     * @param level the severity it was logged at
     * @param line the formatted line, tag and timestamp included, with no trailing newline
     */
    void accept(LogLevel level, String line);
}
