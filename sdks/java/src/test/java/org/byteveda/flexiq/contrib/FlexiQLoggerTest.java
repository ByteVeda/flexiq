package org.byteveda.flexiq.contrib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.byteveda.flexiq.logging.FlexiQLogger;
import org.byteveda.flexiq.logging.LogLevel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FlexiQLoggerTest {
    private final List<String> lines = new ArrayList<>();

    @AfterEach
    void restoreDefaults() {
        FlexiQLogger.setLevel(LogLevel.WARN);
        FlexiQLogger.setSink((level, line) -> System.err.println(line));
    }

    @Test
    void dropsMessagesBelowThresholdAndTagsNamespace() {
        FlexiQLogger.setSink((level, line) -> lines.add(line));
        FlexiQLogger.setLevel(LogLevel.WARN);
        FlexiQLogger log = FlexiQLogger.create("worker");

        log.info("dropped");
        log.warn("kept");

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("[flexiq:worker] WARN kept"));
    }

    @Test
    void appendsStackTraceForThrowables() {
        FlexiQLogger.setSink((level, line) -> lines.add(line));
        FlexiQLogger.setLevel(LogLevel.ERROR);

        FlexiQLogger.root().error("boom", new IllegalStateException("cause detail"));

        assertEquals(1, lines.size());
        assertTrue(lines.get(0).contains("[flexiq] ERROR boom"));
        assertTrue(lines.get(0).contains("IllegalStateException: cause detail"));
    }

    @Test
    void silentDisablesEverything() {
        FlexiQLogger.setSink((level, line) -> lines.add(line));
        FlexiQLogger.setLevel(LogLevel.SILENT);

        FlexiQLogger.root().error("never emitted");

        assertTrue(lines.isEmpty());
    }
}
