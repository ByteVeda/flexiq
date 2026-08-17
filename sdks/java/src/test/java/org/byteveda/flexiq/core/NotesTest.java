package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.errors.NotesValidationException;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.serialization.Notes;
import org.byteveda.flexiq.task.EnqueueOptions;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class NotesTest {

    @Test
    void nullEncodesToNullAndEmptyToObject() {
        assertNull(Notes.encode(null));
        assertEquals("{}", Notes.encode(Map.of()));
    }

    @Test
    void encodesWithSortedKeys() {
        Map<String, Object> notes = new LinkedHashMap<>();
        notes.put("b", 1);
        notes.put("a", "x");
        assertEquals("{\"a\":\"x\",\"b\":1}", Notes.encode(notes));
    }

    @Test
    void rejectsTooManyFields() {
        Map<String, Object> notes = new LinkedHashMap<>();
        for (int i = 0; i <= Notes.MAX_FIELDS; i++) {
            notes.put("k" + i, i);
        }
        assertThrows(NotesValidationException.class, () -> Notes.encode(notes));
    }

    @Test
    void rejectsOverlongKey() {
        String key = "k".repeat(Notes.MAX_KEY_LENGTH + 1);
        assertThrows(NotesValidationException.class, () -> Notes.encode(Map.of(key, 1)));
    }

    @Test
    void rejectsOverlongStringLeaf() {
        String value = "v".repeat(Notes.MAX_VALUE_LENGTH + 1);
        assertThrows(NotesValidationException.class, () -> Notes.encode(Map.of("k", value)));
    }

    @Test
    void rejectsExcessiveNesting() {
        // depth 3 leaf is allowed; depth 4 is not.
        Notes.encode(Map.of("a", Map.of("b", Map.of("c", 1))));
        assertThrows(
                NotesValidationException.class,
                () -> Notes.encode(Map.of("a", Map.of("b", Map.of("c", Map.of("d", 1))))));
    }

    @Test
    void rejectsUnsupportedLeafType() {
        assertThrows(NotesValidationException.class, () -> Notes.encode(Map.of("k", new Object())));
    }

    @Test
    void rejectsNonFiniteNumber() {
        assertThrows(NotesValidationException.class, () -> Notes.encode(Map.of("k", Double.NaN)));
    }

    @Test
    void rejectsOversizedEncoding() {
        // 15 fields (the max) each holding a 500-char value (the max) blows past the byte cap.
        Map<String, Object> notes = new LinkedHashMap<>();
        String big = "x".repeat(Notes.MAX_VALUE_LENGTH);
        for (int i = 0; i < Notes.MAX_FIELDS; i++) {
            notes.put("k" + i, big);
        }
        assertThrows(NotesValidationException.class, () -> Notes.encode(notes));
    }

    @Test
    @Timeout(30)
    void notesRoundTripThroughStorage(@TempDir Path dir) throws Exception {
        try (FlexiQ queue =
                FlexiQ.builder().url(dir.resolve("notes.db").toString()).open()) {
            Task<String> task = Task.of("notes.echo", String.class);
            Map<String, Object> notes = new LinkedHashMap<>();
            notes.put("env", "prod");
            notes.put("attempt", 3);
            String id = queue.enqueue(
                    task, "hi", EnqueueOptions.builder().notes(notes).build());

            Job job = queue.getJob(id).orElseThrow();
            assertEquals(notes, job.notesMap().orElseThrow());
        }
    }
}
