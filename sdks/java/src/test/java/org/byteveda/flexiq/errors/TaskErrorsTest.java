package org.byteveda.flexiq.errors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.junit.jupiter.api.Test;

class TaskErrorsTest {

    /** Named so the fully-qualified {@code errtype} assertion below stays stable. */
    static final class BoomError extends RuntimeException {
        BoomError(String message) {
            super(message);
        }
    }

    private static BoomError boom(String message) {
        BoomError error = new BoomError(message);
        error.setStackTrace(new StackTraceElement[] {
            new StackTraceElement("Frame", "one", "Frame.java", 1),
            new StackTraceElement("Frame", "two", "Frame.java", 2),
        });
        return error;
    }

    @Test
    void encodeMatchesContractVectorShape() {
        // BINDING_CONTRACT.md vector semantics: errtype/message/traceback keys in
        // that order, compact JSON, frames in getStackTrace() order.
        String expected = "{\"errtype\":\"" + BoomError.class.getName()
                + "\",\"message\":\"it broke\","
                + "\"traceback\":[\"Frame.one(Frame.java:1)\",\"Frame.two(Frame.java:2)\"]}";
        assertEquals(expected, TaskErrors.encode(boom("it broke")));
    }

    @Test
    void encodeNullMessageBecomesEmptyString() {
        String expected = "{\"errtype\":\"" + BoomError.class.getName()
                + "\",\"message\":\"\","
                + "\"traceback\":[\"Frame.one(Frame.java:1)\",\"Frame.two(Frame.java:2)\"]}";
        assertEquals(expected, TaskErrors.encode(boom(null)));
    }

    @Test
    void decodeContractVectorLiteral() {
        String raw = "{\"errtype\":\"BoomError\",\"message\":\"it broke\",\"traceback\":[\"frame1\",\"frame2\"]}";
        TaskError decoded = TaskErrors.decode(raw);
        assertEquals("BoomError", decoded.errtype);
        assertEquals("it broke", decoded.message);
        assertEquals(List.of("frame1", "frame2"), decoded.traceback);
        assertEquals(raw, decoded.raw);
    }

    @Test
    void decodeRoundTripsEncode() {
        TaskError decoded = TaskErrors.decode(TaskErrors.encode(boom("it broke")));
        assertEquals(BoomError.class.getName(), decoded.errtype);
        assertEquals("it broke", decoded.message);
        assertEquals(List.of("Frame.one(Frame.java:1)", "Frame.two(Frame.java:2)"), decoded.traceback);
    }

    @Test
    void decodeFallsBackToNullForUnstructuredStrings() {
        assertNull(TaskErrors.decode("worker died mid-flight")); // plain system string
        assertNull(TaskErrors.decode("[\"frame1\"]")); // JSON but not an object
        assertNull(TaskErrors.decode("{\"errtype\":\"BoomError\"}")); // object without message
        assertNull(TaskErrors.decode("{\"message\":42}")); // message not a string
        assertNull(TaskErrors.decode(""));
        assertNull(TaskErrors.decode(null));
    }

    @Test
    void summarizePrefersErrtypeMessageHeadline() {
        assertEquals(
                "BoomError: it broke",
                TaskErrors.summarize("{\"errtype\":\"BoomError\",\"message\":\"it broke\",\"traceback\":[]}"));
        // A missing errtype defaults to "Error" (cross-SDK contract), so the
        // headline is "Error: <message>", matching every other SDK's summary.
        assertEquals("Error: just text", TaskErrors.summarize("{\"message\":\"just text\"}"));
        assertEquals(
                "task timed out after 5000ms", TaskErrors.summarize("task timed out after 5000ms")); // raw fallback
    }
}
