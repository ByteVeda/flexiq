package org.byteveda.flexiq.contrib;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The metadata merge rule for W3C trace context, pinned to the core's vectors. */
class TraceCarrierTest {
    private static final String PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private static Map<String, String> carrier() {
        Map<String, String> carrier = new LinkedHashMap<>();
        carrier.put("traceparent", PARENT);
        carrier.put("tracestate", "a=1");
        return carrier;
    }

    @Test
    void absentOrBlankMetadataBecomesTheCarrier() {
        String expected = "{\"traceparent\":\"" + PARENT + "\",\"tracestate\":\"a=1\"}";
        assertEquals(expected, TraceCarrier.merge(null, carrier()));
        assertEquals(expected, TraceCarrier.merge(" ", carrier()));
    }

    @Test
    void anObjectGainsTheKeysWithItsOwnBytesKept() {
        Map<String, String> carrier = Map.of("traceparent", PARENT);
        assertEquals(
                "  {\"traceparent\":\"" + PARENT + "\", \"user\" : 1 }",
                TraceCarrier.merge("  { \"user\" : 1 }", carrier));
        assertEquals("{\"traceparent\":\"" + PARENT + "\" }", TraceCarrier.merge("{ }", carrier));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{\"traceparent\":\"mine\"}", "{\"tracestate\":\"mine=1\"}"})
    void aCallersOwnContextWinsWhole(String own) {
        assertEquals(own, TraceCarrier.merge(own, carrier()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"[1,2]", "\"text\"", "42", "not json", "{broken", "null", "{\"a\":1} junk"})
    void metadataThatIsNotAnObjectIsLeftAlone(String other) {
        assertEquals(other, TraceCarrier.merge(other, carrier()));
    }

    @Test
    void aKeyTheCallerAlreadySetIsNeverOverwritten() throws Exception {
        Map<String, String> carrier = carrier();
        carrier.put("baggage", "theirs");
        String merged = TraceCarrier.merge("{\"baggage\":\"mine\"}", carrier);
        Map<?, ?> parsed = new ObjectMapper().readValue(merged, Map.class);
        assertEquals("mine", parsed.get("baggage"));
        assertEquals(PARENT, parsed.get("traceparent"));
    }

    @Test
    void anEmptyCarrierChangesNothing() {
        assertNull(TraceCarrier.merge(null, Map.of()));
        assertEquals("plain", TraceCarrier.merge("plain", Map.of()));
    }

    @Test
    void valuesAreJsonEscaped() throws Exception {
        String merged = TraceCarrier.merge(null, Map.of("tracestate", "k=\"v\""));
        assertEquals("k=\"v\"", new ObjectMapper().readValue(merged, Map.class).get("tracestate"));
    }

    @Test
    void theCarrierIsTheMetadatasStringMembers() {
        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("traceparent", PARENT);
        metadata.put("n", 1);
        assertEquals(Map.of("traceparent", PARENT), TraceCarrier.extract(metadata));
    }
}
