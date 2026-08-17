package org.byteveda.flexiq.webhooks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.byteveda.flexiq.events.EnqueuedEvent;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.OutcomeEvent;
import org.byteveda.flexiq.events.PredicateEvent;
import org.byteveda.flexiq.events.QueueEvent;
import org.junit.jupiter.api.Test;

class WebhookPayloadTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parse(byte[] body) throws Exception {
        return JSON.readValue(body, Map.class);
    }

    @Test
    void outcomeBodyCarriesTheMeasuredDuration() throws Exception {
        OutcomeEvent event = new OutcomeEvent(EventName.SUCCESS, "j1", "t", null, -1, false, 42_000_000L);
        Map<String, Object> body = parse(WebhookManager.payload(event, "job.completed"));
        assertEquals("job.completed", body.get("event"));
        assertEquals("j1", body.get("job_id"));
        assertEquals(42, ((Number) body.get("duration_ms")).intValue());
    }

    @Test
    void unmeasuredOutcomeCarriesNullDuration() throws Exception {
        OutcomeEvent event = new OutcomeEvent(EventName.DEAD, "j1", "t", "boom", 3, false);
        Map<String, Object> body = parse(WebhookManager.payload(event, "job.dead"));
        assertTrue(body.containsKey("duration_ms"));
        assertNull(body.get("duration_ms"));
    }

    @Test
    void nonOutcomeBodiesUseSnakeCasePerTypeShapes() throws Exception {
        Map<String, Object> enqueued =
                parse(WebhookManager.eventPayload(new EnqueuedEvent("j1", "t", "emails"), "job.enqueued"));
        assertEquals(Map.of("event", "job.enqueued", "job_id", "j1", "task_name", "t", "queue", "emails"), enqueued);

        Map<String, Object> paused =
                parse(WebhookManager.eventPayload(new QueueEvent(EventName.QUEUE_PAUSED, "emails"), "queue.paused"));
        assertEquals(Map.of("event", "queue.paused", "queue", "emails"), paused);

        Map<String, Object> rejected =
                parse(WebhookManager.eventPayload(new PredicateEvent("t", "nope"), "predicate.rejected"));
        assertEquals(Map.of("event", "predicate.rejected", "task_name", "t", "reason", "nope"), rejected);
    }
}
