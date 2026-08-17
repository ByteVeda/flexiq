package org.byteveda.flexiq.contrib;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.OutcomeEvent;
import org.byteveda.flexiq.middleware.TaskContext;
import org.junit.jupiter.api.Test;

class SentryMiddlewareTest {

    @Test
    void safeNoOpWhenSentryNotInitialized() {
        SentryMiddleware middleware = new SentryMiddleware();
        // With no Sentry.init(...), the hooks must not break the worker.
        assertDoesNotThrow(() -> middleware.onError(new TaskContext("j", "t"), new RuntimeException("x")));
        assertDoesNotThrow(() -> middleware.onDeadLetter(new OutcomeEvent(EventName.DEAD, "j", "t", "err", 0, false)));
    }

    @Test
    void filterSkipsUnreportedTasks() {
        SentryMiddleware middleware = new SentryMiddleware(task -> task.equals("included"));
        assertDoesNotThrow(() -> middleware.onError(new TaskContext("j", "excluded"), new RuntimeException("x")));
    }
}
