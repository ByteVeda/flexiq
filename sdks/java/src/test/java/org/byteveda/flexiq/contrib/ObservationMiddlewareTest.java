package org.byteveda.flexiq.contrib;

import static io.micrometer.observation.tck.TestObservationRegistryAssert.assertThat;

import io.micrometer.observation.tck.TestObservationRegistry;
import org.byteveda.flexiq.middleware.TaskContext;
import org.junit.jupiter.api.Test;

class ObservationMiddlewareTest {

    @Test
    void recordsObservationOnSuccess() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        FlexiQObservation middleware = new FlexiQObservation(registry);
        TaskContext context = new TaskContext("job-1", "my.task");

        middleware.before(context);
        middleware.after(context, "ok");

        assertThat(registry)
                .hasObservationWithNameEqualTo("flexiq.task")
                .that()
                .hasBeenStarted()
                .hasBeenStopped()
                .hasLowCardinalityKeyValue("flexiq.task", "my.task");
    }

    @Test
    void recordsErrorOnFailure() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        FlexiQObservation middleware = new FlexiQObservation(registry);
        TaskContext context = new TaskContext("job-2", "my.task");

        middleware.before(context);
        middleware.onError(context, new IllegalStateException("boom"));

        assertThat(registry).hasObservationWithNameEqualTo("flexiq.task").that().hasError();
    }

    @Test
    void filterSkipsUnobservedTasks() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        FlexiQObservation middleware = new FlexiQObservation(registry, "flexiq.task", task -> task.equals("included"));
        TaskContext context = new TaskContext("job-3", "excluded");

        middleware.before(context);
        middleware.after(context, "ok");

        assertThat(registry).doesNotHaveAnyObservation();
    }
}
