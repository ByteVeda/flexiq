package org.byteveda.flexiq.contrib;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import java.util.function.Predicate;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;
import org.jspecify.annotations.Nullable;

/**
 * Wraps each task execution in a Micrometer {@link Observation}: one
 * instrumentation that yields both metrics (a timer) and a trace span. Plug in
 * OpenTelemetry (or any backend) by configuring the {@link ObservationRegistry}
 * the application passes in.
 *
 * <p>The observation is created in {@code before}, made current for the handler
 * (an open {@link Observation.Scope}), and stopped in {@code after}/{@code onError};
 * state is carried on the per-task {@link TaskContext#attributes()}.
 */
public final class FlexiQObservation implements Middleware {
    private static final String OBSERVATION = "flexiq.contrib.observation";
    private static final String SCOPE = "flexiq.contrib.observation.scope";

    private final ObservationRegistry registry;
    private final String name;
    private final Predicate<String> taskFilter;

    public FlexiQObservation(ObservationRegistry registry) {
        this(registry, "flexiq.task", task -> true);
    }

    public FlexiQObservation(ObservationRegistry registry, String name, Predicate<String> taskFilter) {
        this.registry = registry;
        this.name = name;
        this.taskFilter = taskFilter;
    }

    @Override
    public void before(TaskContext context) {
        if (!taskFilter.test(context.taskName)) {
            return;
        }
        Observation observation = Observation.createNotStarted(name, registry)
                .lowCardinalityKeyValue("flexiq.task", context.taskName)
                .start();
        context.attributes().put(OBSERVATION, observation);
        context.attributes().put(SCOPE, observation.openScope());
    }

    @Override
    public void after(TaskContext context, Object result) {
        stop(context, null);
    }

    @Override
    public void onError(TaskContext context, Throwable error) {
        stop(context, error);
    }

    private void stop(TaskContext context, @Nullable Throwable error) {
        Observation.Scope scope = (Observation.Scope) context.attributes().remove(SCOPE);
        if (scope != null) {
            scope.close();
        }
        Observation observation = (Observation) context.attributes().remove(OBSERVATION);
        if (observation == null) {
            return;
        }
        if (error != null) {
            observation.error(error);
        }
        observation.stop();
    }
}
