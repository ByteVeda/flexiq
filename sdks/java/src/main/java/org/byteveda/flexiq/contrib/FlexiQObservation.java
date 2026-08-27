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

    /**
     * Observe every task under the {@code flexiq.task} name.
     *
     * @param registry the application's registry, which decides where the timer and
     *     span actually go
     */
    public FlexiQObservation(ObservationRegistry registry) {
        this(registry, "flexiq.task", task -> true);
    }

    /**
     * Observe the tasks {@code taskFilter} accepts, under {@code name}.
     *
     * @param registry the application's registry, which decides where the timer and
     *     span actually go
     * @param name the observation name every task shares; the task itself is a
     *     low-cardinality tag, not part of the name
     * @param taskFilter which tasks to instrument, by task name
     */
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

    /**
     * The attempt ended in a durable {@code step.sleep}.
     *
     * <p>Closed with a {@code sleep} event and no error: the work has not
     * finished, so recording it as a success would inflate the success timer and
     * close the span green on a job that has not run yet. The next attempt opens
     * its own observation, which is the honest shape — a sleeping job is not
     * occupying a worker.
     */
    @Override
    public void onSleep(TaskContext context, long wakeAt) {
        Observation observation = (Observation) context.attributes().get(OBSERVATION);
        if (observation != null) {
            observation.event(Observation.Event.of("flexiq.sleep", "sleeping until " + wakeAt));
        }
        stop(context, null);
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
