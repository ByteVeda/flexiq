package org.byteveda.flexiq.contrib;

import io.sentry.Sentry;
import io.sentry.SentryLevel;
import java.util.function.Predicate;
import org.byteveda.flexiq.errors.TaskErrors;
import org.byteveda.flexiq.events.OutcomeEvent;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;

/**
 * Reports task failures to Sentry: {@code onError} captures the exception, and
 * {@code onDeadLetter} records a fatal event for a job that exhausted its
 * retries. Each event is tagged with the task and job id. The application must
 * initialise Sentry ({@code Sentry.init(...)}); when it has not, the calls are
 * safe no-ops.
 */
public final class SentryMiddleware implements Middleware {
    private final Predicate<String> taskFilter;

    public SentryMiddleware() {
        this(task -> true);
    }

    public SentryMiddleware(Predicate<String> taskFilter) {
        this.taskFilter = taskFilter;
    }

    @Override
    public void onError(TaskContext context, Throwable error) {
        if (!taskFilter.test(context.taskName)) {
            return;
        }
        Sentry.withScope(scope -> {
            scope.setTag("flexiq.task", context.taskName);
            scope.setTag("flexiq.job", context.jobId);
            Sentry.captureException(error);
        });
    }

    @Override
    public void onDeadLetter(OutcomeEvent event) {
        if (!taskFilter.test(event.taskName)) {
            return;
        }
        Sentry.withScope(scope -> {
            scope.setLevel(SentryLevel.FATAL);
            scope.setTag("flexiq.task", event.taskName);
            scope.setTag("flexiq.job", event.jobId);
            // Stored errors may be canonical JSON; headline with "errtype: message" when so.
            Sentry.captureMessage(
                    "task dead-lettered: " + (event.error == null ? "" : TaskErrors.summarize(event.error)));
        });
    }
}
