package org.byteveda.flexiq.workflows;

import org.byteveda.flexiq.task.Task;

/**
 * Celery-style shortcuts that build a {@link Workflow} from a few links:
 * {@link #chain} (sequential), {@link #group} (parallel), and {@link #chord}
 * (a callback after a parallel group). Submit the result like any workflow.
 *
 * <p>A {@code chord}'s callback runs once every group step completes; like the
 * rest of the static-DAG model it receives its own payload, not the group's
 * aggregated results (use a fan-out + fan-in for aggregation).
 */
public final class Canvas {
    private Canvas() {}

    /** A named step bound to a task and payload, for use in canvas shortcuts. */
    public static <T> Link link(String name, Task<T> task, T payload) {
        return new Link(name, task.name(), payload);
    }

    /** Run {@code links} one after another (each depends on the previous). */
    public static Workflow chain(String name, Link... links) {
        Workflow workflow = Workflow.named(name);
        String previous = null;
        for (Link link : links) {
            workflow.step(link.toStep(previous == null ? new String[0] : new String[] {previous}));
            previous = link.name;
        }
        return workflow;
    }

    /** Run all {@code links} in parallel (no dependencies between them). */
    public static Workflow group(String name, Link... links) {
        Workflow workflow = Workflow.named(name);
        for (Link link : links) {
            workflow.step(link.toStep(new String[0]));
        }
        return workflow;
    }

    /** Run {@code group} in parallel, then {@code callback} once all of them complete. */
    public static Workflow chord(String name, Link callback, Link... group) {
        Workflow workflow = Workflow.named(name);
        String[] groupNames = new String[group.length];
        for (int i = 0; i < group.length; i++) {
            workflow.step(group[i].toStep(new String[0]));
            groupNames[i] = group[i].name;
        }
        workflow.step(callback.toStep(groupNames));
        return workflow;
    }

    /** A canvas building block: a step name, its task, and its payload. */
    public static final class Link {
        private final String name;
        private final String taskName;
        private final Object payload;

        Link(String name, String taskName, Object payload) {
            this.name = name;
            this.taskName = taskName;
            this.payload = payload;
        }

        Step toStep(String[] after) {
            return Step.of(name, taskName, payload).after(after).build();
        }
    }
}
