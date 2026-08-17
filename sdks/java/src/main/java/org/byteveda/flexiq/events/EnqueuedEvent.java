package org.byteveda.flexiq.events;

/**
 * A job was accepted into a queue ({@link EventName#JOB_ENQUEUED}).
 *
 * @param jobId the accepted job's id
 * @param taskName the task the job will run
 * @param queue the queue the job landed on
 */
public record EnqueuedEvent(String jobId, String taskName, String queue) implements FlexiQEvent {

    @Override
    public EventName name() {
        return EventName.JOB_ENQUEUED;
    }
}
