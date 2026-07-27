package org.byteveda.taskito.worker;

/**
 * Notified as a worker built from a client starts and closes, so the client can
 * track its live workers (what backs {@code Taskito.shutdown()}). Wired by
 * {@code Taskito.worker()}; a manually built worker declares none.
 */
public interface WorkerLifecycle {

    /** The worker started and is dispatching. */
    void started(Worker worker);

    /** The worker is closing and will dispatch no more. */
    void closed(Worker worker);
}
