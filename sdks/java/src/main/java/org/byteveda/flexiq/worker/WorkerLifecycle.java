package org.byteveda.flexiq.worker;

/**
 * Notified as a worker built from a client starts and closes, so the client can
 * track its live workers (what backs {@code FlexiQ.shutdown()}). Wired by
 * {@code FlexiQ.worker()}; a manually built worker declares none.
 */
public interface WorkerLifecycle {

    /**
     * The worker started and is dispatching.
     *
     * @param worker the worker, now live
     */
    void started(Worker worker);

    /**
     * The worker is closing and will dispatch no more.
     *
     * @param worker the worker, now shutting down
     */
    void closed(Worker worker);
}
