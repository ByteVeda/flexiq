package org.byteveda.flexiq.worker;

import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.task.TaskFunction;

/**
 * A task descriptor paired with the function that handles it.
 *
 * @param <T> the task's payload type
 * @param <R> the handler's result type
 */
public final class Handler<T, R> {
    private final Task<T> task;
    private final TaskFunction<T, R> function;

    private Handler(Task<T> task, TaskFunction<T, R> function) {
        this.task = task;
        this.function = function;
    }

    /**
     * Pair a descriptor with the code that runs it.
     *
     * @param task the descriptor, carrying the name and every per-task setting
     * @param function what runs when a job of that task is dispatched
     * @param <T> the task's payload type
     * @param <R> the handler's result type
     * @return the pairing, to be registered on a worker
     */
    public static <T, R> Handler<T, R> of(Task<T> task, TaskFunction<T, R> function) {
        return new Handler<>(task, function);
    }

    /**
     * The task this handles.
     *
     * @return the descriptor
     */
    public Task<T> task() {
        return task;
    }

    /**
     * The code that runs it.
     *
     * @return the function
     */
    public TaskFunction<T, R> function() {
        return function;
    }
}
