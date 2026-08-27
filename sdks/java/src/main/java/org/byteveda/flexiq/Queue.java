package org.byteveda.flexiq;

/**
 * A handle to one named queue. Obtain it from {@link FlexiQ#queue(String)}.
 * Pausing stops workers from dispatching this queue's jobs until resumed;
 * in-flight jobs run to completion.
 */
public interface Queue {

    /**
     * This queue's name.
     *
     * @return the name it was obtained under
     */
    String name();

    /** Stop workers from dispatching jobs on this queue. */
    void pause();

    /** Resume dispatching after a {@link #pause()}. */
    void resume();

    /**
     * Whether this queue is currently paused.
     *
     * @return {@code true} while workers are holding off this queue's jobs
     */
    boolean isPaused();
}
