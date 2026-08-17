package org.byteveda.flexiq;

/**
 * A handle to one named queue. Obtain it from {@link FlexiQ#queue(String)}.
 * Pausing stops workers from dispatching this queue's jobs until resumed;
 * in-flight jobs run to completion.
 */
public interface Queue {

    /** This queue's name. */
    String name();

    /** Stop workers from dispatching jobs on this queue. */
    void pause();

    /** Resume dispatching after a {@link #pause()}. */
    void resume();

    /** Whether this queue is currently paused. */
    boolean isPaused();
}
