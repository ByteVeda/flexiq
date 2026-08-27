package org.byteveda.flexiq.task;

/**
 * What a saturated rate limit does to a task's jobs.
 *
 * <p>{@link #DEFER} keeps the job and retries it on a later dispatch cycle.
 * {@link #DROP} sheds it to the dead-letter queue, for traffic whose value
 * expires with the moment — metrics samples, cache warms — where a backlog is
 * worth less than nothing.
 */
public enum OnExcess {
    /** Reschedule the job for a later dispatch cycle. The default. */
    DEFER("defer"),
    /** Dead-letter the job immediately with a reserved {@code rate_limit:} reason. */
    DROP("drop");

    private final String wireName;

    OnExcess(String wireName) {
        this.wireName = wireName;
    }

    /**
     * The spelling the binding expects, shared across every SDK.
     *
     * @return the wire name
     */
    public String wireName() {
        return wireName;
    }

    /**
     * The constant for a wire spelling, case-insensitively.
     *
     * @param wireName {@code "defer"} or {@code "drop"}
     * @return the matching constant
     * @throws IllegalArgumentException when {@code wireName} names no constant
     */
    public static OnExcess fromWireName(String wireName) {
        for (OnExcess value : values()) {
            if (value.wireName.equalsIgnoreCase(wireName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("onExcess must be \"defer\" or \"drop\", got \"" + wireName + "\"");
    }
}
