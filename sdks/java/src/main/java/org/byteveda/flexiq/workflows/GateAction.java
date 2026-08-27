package org.byteveda.flexiq.workflows;

import java.util.Locale;

/** What a {@link GateConfig} does when its approval timeout elapses. */
public enum GateAction {
    /** Treat the gate as approved — the node completes and successors run. */
    APPROVE,
    /** Treat the gate as rejected — the node fails. */
    REJECT;

    /**
     * Lowercase wire form used in the persisted gate metadata.
     *
     * @return the wire form
     */
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    /**
     * Parse a wire form ({@code "approve"}/{@code "reject"}); defaults to {@link #REJECT}.
     *
     * @param wire the stored value
     * @return the matching constant, or {@link #REJECT} for anything unrecognised —
     *     an unreadable gate must not auto-approve
     */
    public static GateAction fromWire(String wire) {
        return "approve".equalsIgnoreCase(wire) ? APPROVE : REJECT;
    }
}
