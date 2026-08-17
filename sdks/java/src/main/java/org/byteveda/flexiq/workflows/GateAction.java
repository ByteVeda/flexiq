package org.byteveda.flexiq.workflows;

import java.util.Locale;

/** What a {@link GateConfig} does when its approval timeout elapses. */
public enum GateAction {
    /** Treat the gate as approved — the node completes and successors run. */
    APPROVE,
    /** Treat the gate as rejected — the node fails. */
    REJECT;

    /** Lowercase wire form used in the persisted gate metadata. */
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Parse a wire form ({@code "approve"}/{@code "reject"}); defaults to {@link #REJECT}. */
    public static GateAction fromWire(String wire) {
        return "approve".equalsIgnoreCase(wire) ? APPROVE : REJECT;
    }
}
