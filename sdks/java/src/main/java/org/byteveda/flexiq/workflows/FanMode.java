package org.byteveda.flexiq.workflows;

/** Fan-out / fan-in strategy. The wire form is the lowercase name. */
public enum FanMode {
    /** Fan-out: run the step once per item of the predecessor's result list. */
    EACH,
    /** Fan-in: collect every fan-out child's result into one list. */
    ALL;

    /** Lowercase wire strategy string passed to the native layer. */
    public String wire() {
        return name().toLowerCase(java.util.Locale.ROOT);
    }
}
