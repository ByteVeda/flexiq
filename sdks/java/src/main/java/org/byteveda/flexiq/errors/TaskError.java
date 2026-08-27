package org.byteveda.flexiq.errors;

import java.util.List;

/**
 * A structured task error decoded from the canonical JSON stored in {@code error}
 * fields (jobs, error history, dead letter). Obtain via
 * {@link TaskErrors#decode(String)}; plain system strings decode to {@code null}.
 */
public final class TaskError {
    /** Exception class name reported by the producing worker; empty when absent. */
    public final String errtype;
    /** Human-readable message, verbatim; may be empty. */
    public final String message;
    /** Best-effort stack frames, throw site first; empty when unavailable. */
    public final List<String> traceback;
    /** The stored string this was decoded from. */
    public final String raw;

    TaskError(String errtype, String message, List<String> traceback, String raw) {
        this.errtype = errtype;
        this.message = message;
        this.traceback = List.copyOf(traceback);
        this.raw = raw;
    }

    /**
     * One-line human summary: {@code errtype: message}, or just the message when errtype is empty.
     *
     * @return the summary, for a dashboard row or a log line
     */
    public String summary() {
        return errtype.isEmpty() ? message : errtype + ": " + message;
    }

    // errtype is empty only when a caller constructs a TaskError directly with
    // an empty type; decode() defaults a missing wire errtype to "Error".
}
