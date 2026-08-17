package org.byteveda.flexiq.dashboard.api;

import org.byteveda.flexiq.FlexiQ;

/**
 * Echoes the retention windows the elected cleaner published for this
 * namespace, so the dashboard can explain why rows disappear from its
 * listings. Retention runs in the worker process, so this is never computed
 * from local config — an unreported policy is reported as such.
 */
public final class RetentionHandlers {

    private final FlexiQ queue;

    public RetentionHandlers(FlexiQ queue) {
        this.queue = queue;
    }

    /** The published retention policy for this queue's namespace. */
    public Object retention() {
        return Contract.retention(queue.effectiveRetention().orElse(null));
    }

    /** Preview what a purge would delete under the reported policy (recommended
     *  defaults when unreported), computed in-process — so it always answers. */
    public Object retentionDryRun() {
        return Contract.retentionDryRun(queue.dryRunRetention());
    }
}
