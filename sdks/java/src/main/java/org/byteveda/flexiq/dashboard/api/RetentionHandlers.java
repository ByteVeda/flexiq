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

    /**
     * Handlers reading one queue's retention policy.
     *
     * @param queue what the routes below read from
     */
    public RetentionHandlers(FlexiQ queue) {
        this.queue = queue;
    }

    /**
     * The published retention policy for this queue's namespace.
     *
     * @return the policy, or a null body when no cleaner has published one
     */
    public Object retention() {
        return Contract.retention(queue.effectiveRetention().orElse(null));
    }

    /** Preview what a purge would delete under the reported policy (recommended
     *  defaults when unreported), computed in-process — so it always answers.
     *
     * @return the counts a purge would delete, per class of record
     */
    public Object retentionDryRun() {
        return Contract.retentionDryRun(queue.dryRunRetention());
    }
}
