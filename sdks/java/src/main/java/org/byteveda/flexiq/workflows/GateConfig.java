package org.byteveda.flexiq.workflows;

import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * An approval gate on a workflow step. The node parks ({@code WAITING_APPROVAL})
 * until {@code Worker.approveGate}/{@code rejectGate} resolves it, or — if
 * {@code timeout} is set — until the timeout elapses, when {@code onTimeout}
 * decides the outcome.
 *
 * @param timeout how long to wait before auto-resolving; {@code null} waits forever
 * @param onTimeout the action taken when {@code timeout} elapses (defaults to {@link GateAction#REJECT})
 * @param message an optional human-facing reason shown to the approver
 */
public record GateConfig(@Nullable Duration timeout, GateAction onTimeout, @Nullable String message) {
    public GateConfig {
        if (onTimeout == null) {
            onTimeout = GateAction.REJECT;
        }
        if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
            throw new IllegalArgumentException("gate timeout must be positive");
        }
    }

    /** A gate that waits indefinitely for a manual decision. */
    public static GateConfig manual() {
        return new GateConfig(null, GateAction.REJECT, null);
    }

    /** A gate that auto-resolves to {@code onTimeout} after {@code timeout}. */
    public static GateConfig timeout(Duration timeout, GateAction onTimeout) {
        return new GateConfig(Objects.requireNonNull(timeout, "timeout"), onTimeout, null);
    }

    /** A gate with a timeout and an approver-facing message. */
    public static GateConfig timeout(Duration timeout, GateAction onTimeout, @Nullable String message) {
        return new GateConfig(Objects.requireNonNull(timeout, "timeout"), onTimeout, message);
    }
}
