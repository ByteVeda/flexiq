package org.byteveda.flexiq.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.byteveda.flexiq.steps.StepDivergedError;
import org.byteveda.flexiq.steps.StepError;
import org.jspecify.annotations.Nullable;

/**
 * One attempt's walk through a job's recorded step sequence.
 *
 * <p>Pure: it holds the snapshot read at attempt start and decides, for each
 * step the code asks for, whether the value is already known, whether new ground
 * has been reached, or whether the code has changed underneath the recorded
 * sequence. Nothing here touches the backend.
 *
 * <p>The "fingerprint" of a job's steps is this ordered list of keys — there is
 * no digest. Each step is matched as it is asked for, which is what makes a
 * divergence surface <i>before</i> the body runs.
 *
 * <p>The two identities are matched differently, which is the whole point of
 * having both. An unkeyed step is matched <b>by position</b>: {@code fetch#1} is
 * "the second {@code fetch} of this attempt", so it is only the same step at the
 * same point. An explicit key is matched <b>by key, wherever it sits</b> — a key
 * exists precisely so a loop over something unordered can hand its steps back in
 * a different order without every one of them looking like a different question.
 */
final class StepSequence {

    /** Name an unnamed sleep is numbered under, so {@code sleep(1h)} is {@code sleep#0}. */
    static final String DEFAULT_SLEEP_NAME = "sleep";

    /** Neighbours a divergence message keeps on either side of the position that failed. */
    private static final int DIVERGENCE_CONTEXT = 5;

    private final String jobId;

    /** The snapshot, ordered by {@code seq} and gapless — a hole would shift every later memo. */
    private final List<StepRecord> recorded;

    /**
     * Which recorded steps this attempt has claimed, parallel to {@link #recorded}.
     * A keyed hit can claim one out of order, so the positional walk skips what is
     * already spoken for rather than counting blindly.
     */
    private final boolean[] claimed;

    /** Recorded key to its index, for the keyed lookup. */
    private final Map<String, Integer> byKey = new HashMap<>();

    /** Keys this attempt has asked for, in order — the divergence message needs them. */
    private final List<String> issued = new ArrayList<>();

    /** The same keys as a set: a duplicate explicit key is refused, and a scan would be quadratic. */
    private final Set<String> issuedKeys = new HashSet<>();

    /** Per-name occurrence counters. Explicit keys never touch these. */
    private final Map<String, Integer> occurrences = new HashMap<>();

    /** Where the positional walk has got to in the snapshot. */
    private int cursor;

    /** The step handed out and not yet committed. At most one at a time. */
    private @Nullable PendingStep pending;

    /**
     * Rows committed for this job and the bytes they hold, seeded from the
     * snapshot so the caps need no second read. The count is also the next free
     * {@code seq} — the sequence is gapless by construction.
     */
    private int storedCount;

    private int storedBytes;

    StepSequence(String jobId, List<StepRecord> recorded) {
        this.jobId = jobId;
        this.recorded = List.copyOf(recorded);
        this.claimed = new boolean[this.recorded.size()];
        int bytes = 0;
        for (int index = 0; index < this.recorded.size(); index++) {
            StepRecord step = this.recorded.get(index);
            if (step.seq() != index) {
                throw new StepError(
                        "job " + jobId + " has a hole in its step sequence: position " + index + " holds seq "
                                + step.seq(),
                        false);
            }
            byKey.put(step.stepKey(), index);
            bytes += step.resultLength();
        }
        this.storedCount = this.recorded.size();
        this.storedBytes = bytes;
    }

    /** A step that has been issued but not yet committed. */
    record PendingStep(int seq, String stepKey, boolean sleep) {}

    /**
     * What one {@code step.run} must do: replay {@code memoized}, or run the body
     * and commit at {@code pending}. Exactly one is non-null.
     */
    record RunOutcome(String stepKey, byte @Nullable [] memoized, @Nullable PendingStep pending) {}

    /**
     * What one {@code step.sleep} must do.
     *
     * @param elapsed the deadline had already passed: nothing to write, and the
     *     attempt carries on
     * @param fresh this sleep is new ground, so it is the one that can be refused
     *     by the step-count cap — a resume writes nothing to count
     */
    record SleepOutcome(String stepKey, boolean elapsed, long wakeAt, @Nullable PendingStep pending, boolean fresh) {}

    /**
     * Decide what {@code step.run(name)} — or {@code step.run(name, key)} — must do.
     *
     * <p>Nothing counts as done until the commit lands, so a body that raises
     * leaves the sequence exactly where it was.
     */
    RunOutcome beginRun(String name, @Nullable String key) {
        String stepKey = resolveKey(name, key);
        checkIssuable(stepKey);
        Integer index = landed(stepKey, false, key != null);
        spendOccurrence(name, key);
        if (index == null) {
            return new RunOutcome(stepKey, null, newGround(stepKey, false));
        }
        claimed[index] = true;
        StepRecord hit = recorded.get(index);
        byte[] result = hit.result();
        return new RunOutcome(hit.stepKey(), result == null ? new byte[0] : result.clone(), null);
    }

    /**
     * Decide what {@code step.sleep(…)} must do.
     *
     * <p>{@code now} is passed in rather than read, because whether a recorded
     * sleep is finished is the reader's derivation from {@code now >= wakeAt} and
     * not a stored status. That is what leaves no state for a crash to strand.
     */
    SleepOutcome beginSleep(@Nullable String name, @Nullable String key, long now) {
        String sleepName = name == null ? DEFAULT_SLEEP_NAME : name;
        String stepKey = resolveKey(sleepName, key);
        checkIssuable(stepKey);
        Integer index = landed(stepKey, true, key != null);
        spendOccurrence(sleepName, key);
        if (index == null) {
            return new SleepOutcome(stepKey, false, 0L, newGround(stepKey, true), true);
        }
        StepRecord hit = recorded.get(index);
        Long wakeAt = hit.wakeAt();
        if (wakeAt == null) {
            throw new StepDivergedError("step divergence on job " + jobId + " at position " + hit.seq()
                    + ": expected a sleep step with a deadline, found '" + StepKeys.abbreviate(hit.stepKey())
                    + "' with none");
        }
        claimed[index] = true;
        if (now >= wakeAt) {
            return new SleepOutcome(hit.stepKey(), true, wakeAt, null, false);
        }
        // Re-issued at the *recorded* position, so the store recognizes the row
        // and answers with the deadline it already holds. A fresh position would
        // commit a second sleep and start the clock again.
        PendingStep resume = new PendingStep(hit.seq(), hit.stepKey(), true);
        this.pending = resume;
        return new SleepOutcome(hit.stepKey(), false, wakeAt, resume, false);
    }

    /** Acknowledge that {@code step} was committed, and move on. */
    void commit(PendingStep step, int encodedLength) {
        takePending(step);
        storedCount++;
        storedBytes += encodedLength;
    }

    /**
     * Acknowledge the sleep {@code step} issued.
     *
     * <p>Only a fresh commit adds a row: a resume found the deadline already
     * stored and wrote nothing, so counting it would put the sequence one ahead
     * of the store. A sleep row holds no result, so the byte total never moves.
     */
    void commitSleep(PendingStep step, boolean fresh) {
        takePending(step);
        if (fresh) {
            storedCount++;
        }
    }

    /** Recorded steps this attempt never asked for. */
    List<String> orphanedTail() {
        List<String> orphans = new ArrayList<>();
        for (int index = 0; index < recorded.size(); index++) {
            if (!claimed[index]) {
                orphans.add(recorded.get(index).stepKey());
            }
        }
        return orphans;
    }

    /** The recorded sequence, for a log line that has to show both. */
    List<String> recordedKeys() {
        return recorded.stream().map(StepRecord::stepKey).toList();
    }

    /** The keys this attempt has asked for, in order. */
    List<String> issuedKeys() {
        return List.copyOf(issued);
    }

    /** How many steps this job has committed, snapshot plus this attempt. */
    int committedSteps() {
        return storedCount;
    }

    /** How many encoded bytes those steps hold. */
    int committedBytes() {
        return storedBytes;
    }

    // -------------------------------------------------------------- private

    private String resolveKey(String name, @Nullable String key) {
        return key == null ? StepKeys.derive(name, occurrences.getOrDefault(name, 0)) : StepKeys.explicit(name, key);
    }

    /**
     * Spend the name's occurrence, but only once the step is known to be usable:
     * a refused one must not shift the key of the next. An explicit key never
     * spends one at all, so adding a keyed call cannot move an unkeyed one.
     */
    private void spendOccurrence(String name, @Nullable String key) {
        if (key == null) {
            occurrences.merge(name, 1, Integer::sum);
        }
    }

    /** Refuse a step this attempt is in no position to ask for, and record that it asked. */
    private void checkIssuable(String stepKey) {
        PendingStep outstanding = pending;
        if (outstanding != null) {
            throw new StepError(
                    "step '" + StepKeys.abbreviate(stepKey) + "' of job " + jobId + " started while step '"
                            + StepKeys.abbreviate(outstanding.stepKey()) + "' is still uncommitted",
                    false);
        }
        if (!issuedKeys.add(stepKey)) {
            // Two steps sharing a key would memo over each other, and the position
            // check cannot see it — both sequences look identical.
            throw new StepError(
                    "step key '" + StepKeys.abbreviate(stepKey) + "' was used twice in one attempt of job " + jobId
                            + "; give each keyed step a key of its own",
                    false);
        }
        issued.add(stepKey);
    }

    /**
     * Where this step lands in the snapshot: the index of the recorded row it
     * replays, or {@code null} for new ground.
     *
     * <p>Stops short of deciding what that <i>means</i>, because a run and a
     * sleep read a recorded row differently — a run row's presence is its
     * completion, a sleep row's is a deadline. What they share is the match
     * itself, and the divergence when it fails.
     */
    private @Nullable Integer landed(String stepKey, boolean sleep, boolean keyed) {
        Integer index = recordedMatch(stepKey, keyed);
        if (index != null) {
            // Same key, different kind: a `run` replaying onto a recorded `sleep`
            // is a changed sequence like any other.
            if (recorded.get(index).sleep() != sleep) {
                throw divergence(index, stepKey, sleep);
            }
            return index;
        }
        if (keyed || cursor >= recorded.size()) {
            return null;
        }
        // The positional walk reached a step the recorded run does not have here.
        // Nothing later can line up either.
        throw divergence(cursor, stepKey, sleep);
    }

    /**
     * Which recorded step, if any, this one replays. A keyed step is looked up by
     * key wherever it sits; an unkeyed one must be at the cursor, which skips
     * whatever a keyed hit already claimed.
     */
    private @Nullable Integer recordedMatch(String stepKey, boolean keyed) {
        if (keyed) {
            // Never already claimed: a key issued twice in one attempt is refused
            // above, so at most one lookup can reach any given row.
            return byKey.get(stepKey);
        }
        while (cursor < recorded.size() && claimed[cursor]) {
            cursor++;
        }
        if (cursor >= recorded.size()) {
            return null;
        }
        return recorded.get(cursor).stepKey().equals(stepKey) ? cursor : null;
    }

    /**
     * This attempt got further than any before it: the step is new. It takes the
     * next free {@code seq}, which is the number of rows already stored — not the
     * walk's position, which a keyed hit can leave behind.
     */
    private PendingStep newGround(String stepKey, boolean sleep) {
        PendingStep fresh = new PendingStep(storedCount, stepKey, sleep);
        this.pending = fresh;
        return fresh;
    }

    /** Consume the outstanding step, refusing anything but the one handed out. */
    private void takePending(PendingStep step) {
        if (!step.equals(pending)) {
            throw new StepError(
                    "step '" + StepKeys.abbreviate(step.stepKey()) + "' of job " + jobId + " was committed out of turn",
                    false);
        }
        pending = null;
    }

    private StepDivergedError divergence(int position, String stepKey, boolean sleep) {
        StepRecord at = recorded.get(position);
        String expected;
        String found;
        if (at.stepKey().equals(stepKey)) {
            // Same key, different kind: say so, or the message reads as if nothing
            // changed.
            expected = "'" + at.stepKey() + "' as a " + at.kind() + " step";
            found = "'" + stepKey + "' as a " + (sleep ? "sleep" : "run") + " step";
        } else {
            expected = "'" + StepKeys.abbreviate(at.stepKey()) + "'";
            found = "'" + StepKeys.abbreviate(stepKey) + "'";
        }
        return new StepDivergedError("step sequence changed for job " + jobId + " at position " + position + "\n"
                + "  recorded: " + renderSequence(recordedKeys(), position) + "\n"
                + "  running:  " + renderSequence(issuedKeys(), Math.max(issued.size() - 1, 0)) + "\n"
                + "  step " + position + " was " + expected + ", now " + found + "\n"
                + "A memoized result would answer a different question than the step asking for it. "
                + "Drain or dead-letter this task's in-flight jobs before deploying a change to its step sequence.");
    }

    /**
     * Render a sequence around the position that failed. Bounded on purpose: a
     * job may commit a thousand steps, and an error nobody can read is not louder
     * for being longer.
     */
    private static String renderSequence(List<String> keys, int position) {
        if (keys.isEmpty()) {
            return "(none)";
        }
        int end = Math.min(position + DIVERGENCE_CONTEXT + 1, keys.size());
        // Clamped both ways: an index past the end of these keys must render a
        // shorter window, never an inverted slice.
        int start = Math.min(Math.max(position - DIVERGENCE_CONTEXT, 0), end);
        StringBuilder out = new StringBuilder();
        if (start > 0) {
            out.append("…(").append(start).append(" earlier), ");
        }
        for (int index = start; index < end; index++) {
            if (index > start) {
                out.append(", ");
            }
            out.append(StepKeys.abbreviate(keys.get(index)));
        }
        if (end < keys.size()) {
            out.append(", …(").append(keys.size() - end).append(" more)");
        }
        return out.toString();
    }
}
