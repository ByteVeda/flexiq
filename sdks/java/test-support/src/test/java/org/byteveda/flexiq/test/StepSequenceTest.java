package org.byteveda.flexiq.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.byteveda.flexiq.steps.StepDivergedError;
import org.byteveda.flexiq.steps.StepError;
import org.junit.jupiter.api.Test;

/**
 * The harness's step rules, unit-tested where they are cheapest to pin down.
 *
 * <p>These are a Java restatement of rules that live in the core, so they get
 * two kinds of cover: this file, which asserts each rule directly, and
 * {@code StepHarnessParityTest}, which asserts that a task behaves the same over
 * this backend as over a real worker. Neither alone is enough — this one can
 * agree with itself while drifting, and that one cannot reach every branch.
 */
class StepSequenceTest {

    private static StepRecord run(int seq, String stepKey, String result) {
        return StepRecord.run(seq, stepKey, result.getBytes(StandardCharsets.UTF_8));
    }

    private static StepSequence empty() {
        return new StepSequence("job-1", List.of());
    }

    private static String memoized(StepSequence.RunOutcome outcome) {
        byte[] bytes = outcome.memoized();
        assertNotNull(bytes, "expected a memo hit");
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // ── identity ────────────────────────────────────────────────────

    @Test
    void anUnkeyedStepIsNamedByItsOccurrence() {
        StepSequence sequence = empty();

        StepSequence.RunOutcome first = sequence.beginRun("charge", null);
        assertEquals("charge#0", first.stepKey());
        sequence.commit(first.pending(), 0);

        StepSequence.RunOutcome second = sequence.beginRun("charge", null);
        assertEquals("charge#1", second.stepKey());
    }

    @Test
    void aKeyedStepIsNamedByItsData() {
        assertEquals("fetch:1234", empty().beginRun("fetch", "1234").stepKey());
    }

    @Test
    void aKeyedStepSpendsNoOccurrence() {
        // The whole point of the two forms counting independently: adding a keyed
        // call must not shift the key of a later unkeyed one, which would be a
        // divergence caused by an edit that changed nothing about it.
        StepSequence sequence = empty();
        StepSequence.RunOutcome keyed = sequence.beginRun("charge", "order-7");
        sequence.commit(keyed.pending(), 0);

        assertEquals("charge#0", sequence.beginRun("charge", null).stepKey());
    }

    @Test
    void anEmptyKeyIsRefusedRatherThanNumberedByOccurrence() {
        // The exact drift a shell-side reimplementation produced once: an empty
        // key fell back to positional numbering here while the core raised, so a
        // test passed for a key the real run rejects.
        StepError refused = assertThrows(StepError.class, () -> empty().beginRun("charge", ""));
        assertFalse(refused.shouldRetry(), "a key written that way in the code is refused identically next attempt");
        assertTrue(refused.getMessage().contains("empty key"), refused.getMessage());
    }

    @Test
    void anAmbiguousNameIsRefusedBeforeAnythingRuns() {
        for (String name : List.of("", "charge#1", "charge:1", "n".repeat(129))) {
            StepError refused = assertThrows(StepError.class, () -> empty().beginRun(name, null), name);
            assertFalse(refused.shouldRetry(), name);
        }
    }

    @Test
    void aNameIsMeasuredInBytesNotCharacters() {
        // 65 two-byte characters is 130 bytes: under the character limit, over the
        // byte limit the core enforces.
        assertThrows(StepError.class, () -> empty().beginRun("é".repeat(65), null));
        assertNotNull(empty().beginRun("é".repeat(64), null).stepKey());
    }

    @Test
    void anOversizeKeyIsRefused() {
        assertThrows(StepError.class, () -> empty().beginRun("fetch", "k".repeat(257)));
    }

    @Test
    void aRefusedStepDoesNotSpendItsOccurrence() {
        StepSequence sequence = empty();
        assertThrows(StepError.class, () -> sequence.beginRun("charge", ""));

        // Still #0: a refused step that took a number would shift the next one's
        // key, and the whole point of that key is that it does not move.
        assertEquals("charge#0", sequence.beginRun("charge", null).stepKey());
    }

    // ── the one-at-a-time rule ──────────────────────────────────────

    @Test
    void aSecondStepIsRefusedWhileOneIsUncommitted() {
        StepSequence sequence = empty();
        sequence.beginRun("charge", null);

        StepError refused = assertThrows(StepError.class, () -> sequence.beginRun("receipt", null));
        assertFalse(refused.shouldRetry());
        assertTrue(refused.getMessage().contains("still uncommitted"), refused.getMessage());
    }

    @Test
    void oneExplicitKeyMayNotBeUsedTwiceInAnAttempt() {
        StepSequence sequence = empty();
        StepSequence.RunOutcome first = sequence.beginRun("charge", "order-7");
        sequence.commit(first.pending(), 0);

        StepError refused = assertThrows(StepError.class, () -> sequence.beginRun("charge", "order-7"));
        assertTrue(refused.getMessage().contains("used twice"), refused.getMessage());
    }

    @Test
    void aStepCommittedOutOfTurnIsRefused() {
        StepSequence sequence = empty();
        sequence.beginRun("charge", null);

        assertThrows(StepError.class, () -> sequence.commit(new StepSequence.PendingStep(0, "receipt#0", false), 0));
    }

    // ── replay ──────────────────────────────────────────────────────

    @Test
    void aRecordedStepReplaysItsStoredBytes() {
        StepSequence sequence = new StepSequence("job-1", List.of(run(0, "charge#0", "receipt-1")));

        StepSequence.RunOutcome outcome = sequence.beginRun("charge", null);
        assertNull(outcome.pending(), "a memo hit runs nothing");
        assertEquals("receipt-1", memoized(outcome));
    }

    @Test
    void aReplayCannotMutateTheStoredBytes() {
        byte[] stored = "receipt-1".getBytes(StandardCharsets.UTF_8);
        StepSequence sequence = new StepSequence("job-1", List.of(StepRecord.run(0, "charge#0", stored)));

        byte[] replayed = sequence.beginRun("charge", null).memoized();
        assertNotNull(replayed);
        replayed[0] = 'X';

        assertArrayEquals("receipt-1".getBytes(StandardCharsets.UTF_8), stored);
    }

    @Test
    void aKeyedStepIsMatchedWhereverItSits() {
        // The assertion that separates key matching from position matching:
        // counting how often bodies ran cannot tell them apart.
        StepSequence sequence =
                new StepSequence("job-1", List.of(run(0, "hello:alice", "alice"), run(1, "hello:bob", "bob")));

        assertEquals("bob", memoized(sequence.beginRun("hello", "bob")));
        assertEquals("alice", memoized(sequence.beginRun("hello", "alice")));
    }

    @Test
    void anUnkeyedStepSkipsWhatAKeyedHitAlreadyClaimed() {
        StepSequence sequence =
                new StepSequence("job-1", List.of(run(0, "a#0", "a"), run(1, "b:x", "b"), run(2, "c#0", "c")));

        assertEquals("b", memoized(sequence.beginRun("b", "x")));
        assertEquals("a", memoized(sequence.beginRun("a", null)));
        assertEquals("c", memoized(sequence.beginRun("c", null)));
    }

    @Test
    void newGroundTakesTheNextFreePositionNotTheWalksPosition() {
        // The snapshot is empty, so the positional walk never moves — every
        // position this attempt takes has to come from what it has committed.
        StepSequence sequence = empty();
        StepSequence.RunOutcome first = sequence.beginRun("a", null);
        sequence.commit(first.pending(), 1);

        StepSequence.RunOutcome second = sequence.beginRun("b", null);
        assertNull(second.memoized());
        assertEquals(1, second.pending().seq());
    }

    @Test
    void newGroundPastAMemoTakesThePositionAfterIt() {
        StepSequence sequence = new StepSequence("job-1", List.of(run(0, "a#0", "a")));
        sequence.beginRun("a", null);

        StepSequence.RunOutcome fresh = sequence.beginRun("b", null);
        assertNull(fresh.memoized());
        assertEquals(1, fresh.pending().seq());
    }

    // ── divergence ──────────────────────────────────────────────────

    @Test
    void aChangedSequenceDivergesAtTheStepThatChanged() {
        StepSequence sequence = new StepSequence("job-1", List.of(run(0, "charge#0", "x"), run(1, "email#0", "y")));

        StepSequence.RunOutcome first = sequence.beginRun("charge", null);
        assertEquals("x", memoized(first));

        StepDivergedError diverged = assertThrows(StepDivergedError.class, () -> sequence.beginRun("refund", null));
        assertFalse(diverged.shouldRetry(), "a divergence must never spend the retry budget");
        assertTrue(diverged.getMessage().contains("email#0"), diverged.getMessage());
        assertTrue(diverged.getMessage().contains("refund#0"), diverged.getMessage());
    }

    @Test
    void aRunReplayingOntoARecordedSleepDiverges() {
        StepSequence sequence = new StepSequence("job-1", List.of(StepRecord.sleep(0, "cooldown#0", 10)));

        StepDivergedError diverged = assertThrows(StepDivergedError.class, () -> sequence.beginRun("cooldown", null));
        assertTrue(diverged.getMessage().contains("as a sleep step"), diverged.getMessage());
        assertTrue(diverged.getMessage().contains("as a run step"), diverged.getMessage());
    }

    @Test
    void aHoleInTheRecordedSequenceIsRefused() {
        // A hole would silently shift every memo after it.
        assertThrows(StepError.class, () -> new StepSequence("job-1", List.of(run(1, "charge#0", "x"))));
    }

    @Test
    void aDivergenceMessageDoesNotPasteAThousandKeys() {
        List<StepRecord> recorded = new java.util.ArrayList<>();
        for (int seq = 0; seq < 200; seq++) {
            recorded.add(run(seq, "step" + seq + "#0", "v"));
        }
        StepSequence sequence = new StepSequence("job-1", recorded);

        StepDivergedError diverged = assertThrows(StepDivergedError.class, () -> sequence.beginRun("other", null));
        assertTrue(diverged.getMessage().contains("more)"), diverged.getMessage());
        assertTrue(
                diverged.getMessage().length() < 1000,
                "message was " + diverged.getMessage().length() + " chars");
    }

    // ── sleep ───────────────────────────────────────────────────────

    @Test
    void anUnnamedSleepIsNumberedUnderADefaultName() {
        assertEquals("sleep#0", empty().beginSleep(null, null, 0).stepKey());
    }

    @Test
    void afreshSleepIsNewGround() {
        StepSequence.SleepOutcome outcome = empty().beginSleep("cooldown", null, 100);

        assertFalse(outcome.elapsed());
        assertTrue(outcome.fresh());
        assertEquals(0, outcome.pending().seq());
    }

    @Test
    void aRecordedSleepPastItsDeadlineElapses() {
        StepSequence sequence = new StepSequence("job-1", List.of(StepRecord.sleep(0, "cooldown#0", 100)));

        StepSequence.SleepOutcome outcome = sequence.beginSleep("cooldown", null, 100);
        assertTrue(outcome.elapsed(), "now >= wakeAt is the whole completeness test");
        assertEquals(100, outcome.wakeAt());
        assertNull(outcome.pending());
    }

    @Test
    void aRecordedSleepStillAheadResumesAtItsRecordedPosition() {
        // Re-issued where it sits, so the store answers with the deadline it
        // already holds. A fresh position would start the clock again.
        StepSequence sequence =
                new StepSequence("job-1", List.of(run(0, "charge#0", "x"), StepRecord.sleep(1, "cooldown#0", 500)));
        sequence.beginRun("charge", null);

        StepSequence.SleepOutcome outcome = sequence.beginSleep("cooldown", null, 100);
        assertFalse(outcome.elapsed());
        assertFalse(outcome.fresh(), "a resume writes nothing to count");
        assertEquals(1, outcome.pending().seq());
        assertEquals(500, outcome.wakeAt());
    }

    @Test
    void aResumeDoesNotAdvanceTheCommittedCount() {
        StepSequence sequence = new StepSequence("job-1", List.of(StepRecord.sleep(0, "cooldown#0", 500)));
        StepSequence.SleepOutcome outcome = sequence.beginSleep("cooldown", null, 100);

        sequence.commitSleep(outcome.pending(), false);
        assertEquals(1, sequence.committedSteps(), "counting a resume would put the sequence ahead of the store");
    }

    // ── the tail ────────────────────────────────────────────────────

    @Test
    void stepsTheCodeNoLongerRunsAreReportedAsOrphans() {
        StepSequence sequence = new StepSequence("job-1", List.of(run(0, "a#0", "a"), run(1, "b#0", "b")));
        sequence.beginRun("a", null);

        assertEquals(List.of("b#0"), sequence.orphanedTail());
    }

    @Test
    void aSequenceTheCodeWalkedFullyHasNoOrphans() {
        StepSequence sequence = new StepSequence("job-1", List.of(run(0, "a#0", "a")));
        sequence.beginRun("a", null);

        assertTrue(sequence.orphanedTail().isEmpty());
    }
}
