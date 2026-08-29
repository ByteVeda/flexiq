package org.byteveda.flexiq.steps;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.flexiq.JobContext;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.task.TaskFunction;
import org.byteveda.flexiq.worker.Executor;
import org.byteveda.flexiq.worker.FakeScheduler;
import org.byteveda.flexiq.worker.Handler;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * §9 — durable steps for a job running on an attached executor.
 *
 * <p>This process holds no database and no execution claim, so neither half of
 * a step happens locally: the snapshot a replay answers from rides in on the
 * dispatch, and every new step crosses to the scheduler, which writes it under
 * the claim it holds. The wire is the contract, so these assert on frames.
 */
class ExecutorStepsTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonSerializer SERIALIZER = new JsonSerializer();

    /** What a scheduler with a step store advertises. */
    private static final List<String> WITH_STEPS = List.of("steps");

    private @Nullable FakeScheduler scheduler;
    private @Nullable Executor executor;

    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.close();
            executor = null;
        }
        if (scheduler != null) {
            scheduler.close();
            scheduler = null;
        }
    }

    private FakeScheduler listen(List<String> capabilities) throws Exception {
        FakeScheduler fake = new FakeScheduler(false, capabilities);
        scheduler = fake;
        return fake;
    }

    /** Attach an executor running {@code handler}, and wait for the handshake. */
    private Executor attach(FakeScheduler fake, Handler<String, ?> handler) throws Exception {
        Executor started = Executor.builder()
                .register(handler)
                .attach("127.0.0.1:" + fake.port())
                .heartbeatIntervalMs(50)
                .shutdownDrainMs(5_000)
                .start();
        executor = started;
        fake.awaitHello();
        return started;
    }

    /** The one task these tests dispatch, whose body drives {@code ctx.step()}. */
    private static <R> Handler<String, R> checkout(TaskFunction<String, R> body) {
        return Handler.of(Task.of("checkout", String.class), body);
    }

    private static byte[] call(String argument) {
        return SERIALIZER.serialize(argument);
    }

    @Test
    @Timeout(60)
    void announcesThatItCanRunDurableSteps() throws Exception {
        // Only an executor whose job context can actually open a session may
        // claim this: a scheduler sends the snapshot to nobody who would
        // discard it. Task bodies run in this process, so it can.
        FakeScheduler fake = listen(List.of());
        attach(fake, checkout(who -> "hello " + who));

        JsonNode capabilities = fake.awaitHello().get("capabilities");

        assertNotNull(capabilities, "the hello must carry a capability list");
        List<String> announced = new ArrayList<>();
        capabilities.forEach(capability -> announced.add(capability.asText()));
        // Membership, not equality: a second capability here must not turn this
        // into a failing test.
        assertTrue(announced.contains("steps"), "expected steps to be advertised, got " + announced);
    }

    @Test
    @Timeout(60)
    void replaysAStepFromTheDispatchSnapshotInsteadOfRunningIt() throws Exception {
        // The read half of §9: one snapshot per attempt, and it is the
        // scheduler's read, not one this process has credentials to make.
        FakeScheduler fake = listen(WITH_STEPS);
        AtomicInteger charges = new AtomicInteger();
        attach(fake, checkout(who -> JobContext.current().step().run("charge", String.class, () -> {
            charges.incrementAndGet();
            return "ch_fresh";
        })));

        fake.sendJobSteps(
                "job-1", List.of(FakeScheduler.SnapshotStep.run(0, "charge#0", SERIALIZER.serialize("ch_1"))));
        fake.sendJob("job-1", "checkout", call("ada"));

        FakeScheduler.Frame result = fake.nextFrame();
        assertEquals("success", result.type());
        // The point of the whole feature, over a wire this time: the card is not
        // charged again on the attempt that replays it.
        assertEquals(0, charges.get(), "a memoized step must not run its body again");
        assertEquals("ch_1", JSON.readTree(result.payload()).asText());
    }

    @Test
    @Timeout(60)
    void commitsANewStepThroughTheSchedulerAndWaitsForTheAck() throws Exception {
        FakeScheduler fake = listen(WITH_STEPS);
        attach(fake, checkout(who -> JobContext.current().step().run("charge", String.class, () -> "ch_1")));

        fake.sendJob("job-1", "checkout", call("ada"));

        FakeScheduler.Frame commit = fake.nextFrame("step_commit");
        assertEquals("job-1", commit.header().path("job_id").asText());
        assertEquals(0, commit.header().path("seq").asInt());
        assertEquals("charge#0", commit.header().path("step_key").asText());
        assertEquals("run", commit.header().path("kind").asText());
        // Post-serializer, post-codec: these are the bytes the scheduler stores,
        // and the ones a replay hands back.
        assertEquals("ch_1", JSON.readTree(commit.payload()).asText());
        // No owner rides with a commit, and none may: an owner an executor fills
        // in is an owner it can forge, and a forged one writes straight into the
        // live attempt's sequence.
        assertTrue(commit.header().get("owner") == null, "a step commit must carry no owner");

        fake.ackStep(commit, null);
        assertEquals("success", fake.nextFrame().type(), "the attempt continues once the commit is durable");
    }

    @Test
    @Timeout(60)
    void endsTheAttemptInASleepTheSchedulerSettled() throws Exception {
        // Two frames, not one: `step.sleep` has to return the deadline storage
        // settled on before the body unwinds, and the terminal frame is only
        // written once it has.
        FakeScheduler fake = listen(WITH_STEPS);
        long settled = System.currentTimeMillis() + 90_000;
        attach(fake, checkout(who -> {
            JobContext.current().step().sleep(Duration.ofHours(1));
            return "never reached this attempt";
        }));

        fake.sendJob("job-1", "checkout", call("ada"));

        FakeScheduler.Frame commit = fake.nextFrame("step_commit");
        assertEquals("sleep", commit.header().path("kind").asText());
        assertEquals(0, commit.header().path("payload_len").asInt(), "a sleep commits no bytes");
        // The ack echoes the deadline the job was *actually* rescheduled to,
        // which on a replay is the stored one rather than the one proposed here.
        fake.ackStep(commit, settled);

        FakeScheduler.Frame slept = fake.nextFrame("slept");
        assertEquals(settled, slept.header().path("wake_at").asLong());
    }

    @Test
    @Timeout(60)
    void failsTheAttemptRetryablyWhenACommitIsNeverAcknowledged() throws Exception {
        // The one genuinely uncertain case in §9.2's taxonomy. An unconfirmed
        // commit is indistinguishable from one that never happened, so the
        // attempt must not proceed past it — and the replay re-runs the step
        // under the same downstream idempotency key, which is what makes
        // retrying safe.
        FakeScheduler fake = listen(WITH_STEPS);
        AtomicReference<@Nullable Throwable> refusal = new AtomicReference<>();
        CountDownLatch refused = new CountDownLatch(1);
        attach(fake, checkout(who -> {
            try {
                return JobContext.current().step().run("charge", String.class, () -> "ch_1");
            } catch (Throwable t) {
                refusal.set(t);
                refused.countDown();
                throw t;
            }
        }));

        fake.sendJob("job-1", "checkout", call("ada"));

        // Asserted in the handler rather than off a frame: the connection
        // carrying the answer is the one being dropped, so there is no failure
        // frame to read.
        FakeScheduler.Frame commit = fake.nextFrame("step_commit");
        assertEquals("charge#0", commit.header().path("step_key").asText());
        fake.disconnect();

        assertTrue(refused.await(FakeScheduler.SETTLE_MS, TimeUnit.MILLISECONDS), "the commit never came back");
        StepUnavailableError error =
                assertInstanceOf(StepUnavailableError.class, refusal.get(), "an unconfirmed commit is retryable");
        assertTrue(error.shouldRetry(), "the next attempt may land somewhere that can commit");
    }

    @Test
    @Timeout(60)
    void refusesAStepWhenTheSchedulerOffersNoStepStore() throws Exception {
        // §9.4, and it stays: an executor attached to a scheduler that never
        // advertised the capability has no channel to commit on, and says so.
        // Retryably, because a fleet mid-rollout may place the next attempt
        // somewhere that can commit — and there is no version of "your charge
        // step silently lost its memo" that beats a failure naming the reason.
        FakeScheduler fake = listen(List.of());
        AtomicInteger charges = new AtomicInteger();
        Executor started =
                attach(fake, checkout(who -> JobContext.current().step().run("charge", String.class, () -> {
                    charges.incrementAndGet();
                    return "ch_1";
                })));
        assertFalse(started.supportsSteps(), "this scheduler advertised no step store");

        fake.sendJob("job-1", "checkout", call("ada"));

        JsonNode result = fake.nextResult();
        assertEquals("failure", result.path("type").asText());
        assertTrue(result.path("should_retry").asBoolean(), "a fleet mid-rollout must still make progress");
        assertTrue(
                result.path("error").asText().contains("no step store"),
                result.path("error").asText());
        assertEquals(0, charges.get(), "refusing after the side effect would be worse than not refusing at all");
    }
}
