package io.github.albertoclarit.durable;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DurableProgrammingModelTest {

    @Test
    void startIsIdempotentUnderConcurrency() throws Exception {
        try (InMemoryDurable env = InMemoryDurable.create()) {
            Durable durable = env.client();
            CountDownLatch ready = new CountDownLatch(8);
            CountDownLatch go = new CountDownLatch(1);
            List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                Thread thread = Thread.ofVirtual().start(() -> {
                    ready.countDown();
                    try {
                        go.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    durable.start("settlement-123", new EchoJob("ok"));
                });
                threads.add(thread);
            }
            ready.await();
            go.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            assertEquals("ok", durable.start("settlement-123", new EchoJob("ok")).result(Duration.ofSeconds(5)));
        }
    }

    @Test
    void existingFailedWorkflowIsNotReplaced() {
        FailFastJob.calls.set(0);
        try (InMemoryDurable env = InMemoryDurable.create()) {
            Durable durable = env.client();
            WorkflowHandle<String> first = durable.start("fail-1", new FailFastJob());
            assertThrows(WorkflowFailedException.class, () -> first.result(Duration.ofSeconds(5)));
            WorkflowHandle<String> second = durable.start("fail-1", new FailFastJob());
            assertEquals(WorkflowStatus.FAILED, second.status());
            assertThrows(WorkflowFailedException.class, () -> second.result(Duration.ofSeconds(1)));
        }
    }

    @Test
    void incompatibleDefinitionIsRejected() {
        try (InMemoryDurable env = InMemoryDurable.create()) {
            Durable durable = env.client();
            durable.start("job-1", new EchoJob("a")).result(Duration.ofSeconds(5));
            assertThrows(IncompatibleWorkflowException.class, () -> durable.start("job-1", new EchoJob("b")));
        }
    }

    @Test
    void replaySkipsCompletedOperationsAfterCrash() {
        Calls.a.set(0);
        Calls.b.set(0);
        Calls.c.set(0);
        InMemoryDurable.SharedStore shared = InMemoryDurable.sharedStore();
        try (InMemoryDurable workerA = shared.openWorker("a")) {
            workerA.crashAfterExecutions(2);
            workerA.client().start("wf", new AbcJob());
            waitUntil(() -> Calls.b.get() == 1 && Calls.c.get() == 0, Duration.ofSeconds(5));
        }
        assertEquals(1, Calls.a.get());
        assertEquals(1, Calls.b.get());
        assertEquals(0, Calls.c.get());
        try (InMemoryDurable workerB = shared.openWorker("b")) {
            assertEquals("abc", workerB.client().start("wf", new AbcJob()).result(Duration.ofSeconds(5)));
        }
        assertEquals(1, Calls.a.get());
        assertEquals(1, Calls.b.get());
        assertEquals(1, Calls.c.get());
    }

    @Test
    void failFastWhenNoRetryPolicy() {
        FailFastJob.calls.set(0);
        try (InMemoryDurable env = InMemoryDurable.create()) {
            assertThrows(
                    WorkflowFailedException.class,
                    () -> env.client().start("ff", new FailFastJob()).result(Duration.ofSeconds(5))
            );
            assertEquals(1, FailFastJob.calls.get());
        }
    }

    @Test
    void explicitRetrySurvivesWorkerRestart() {
        RetryJob.calls.set(0);
        InMemoryDurable.SharedStore shared = InMemoryDurable.sharedStore();
        try (InMemoryDurable workerA = shared.openWorker("a")) {
            workerA.client().start("retry", new RetryJob());
            awaitStatus(workerA.client(), "retry", WorkflowStatus.WAITING, Duration.ofSeconds(5));
        }
        assertEquals(1, RetryJob.calls.get());
        shared.advance(Duration.ofSeconds(10));
        try (InMemoryDurable workerB = shared.openWorker("b")) {
            workerB.advance(Duration.ZERO);
            assertEquals("ok", workerB.client().find("retry").orElseThrow().result(Duration.ofSeconds(5)));
        }
        assertEquals(2, RetryJob.calls.get());
    }

    @Test
    void durableSleepDoesNotHoldAThread() {
        try (InMemoryDurable env = InMemoryDurable.create()) {
            WorkflowHandle<String> handle = env.client().start("sleep", new SleepJob());
            awaitStatus(env.client(), "sleep", WorkflowStatus.WAITING, Duration.ofSeconds(5));
            env.advance(Duration.ofHours(6));
            assertEquals("woke", handle.result(Duration.ofSeconds(5)));
        }
    }

    @Test
    void awaitReceivesSignal() {
        try (InMemoryDurable env = InMemoryDurable.create()) {
            Durable durable = env.client();
            WorkflowHandle<String> handle = durable.start("sig", new AwaitJob());
            awaitStatus(durable, "sig", WorkflowStatus.WAITING, Duration.ofSeconds(5));
            durable.signal("sig", "bank-confirmation", "CONFIRMED");
            assertEquals("CONFIRMED", handle.result(Duration.ofSeconds(5)));
        }
    }

    @Test
    void signalBeforeAwaitIsNotLost() {
        try (InMemoryDurable env = InMemoryDurable.create()) {
            Durable durable = env.client();
            durable.start("early-await", new AwaitJob());
            durable.signal("early-await", "bank-confirmation", "DONE");
            assertEquals("DONE", durable.find("early-await").orElseThrow().result(Duration.ofSeconds(5)));
        }
    }

    @Test
    void nowAndNewIdAreStableAcrossReplay() {
        DeterminismJob.firstNow.set(null);
        DeterminismJob.firstId.set(null);
        InMemoryDurable.SharedStore shared = InMemoryDurable.sharedStore();
        try (InMemoryDurable workerA = shared.openWorker("a")) {
            workerA.crashAfterExecutions(2);
            workerA.client().start("det", new DeterminismJob());
            waitUntil(() -> DeterminismJob.firstNow.get() != null && DeterminismJob.firstId.get() != null, Duration.ofSeconds(5));
        }
        Instant storedNow = DeterminismJob.firstNow.get();
        UUID storedId = DeterminismJob.firstId.get();
        try (InMemoryDurable workerB = shared.openWorker("b")) {
            String result = workerB.client().start("det", new DeterminismJob()).result(Duration.ofSeconds(5));
            assertEquals(storedNow + "/" + storedId, result);
        }
    }

    @Test
    void childWorkflowIsIdempotentAndTyped() {
        try (InMemoryDurable env = InMemoryDurable.create()) {
            String result = env.client().start("parent", new ParentJob()).result(Duration.ofSeconds(5));
            assertEquals("child:v", result);
            env.client().start("parent", new ParentJob()).result(Duration.ofSeconds(5));
        }
    }

    @Test
    void duplicateOperationNameFails() {
        try (InMemoryDurable env = InMemoryDurable.create()) {
            assertThrows(
                    WorkflowFailedException.class,
                    () -> env.client().start("dup", new DuplicateNameJob()).result(Duration.ofSeconds(5))
            );
        }
    }

    @Test
    void typedValuesPassBetweenOperations() {
        try (InMemoryDurable env = InMemoryDurable.create()) {
            Payload result = env.client().start("typed", new TypedJob()).result(Duration.ofSeconds(5));
            assertEquals("Ada", result.name());
            assertEquals(2, result.count());
        }
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        assertTrue(condition.getAsBoolean(), "timed out waiting for condition");
    }

    private static void awaitStatus(Durable durable, String id, WorkflowStatus expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            WorkflowStatus status = durable.find(id).orElseThrow().status();
            if (status == expected || status == WorkflowStatus.COMPLETED || status == WorkflowStatus.FAILED) {
                if (status == expected) {
                    return;
                }
                if (expected == WorkflowStatus.RUNNING && status == WorkflowStatus.RUNNING) {
                    return;
                }
            }
            if (status == expected) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        assertEquals(expected, durable.find(id).orElseThrow().status());
    }

    public static final class Calls {
        static final AtomicInteger a = new AtomicInteger();
        static final AtomicInteger b = new AtomicInteger();
        static final AtomicInteger c = new AtomicInteger();
    }

    public static class EchoJob implements DurableJob<String> {
        private final String value;

        public EchoJob() {
            this("");
        }

        public EchoJob(String value) {
            this.value = value;
        }

        @Override
        public String run(Context ctx) {
            return ctx.step("echo", () -> value);
        }
    }

    public static class FailFastJob implements DurableJob<String> {
        static final AtomicInteger calls = new AtomicInteger();

        @Override
        public String run(Context ctx) {
            return ctx.activity("boom", () -> {
                calls.incrementAndGet();
                throw new IllegalStateException("no retry");
            });
        }
    }

    public static class AbcJob implements DurableJob<String> {
        @Override
        public String run(Context ctx) {
            var a = ctx.step("A", () -> {
                Calls.a.incrementAndGet();
                return "a";
            });
            var b = ctx.activity("B", () -> {
                Calls.b.incrementAndGet();
                return a + "b";
            });
            return ctx.step("C", () -> {
                Calls.c.incrementAndGet();
                return b + "c";
            });
        }
    }

    public static class RetryJob implements DurableJob<String> {
        static final AtomicInteger calls = new AtomicInteger();

        @Override
        public String run(Context ctx) {
            return ctx.activity("flaky", RetryPolicy.exponentialBackoff(5, Duration.ofSeconds(10)), () -> {
                if (calls.incrementAndGet() == 1) {
                    throw new IllegalStateException("transient");
                }
                return "ok";
            });
        }
    }

    public static class SleepJob implements DurableJob<String> {
        @Override
        public String run(Context ctx) {
            ctx.sleep(Duration.ofHours(6));
            return "woke";
        }
    }

    public static class AwaitJob implements DurableJob<String> {
        @Override
        public String run(Context ctx) {
            return ctx.await("bank-confirmation");
        }
    }

    public static class DeterminismJob implements DurableJob<String> {
        static final java.util.concurrent.atomic.AtomicReference<Instant> firstNow = new java.util.concurrent.atomic.AtomicReference<>();
        static final java.util.concurrent.atomic.AtomicReference<UUID> firstId = new java.util.concurrent.atomic.AtomicReference<>();

        @Override
        public String run(Context ctx) {
            Instant now = ctx.now();
            UUID id = ctx.newId();
            firstNow.compareAndSet(null, now);
            firstId.compareAndSet(null, id);
            ctx.step("marker", () -> "x");
            return ctx.step("join", () -> now + "/" + id);
        }
    }

    public static class ChildJob implements DurableJob<String> {
        private final String value;

        public ChildJob() {
            this("");
        }

        public ChildJob(String value) {
            this.value = value;
        }

        @Override
        public String run(Context ctx) {
            return ctx.step("work", () -> "child:" + value);
        }
    }

    public static class ParentJob implements DurableJob<String> {
        @Override
        public String run(Context ctx) {
            return ctx.startChild("payout", new ChildJob("v")).result();
        }
    }

    public static class DuplicateNameJob implements DurableJob<String> {
        @Override
        public String run(Context ctx) {
            ctx.step("same", () -> 1);
            ctx.step("same", () -> 2);
            return "no";
        }
    }

    public record Payload(String name, int count) {
    }

    public static class TypedJob implements DurableJob<Payload> {
        @Override
        public Payload run(Context ctx) {
            var loaded = ctx.step("load", () -> new Payload("Ada", 1));
            return ctx.activity("inc", () -> new Payload(loaded.name(), loaded.count() + 1));
        }
    }
}
