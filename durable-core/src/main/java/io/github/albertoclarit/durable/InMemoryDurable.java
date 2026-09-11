package io.github.albertoclarit.durable;

import io.github.albertoclarit.durable.internal.DefaultDurable;
import io.github.albertoclarit.durable.internal.InMemoryWorkflowStore;
import io.github.albertoclarit.durable.internal.NoOpJobInjector;
import io.github.albertoclarit.durable.internal.WorkflowRuntime;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;

/**
 * In-process durable engine for tests and local use. State is not shared across JVMs.
 */
public final class InMemoryDurable implements AutoCloseable {

    private final MutableClock clock;
    private final InMemoryWorkflowStore store;
    private final WorkflowRuntime runtime;
    private final DefaultDurable durable;

    public static InMemoryDurable create() {
        return create(new MutableClock(Instant.parse("2026-01-01T00:00:00Z")));
    }

    static InMemoryDurable create(MutableClock clock) {
        InMemoryWorkflowStore store = new InMemoryWorkflowStore(clock);
        WorkflowRuntime runtime = new WorkflowRuntime(store, clock, Duration.ofMillis(20), Duration.ofSeconds(30), "in-memory");
        DefaultDurable durable = new DefaultDurable(store, runtime, clock, new NoOpJobInjector(), ignored -> {});
        runtime.setExecutor(durable);
        runtime.start();
        return new InMemoryDurable(clock, store, runtime, durable);
    }

    /**
     * Two runtimes sharing one store: models worker crash and another worker resuming.
     */
    public static SharedStore sharedStore() {
        MutableClock clock = new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        InMemoryWorkflowStore store = new InMemoryWorkflowStore(clock);
        return new SharedStore(clock, store);
    }

    private InMemoryDurable(MutableClock clock, InMemoryWorkflowStore store, WorkflowRuntime runtime, DefaultDurable durable) {
        this.clock = clock;
        this.store = store;
        this.runtime = runtime;
        this.durable = durable;
    }

    public Durable client() {
        return durable;
    }

    public Instant now() {
        return clock.instant();
    }

    public void advance(Duration duration) {
        clock.advance(duration);
        runtime.kick();
    }

    /**
     * After this many newly executed (not replayed) operations, the worker drops the lease
     * without failing the workflow.
     */
    public void crashAfterExecutions(int count) {
        durable.setCrashAfterExecutions(count);
    }

    InMemoryWorkflowStore store() {
        return store;
    }

    @Override
    public void close() {
        runtime.close();
    }

    public static final class SharedStore {
        private final MutableClock clock;
        private final InMemoryWorkflowStore store;

        SharedStore(MutableClock clock, InMemoryWorkflowStore store) {
            this.clock = clock;
            this.store = store;
        }

        public InMemoryDurable openWorker(String workerId) {
            WorkflowRuntime runtime = new WorkflowRuntime(store, clock, Duration.ofMillis(20), Duration.ofSeconds(30), workerId);
            DefaultDurable durable = new DefaultDurable(store, runtime, clock, new NoOpJobInjector(), ignored -> {});
            runtime.setExecutor(durable);
            runtime.start();
            return new InMemoryDurable(clock, store, runtime, durable);
        }

        public void advance(Duration duration) {
            clock.advance(duration);
        }
    }

    public static final class MutableClock extends Clock {
        private final AtomicReference<Instant> instant;
        private final ZoneId zone = ZoneId.of("UTC");

        public MutableClock(Instant start) {
            this.instant = new AtomicReference<>(start);
        }

        public void advance(Duration duration) {
            instant.updateAndGet(t -> t.plus(duration));
        }

        @Override
        public ZoneId getZone() {
            return zone;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant.get();
        }
    }
}
