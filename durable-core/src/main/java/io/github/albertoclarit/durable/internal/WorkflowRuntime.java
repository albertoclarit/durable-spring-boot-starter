package io.github.albertoclarit.durable.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Dedicated poller thread plus virtual threads for workflow attempts.
 * This is not Spring {@code @Scheduled}.
 */
public final class WorkflowRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRuntime.class);

    private final WorkflowStore store;
    private final Clock clock;
    private final Duration pollInterval;
    private final Duration lease;
    private final String workerId;
    private final Object tick = new Object();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
    private DefaultDurable executor;
    private Thread poller;

    public WorkflowRuntime(
            WorkflowStore store,
            Clock clock,
            Duration pollInterval,
            Duration lease,
            String workerId
    ) {
        this.store = store;
        this.clock = clock;
        this.pollInterval = pollInterval;
        this.lease = lease;
        this.workerId = workerId;
    }

    public void setExecutor(DefaultDurable executor) {
        this.executor = executor;
    }

    public String workerId() {
        return workerId;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        poller = Thread.ofPlatform().name("durable-poller-" + workerId).daemon(true).unstarted(this::loop);
        poller.start();
    }

    public void halt() {
        running.set(false);
        kick();
    }

    public void kick() {
        synchronized (tick) {
            tick.notifyAll();
        }
    }

    public void awaitTick(Duration timeout) {
        synchronized (tick) {
            try {
                tick.wait(timeout.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void loop() {
        while (running.get()) {
            try {
                var id = store.claimNext(workerId, lease, clock.instant());
                if (id.isEmpty()) {
                    synchronized (tick) {
                        tick.wait(pollInterval.toMillis());
                    }
                    continue;
                }
                String workflowId = id.get();
                try {
                    workers.submit(() -> runClaimed(workflowId));
                } catch (RejectedExecutionException e) {
                    store.releaseLease(workflowId, workerId);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.warn("Poller error", e);
            }
        }
    }

    private void runClaimed(String workflowId) {
        try {
            executor.execute(workflowId);
        } catch (RuntimeException e) {
            log.warn("Workflow {} execution error", workflowId, e);
        } finally {
            if (executor == null || !executor.didCrash()) {
                store.releaseLease(workflowId, workerId);
            }
            kick();
        }
    }

    @Override
    public void close() {
        running.set(false);
        kick();
        if (poller != null) {
            poller.interrupt();
        }
        workers.shutdown();
        try {
            workers.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        store.releaseWorker(workerId);
    }
}
