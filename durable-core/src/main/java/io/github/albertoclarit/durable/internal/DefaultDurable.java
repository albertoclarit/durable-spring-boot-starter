package io.github.albertoclarit.durable.internal;

import io.github.albertoclarit.durable.Durable;
import io.github.albertoclarit.durable.DurableJob;
import io.github.albertoclarit.durable.IncompatibleWorkflowException;
import io.github.albertoclarit.durable.WorkflowFailedException;
import io.github.albertoclarit.durable.WorkflowHandle;
import io.github.albertoclarit.durable.WorkflowNotFoundException;
import io.github.albertoclarit.durable.WorkflowStatus;
import io.github.albertoclarit.durable.WorkflowWakePublisher;
import io.github.albertoclarit.durable.internal.WorkflowStore.CreateResult;
import io.github.albertoclarit.durable.internal.WorkflowStore.WorkflowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public final class DefaultDurable implements Durable {

    private static final Logger log = LoggerFactory.getLogger(DefaultDurable.class);

    private final WorkflowStore store;
    private final WorkflowRuntime runtime;
    private final Clock clock;
    private final JobInjector injector;
    private final JsonCodec codec = new JsonCodec();
    private final WorkflowWakePublisher wakePublisher;
    private final AtomicInteger crashAfter = new AtomicInteger(-1);
    private volatile boolean crashed;

    public DefaultDurable(
            WorkflowStore store,
            WorkflowRuntime runtime,
            Clock clock,
            JobInjector injector,
            WorkflowWakePublisher wakePublisher
    ) {
        this.store = store;
        this.runtime = runtime;
        this.clock = clock;
        this.injector = injector;
        this.wakePublisher = wakePublisher == null ? id -> {} : wakePublisher;
    }

    public void setCrashAfterExecutions(int count) {
        crashed = false;
        crashAfter.set(count);
    }

    boolean didCrash() {
        return crashed;
    }

    @Override
    public <T> WorkflowHandle<T> start(String workflowId, DurableJob<T> job) {
        return startInternal(workflowId, job, null, null);
    }

    <T> WorkflowHandle<T> startInternal(String workflowId, DurableJob<T> job, String parentId, String childName) {
        Objects.requireNonNull(workflowId, "workflowId");
        Objects.requireNonNull(job, "job");
        if (workflowId.isBlank()) {
            throw new IllegalArgumentException("workflowId is required");
        }
        String jobJson = codec.write(job);
        String fingerprint = codec.fingerprint(job);
        CreateResult result = store.createOrGet(
                workflowId,
                job.getClass().getName(),
                jobJson,
                fingerprint,
                parentId,
                childName
        );
        WorkflowRecord record = result.record();
        if (!result.created()) {
            if (!record.fingerprint().equals(fingerprint) || !record.jobType().equals(job.getClass().getName())) {
                throw new IncompatibleWorkflowException(workflowId);
            }
            return new DefaultWorkflowHandle<>(workflowId, store, codec, runtime);
        }
        runtime.kick();
        wakePublisher.wake(workflowId);
        return new DefaultWorkflowHandle<>(workflowId, store, codec, runtime);
    }

    @Override
    public void signal(String workflowId, String signalName, Object payload) {
        WorkflowRecord record = store.find(workflowId).orElseThrow(() -> new WorkflowNotFoundException(workflowId));
        store.putSignal(workflowId, signalName, codec.wrap(payload));
        if (record.status() == WorkflowStatus.WAITING && signalName.equals(record.waitingSignal())) {
            store.markRunning(workflowId);
        }
        runtime.kick();
        wakePublisher.wake(workflowId);
    }

    @Override
    public Optional<WorkflowHandle<?>> find(String workflowId) {
        return store.find(workflowId).map(r -> new DefaultWorkflowHandle<>(r.id(), store, codec, runtime));
    }

    void execute(String workflowId) {
        if (crashed) {
            return;
        }
        WorkflowRecord record = store.find(workflowId).orElse(null);
        if (record == null) {
            return;
        }
        if (record.status() == WorkflowStatus.COMPLETED || record.status() == WorkflowStatus.FAILED) {
            return;
        }
        DurableJob<?> job = codec.readJob(record.jobType(), record.jobJson());
        injector.inject(job);
        AtomicInteger crash = crashAfter.get() > 0 ? crashAfter : null;
        ReplayContext ctx = new ReplayContext(workflowId, store, codec, clock, this, crash);
        try {
            Object result = job.run(ctx);
            store.complete(workflowId, codec.wrap(result));
            wakeParents(workflowId);
        } catch (WorkflowSuspended ignored) {
            log.debug("Workflow {} suspended", workflowId);
            runtime.kick();
        } catch (SimulatedCrash ignored) {
            log.warn("Simulated crash while executing {}", workflowId);
            crashed = true;
            runtime.halt();
        } catch (WorkflowFailedException e) {
            if (store.find(workflowId).map(w -> w.status() != WorkflowStatus.FAILED).orElse(true)) {
                store.fail(workflowId, e.getMessage());
            }
            wakeParents(workflowId);
        } catch (RuntimeException e) {
            store.fail(workflowId, e.toString());
            wakeParents(workflowId);
            throw e;
        }
    }

    private void wakeParents(String childId) {
        for (String parent : store.waitingOnChild(childId)) {
            store.markRunning(parent);
            runtime.kick();
            wakePublisher.wake(parent);
        }
    }
}
