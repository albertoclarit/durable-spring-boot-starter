package io.github.albertoclarit.durable.internal;

import io.github.albertoclarit.durable.ActivityContext;
import io.github.albertoclarit.durable.ActivityOptions;
import io.github.albertoclarit.durable.ChildWorkflow;
import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.DuplicateOperationException;
import io.github.albertoclarit.durable.DurableJob;
import io.github.albertoclarit.durable.RetryPolicy;
import io.github.albertoclarit.durable.WorkflowFailedException;
import io.github.albertoclarit.durable.WorkflowStatus;
import io.github.albertoclarit.durable.internal.JsonCodec.TypedValue;
import io.github.albertoclarit.durable.internal.WorkflowStore.OperationRecord;
import io.github.albertoclarit.durable.internal.WorkflowStore.WorkflowRecord;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

final class ReplayContext implements Context {

    private final String workflowId;
    private final WorkflowStore store;
    private final JsonCodec codec;
    private final Clock clock;
    private final DefaultDurable durable;
    private final AtomicInteger crashAfter;
    private final Set<String> seen = new HashSet<>();
    private int sleepSeq;
    private int nowSeq;
    private int idSeq;
    private int randomSeq;

    ReplayContext(
            String workflowId,
            WorkflowStore store,
            JsonCodec codec,
            Clock clock,
            DefaultDurable durable,
            AtomicInteger crashAfter
    ) {
        this.workflowId = workflowId;
        this.store = store;
        this.codec = codec;
        this.clock = clock;
        this.durable = durable;
        this.crashAfter = crashAfter;
    }

    @Override
    public <R> R step(String name, Callable<R> body) {
        return step(name, RetryPolicy.none(), body);
    }

    @Override
    public <R> R step(String name, RetryPolicy retry, Callable<R> body) {
        return execute(name, OperationKind.STEP, retry, null, body);
    }

    @Override
    public <R> R activity(String name, Callable<R> body) {
        return activity(name, RetryPolicy.none(), body);
    }

    @Override
    public <R> R activity(String name, RetryPolicy retry, Callable<R> body) {
        return execute(name, OperationKind.ACTIVITY, retry, null, body);
    }

    @Override
    public <R> R activity(String name, ActivityOptions options, Callable<R> body) {
        return execute(name, OperationKind.ACTIVITY, options.retryPolicy(), options.idempotencyKey(), body);
    }

    @Override
    public <R> R activity(String name, ActivityOptions options, Function<ActivityContext, R> body) {
        ActivityContext activityContext = new ActivityContext(options.idempotencyKey());
        return execute(name, OperationKind.ACTIVITY, options.retryPolicy(), options.idempotencyKey(),
                () -> body.apply(activityContext));
    }

    @Override
    public void sleep(Duration duration) {
        sleep("$sleep:" + (++sleepSeq), duration);
    }

    @Override
    public void sleep(String name, Duration duration) {
        requireName(name);
        OperationRecord existing = store.findOperation(workflowId, name).orElse(null);
        if (existing != null && existing.status() == OperationStatus.COMPLETED) {
            return;
        }
        Instant now = clock.instant();
        Instant wakeAt = existing != null && existing.wakeAt() != null
                ? existing.wakeAt()
                : now.plus(duration);
        if (!now.isBefore(wakeAt)) {
            OperationRecord done = existing == null ? new OperationRecord(name, OperationKind.SLEEP) : existing;
            done.setStatus(OperationStatus.COMPLETED);
            done.setWakeAt(wakeAt);
            store.saveOperation(workflowId, done);
            return;
        }
        OperationRecord op = existing == null ? new OperationRecord(name, OperationKind.SLEEP) : existing;
        op.setWakeAt(wakeAt);
        store.saveOperation(workflowId, op);
        suspend(wakeAt, null, null);
    }

    @Override
    public <R> R await(String name) {
        return await(name, null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <R> R await(String name, Class<R> type) {
        requireName(name);
        OperationRecord existing = store.findOperation(workflowId, name).orElse(null);
        if (existing != null && existing.status() == OperationStatus.COMPLETED) {
            return type == null ? (R) codec.unwrap(existing.result()) : codec.unwrap(existing.result(), type);
        }
        TypedValue signal = store.findSignal(workflowId, name).orElse(null);
        if (signal != null) {
            OperationRecord op = existing == null ? new OperationRecord(name, OperationKind.AWAIT) : existing;
            op.setStatus(OperationStatus.COMPLETED);
            op.setResult(signal);
            store.saveOperation(workflowId, op);
            return type == null ? (R) codec.unwrap(signal) : codec.unwrap(signal, type);
        }
        suspend(null, name, null);
        throw new IllegalStateException("unreachable");
    }

    @Override
    public <R> ChildWorkflow<R> startChild(String name, DurableJob<R> job) {
        requireName(name);
        String childId = workflowId + "/" + name;
        OperationRecord existing = store.findOperation(workflowId, name).orElse(null);
        if (existing == null) {
            durable.startInternal(childId, job, workflowId, name);
            OperationRecord op = new OperationRecord(name, OperationKind.CHILD);
            op.setStatus(OperationStatus.COMPLETED);
            op.setChildWorkflowId(childId);
            store.saveOperation(workflowId, op);
        } else {
            durable.startInternal(childId, job, workflowId, name);
        }
        return new ChildHandle<>(childId);
    }

    @Override
    public Instant now() {
        return execute("$now:" + (++nowSeq), OperationKind.NOW, RetryPolicy.none(), null, clock::instant);
    }

    @Override
    public UUID newId() {
        return execute("$id:" + (++idSeq), OperationKind.NEWID, RetryPolicy.none(), null, UUID::randomUUID);
    }

    @Override
    public double random() {
        return execute("$random:" + (++randomSeq), OperationKind.RANDOM, RetryPolicy.none(), null,
                ThreadLocalRandom.current()::nextDouble);
    }

    @SuppressWarnings("unchecked")
    private <R> R execute(
            String name,
            OperationKind kind,
            RetryPolicy retry,
            String idempotencyKey,
            Callable<R> body
    ) {
        requireName(name);
        RetryPolicy policy = retry == null ? RetryPolicy.none() : retry;
        OperationRecord existing = store.findOperation(workflowId, name).orElse(null);
        if (existing != null && existing.status() == OperationStatus.COMPLETED) {
            return (R) codec.unwrap(existing.result());
        }
        if (existing != null && existing.status() == OperationStatus.FAILED) {
            throw new IllegalStateException("Operation '" + name + "' previously failed: " + existing.error());
        }
        if (existing != null && existing.status() == OperationStatus.RETRY_SCHEDULED) {
            Instant due = existing.nextRetryAt();
            if (due != null && clock.instant().isBefore(due)) {
                suspend(due, null, null);
            }
        }
        noteExecution();
        try {
            R value = body.call();
            OperationRecord op = existing == null ? new OperationRecord(name, kind) : existing;
            op.setStatus(OperationStatus.COMPLETED);
            op.setResult(codec.wrap(value));
            op.setError(null);
            op.setRetry(policy);
            op.setIdempotencyKey(idempotencyKey);
            store.saveOperation(workflowId, op);
            if (crashAfter != null) {
                crashAfter.decrementAndGet();
            }
            return value;
        } catch (WorkflowSuspended | SimulatedCrash e) {
            throw e;
        } catch (Exception e) {
            OperationRecord op = existing == null ? new OperationRecord(name, kind) : existing;
            int attempts = op.attempts() + 1;
            op.setAttempts(attempts);
            op.setRetry(policy);
            op.setIdempotencyKey(idempotencyKey);
            op.setError(e.toString());
            if (attempts < policy.maxAttempts()) {
                Instant next = clock.instant().plus(policy.delayAfterFailure(attempts));
                op.setStatus(OperationStatus.RETRY_SCHEDULED);
                op.setNextRetryAt(next);
                store.saveOperation(workflowId, op);
                suspend(next, null, null);
            }
            op.setStatus(OperationStatus.FAILED);
            store.saveOperation(workflowId, op);
            store.fail(workflowId, e.toString());
            throw new WorkflowFailedException(workflowId, e.toString());
        }
    }

    private void requireName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Operation name is required");
        }
        if (!seen.add(name)) {
            throw new DuplicateOperationException(workflowId, name);
        }
    }

    private void noteExecution() {
        if (crashAfter != null && crashAfter.get() <= 0) {
            throw new SimulatedCrash();
        }
    }

    private void suspend(Instant wakeAt, String waitingSignal, String waitingChild) {
        store.markWaiting(workflowId, wakeAt, waitingSignal, waitingChild);
        throw new WorkflowSuspended();
    }

    private final class ChildHandle<R> implements ChildWorkflow<R> {
        private final String childId;

        private ChildHandle(String childId) {
            this.childId = childId;
        }

        @Override
        public String id() {
            return childId;
        }

        @Override
        @SuppressWarnings("unchecked")
        public R result() {
            WorkflowRecord child = store.find(childId).orElseThrow();
            if (child.status() == WorkflowStatus.COMPLETED) {
                return (R) codec.unwrap(child.result());
            }
            if (child.status() == WorkflowStatus.FAILED) {
                store.fail(workflowId, "Child '" + childId + "' failed: " + child.error());
                throw new WorkflowFailedException(workflowId, child.error());
            }
            suspend(null, null, childId);
            throw new IllegalStateException("unreachable");
        }
    }
}
