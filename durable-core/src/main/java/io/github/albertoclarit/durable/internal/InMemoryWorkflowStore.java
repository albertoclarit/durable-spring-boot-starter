package io.github.albertoclarit.durable.internal;

import io.github.albertoclarit.durable.WorkflowStatus;
import io.github.albertoclarit.durable.internal.JsonCodec.TypedValue;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryWorkflowStore implements WorkflowStore {

    private final Clock clock;
    private final ConcurrentHashMap<String, Held> workflows = new ConcurrentHashMap<>();

    public InMemoryWorkflowStore(Clock clock) {
        this.clock = clock;
    }

    @Override
    public CreateResult createOrGet(
            String id,
            String jobType,
            String jobJson,
            String fingerprint,
            String parentId,
            String childName
    ) {
        Held created = new Held(new WorkflowRecord(id, jobType, jobJson, fingerprint, parentId, childName, WorkflowStatus.RUNNING));
        Held existing = workflows.putIfAbsent(id, created);
        if (existing == null) {
            return new CreateResult(created.record.copy(), true);
        }
        synchronized (existing) {
            return new CreateResult(existing.record.copy(), false);
        }
    }

    @Override
    public Optional<WorkflowRecord> find(String id) {
        Held held = workflows.get(id);
        if (held == null) {
            return Optional.empty();
        }
        synchronized (held) {
            return Optional.of(held.record.copy());
        }
    }

    @Override
    public Optional<String> claimNext(String workerId, Duration lease, Instant now) {
        for (Held held : workflows.values()) {
            if (tryClaimHeld(held, workerId, lease, now)) {
                return Optional.of(held.record.id());
            }
        }
        return Optional.empty();
    }

    @Override
    public boolean tryClaim(String id, String workerId, Duration lease, Instant now) {
        Held held = workflows.get(id);
        if (held == null) {
            return false;
        }
        return tryClaimHeld(held, workerId, lease, now);
    }

    private boolean tryClaimHeld(Held held, String workerId, Duration lease, Instant now) {
        synchronized (held) {
            WorkflowRecord r = held.record;
            if (r.status() == WorkflowStatus.COMPLETED || r.status() == WorkflowStatus.FAILED) {
                return false;
            }
            if (r.leaseUntil() != null && r.leaseUntil().isAfter(now)) {
                return false;
            }
            if (r.status() == WorkflowStatus.WAITING) {
                if (r.waitingSignal() != null) {
                    return false;
                }
                if (r.waitingChild() != null) {
                    Held child = workflows.get(r.waitingChild());
                    if (child == null) {
                        return false;
                    }
                    synchronized (child) {
                        WorkflowStatus cs = child.record.status();
                        if (cs != WorkflowStatus.COMPLETED && cs != WorkflowStatus.FAILED) {
                            return false;
                        }
                    }
                }
                if (r.wakeAt() != null && now.isBefore(r.wakeAt())) {
                    return false;
                }
            }
            r.setStatus(WorkflowStatus.RUNNING);
            r.setWorkerId(workerId);
            r.setLeaseUntil(now.plus(lease));
            r.setWaitingSignal(null);
            r.setWaitingChild(null);
            r.setWakeAt(null);
            return true;
        }
    }

    @Override
    public void releaseLease(String id, String workerId) {
        Held held = workflows.get(id);
        if (held == null) {
            return;
        }
        synchronized (held) {
            if (workerId.equals(held.record.workerId())) {
                held.record.setWorkerId(null);
                held.record.setLeaseUntil(null);
            }
        }
    }

    @Override
    public void releaseWorker(String workerId) {
        for (Held held : workflows.values()) {
            synchronized (held) {
                if (workerId.equals(held.record.workerId())) {
                    held.record.setWorkerId(null);
                    held.record.setLeaseUntil(null);
                }
            }
        }
    }

    @Override
    public void markWaiting(String id, Instant wakeAt, String waitingSignal, String waitingChild) {
        Held held = require(id);
        synchronized (held) {
            held.record.setStatus(WorkflowStatus.WAITING);
            held.record.setWakeAt(wakeAt);
            held.record.setWaitingSignal(waitingSignal);
            held.record.setWaitingChild(waitingChild);
            held.record.setWorkerId(null);
            held.record.setLeaseUntil(null);
        }
    }

    @Override
    public void markRunning(String id) {
        Held held = require(id);
        synchronized (held) {
            if (held.record.status() == WorkflowStatus.COMPLETED || held.record.status() == WorkflowStatus.FAILED) {
                return;
            }
            held.record.setStatus(WorkflowStatus.RUNNING);
            held.record.setWaitingSignal(null);
            held.record.setWaitingChild(null);
            held.record.setWakeAt(null);
        }
    }

    @Override
    public void complete(String id, TypedValue result) {
        Held held = require(id);
        synchronized (held) {
            held.record.setStatus(WorkflowStatus.COMPLETED);
            held.record.setResult(result);
            held.record.setError(null);
            held.record.setWorkerId(null);
            held.record.setLeaseUntil(null);
            held.record.setWaitingSignal(null);
            held.record.setWaitingChild(null);
            held.record.setWakeAt(null);
        }
    }

    @Override
    public void fail(String id, String error) {
        Held held = require(id);
        synchronized (held) {
            held.record.setStatus(WorkflowStatus.FAILED);
            held.record.setError(error);
            held.record.setWorkerId(null);
            held.record.setLeaseUntil(null);
            held.record.setWaitingSignal(null);
            held.record.setWaitingChild(null);
            held.record.setWakeAt(null);
        }
    }

    @Override
    public void saveOperation(String workflowId, OperationRecord operation) {
        Held held = require(workflowId);
        synchronized (held) {
            held.operations.put(operation.name(), copyOp(operation));
        }
    }

    @Override
    public Optional<OperationRecord> findOperation(String workflowId, String name) {
        Held held = workflows.get(workflowId);
        if (held == null) {
            return Optional.empty();
        }
        synchronized (held) {
            OperationRecord op = held.operations.get(name);
            return op == null ? Optional.empty() : Optional.of(copyOp(op));
        }
    }

    @Override
    public void putSignal(String workflowId, String name, TypedValue payload) {
        Held held = require(workflowId);
        synchronized (held) {
            held.signals.putIfAbsent(name, payload);
        }
    }

    @Override
    public Optional<TypedValue> findSignal(String workflowId, String name) {
        Held held = workflows.get(workflowId);
        if (held == null) {
            return Optional.empty();
        }
        synchronized (held) {
            return Optional.ofNullable(held.signals.get(name));
        }
    }

    @Override
    public List<String> waitingOnChild(String childId) {
        List<String> ids = new ArrayList<>();
        for (Held held : workflows.values()) {
            synchronized (held) {
                if (childId.equals(held.record.waitingChild())) {
                    ids.add(held.record.id());
                }
            }
        }
        return ids;
    }

    Instant now() {
        return clock.instant();
    }

    private Held require(String id) {
        Held held = workflows.get(id);
        if (held == null) {
            throw new IllegalStateException("Unknown workflow " + id);
        }
        return held;
    }

    private static OperationRecord copyOp(OperationRecord op) {
        OperationRecord c = new OperationRecord(op.name(), op.kind());
        c.setStatus(op.status());
        c.setResult(op.result());
        c.setError(op.error());
        c.setRetry(op.retry());
        c.setAttempts(op.attempts());
        c.setNextRetryAt(op.nextRetryAt());
        c.setWakeAt(op.wakeAt());
        c.setIdempotencyKey(op.idempotencyKey());
        c.setChildWorkflowId(op.childWorkflowId());
        return c;
    }

    private static final class Held {
        private final WorkflowRecord record;
        private final Map<String, OperationRecord> operations = new ConcurrentHashMap<>();
        private final Map<String, TypedValue> signals = new ConcurrentHashMap<>();

        Held(WorkflowRecord record) {
            this.record = record;
        }
    }
}
