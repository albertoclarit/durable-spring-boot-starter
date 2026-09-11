package io.github.albertoclarit.durable.internal;

import io.github.albertoclarit.durable.RetryPolicy;
import io.github.albertoclarit.durable.WorkflowStatus;
import io.github.albertoclarit.durable.internal.JsonCodec.TypedValue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface WorkflowStore {

    CreateResult createOrGet(
            String id,
            String jobType,
            String jobJson,
            String fingerprint,
            String parentId,
            String childName
    );

    Optional<WorkflowRecord> find(String id);

    Optional<String> claimNext(String workerId, Duration lease, Instant now);

    boolean tryClaim(String id, String workerId, Duration lease, Instant now);

    void releaseLease(String id, String workerId);

    void releaseWorker(String workerId);

    void markWaiting(
            String id,
            Instant wakeAt,
            String waitingSignal,
            String waitingChild
    );

    void markRunning(String id);

    void complete(String id, TypedValue result);

    void fail(String id, String error);

    void saveOperation(String workflowId, OperationRecord operation);

    Optional<OperationRecord> findOperation(String workflowId, String name);

    void putSignal(String workflowId, String name, TypedValue payload);

    Optional<TypedValue> findSignal(String workflowId, String name);

    List<String> waitingOnChild(String childId);

    record CreateResult(WorkflowRecord record, boolean created) {
    }

    final class WorkflowRecord {
        private final String id;
        private final String jobType;
        private final String jobJson;
        private final String fingerprint;
        private final String parentId;
        private final String childName;
        private WorkflowStatus status;
        private TypedValue result;
        private String error;
        private Instant wakeAt;
        private String waitingSignal;
        private String waitingChild;
        private Instant leaseUntil;
        private String workerId;

        WorkflowRecord(
                String id,
                String jobType,
                String jobJson,
                String fingerprint,
                String parentId,
                String childName,
                WorkflowStatus status
        ) {
            this.id = id;
            this.jobType = jobType;
            this.jobJson = jobJson;
            this.fingerprint = fingerprint;
            this.parentId = parentId;
            this.childName = childName;
            this.status = status;
        }

        public String id() {
            return id;
        }

        public String jobType() {
            return jobType;
        }

        public String jobJson() {
            return jobJson;
        }

        public String fingerprint() {
            return fingerprint;
        }

        public String parentId() {
            return parentId;
        }

        public String childName() {
            return childName;
        }

        public WorkflowStatus status() {
            return status;
        }

        public void setStatus(WorkflowStatus status) {
            this.status = status;
        }

        public TypedValue result() {
            return result;
        }

        public void setResult(TypedValue result) {
            this.result = result;
        }

        public String error() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public Instant wakeAt() {
            return wakeAt;
        }

        public void setWakeAt(Instant wakeAt) {
            this.wakeAt = wakeAt;
        }

        public String waitingSignal() {
            return waitingSignal;
        }

        public void setWaitingSignal(String waitingSignal) {
            this.waitingSignal = waitingSignal;
        }

        public String waitingChild() {
            return waitingChild;
        }

        public void setWaitingChild(String waitingChild) {
            this.waitingChild = waitingChild;
        }

        public Instant leaseUntil() {
            return leaseUntil;
        }

        public void setLeaseUntil(Instant leaseUntil) {
            this.leaseUntil = leaseUntil;
        }

        public String workerId() {
            return workerId;
        }

        public void setWorkerId(String workerId) {
            this.workerId = workerId;
        }

        WorkflowRecord copy() {
            WorkflowRecord c = new WorkflowRecord(id, jobType, jobJson, fingerprint, parentId, childName, status);
            c.result = result;
            c.error = error;
            c.wakeAt = wakeAt;
            c.waitingSignal = waitingSignal;
            c.waitingChild = waitingChild;
            c.leaseUntil = leaseUntil;
            c.workerId = workerId;
            return c;
        }
    }

    final class OperationRecord {
        private final String name;
        private final OperationKind kind;
        private OperationStatus status;
        private TypedValue result;
        private String error;
        private RetryPolicy retry;
        private int attempts;
        private Instant nextRetryAt;
        private Instant wakeAt;
        private String idempotencyKey;
        private String childWorkflowId;

        OperationRecord(String name, OperationKind kind) {
            this.name = name;
            this.kind = kind;
        }

        public String name() {
            return name;
        }

        public OperationKind kind() {
            return kind;
        }

        public OperationStatus status() {
            return status;
        }

        public void setStatus(OperationStatus status) {
            this.status = status;
        }

        public TypedValue result() {
            return result;
        }

        public void setResult(TypedValue result) {
            this.result = result;
        }

        public String error() {
            return error;
        }

        public void setError(String error) {
            this.error = error;
        }

        public RetryPolicy retry() {
            return retry;
        }

        public void setRetry(RetryPolicy retry) {
            this.retry = retry;
        }

        public int attempts() {
            return attempts;
        }

        public void setAttempts(int attempts) {
            this.attempts = attempts;
        }

        public Instant nextRetryAt() {
            return nextRetryAt;
        }

        public void setNextRetryAt(Instant nextRetryAt) {
            this.nextRetryAt = nextRetryAt;
        }

        public Instant wakeAt() {
            return wakeAt;
        }

        public void setWakeAt(Instant wakeAt) {
            this.wakeAt = wakeAt;
        }

        public String idempotencyKey() {
            return idempotencyKey;
        }

        public void setIdempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
        }

        public String childWorkflowId() {
            return childWorkflowId;
        }

        public void setChildWorkflowId(String childWorkflowId) {
            this.childWorkflowId = childWorkflowId;
        }
    }
}
