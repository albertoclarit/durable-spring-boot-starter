package io.github.albertoclarit.durable.internal;

import io.github.albertoclarit.durable.WorkflowFailedException;
import io.github.albertoclarit.durable.WorkflowHandle;
import io.github.albertoclarit.durable.WorkflowStatus;
import io.github.albertoclarit.durable.WorkflowTimeoutException;
import io.github.albertoclarit.durable.internal.WorkflowStore.WorkflowRecord;

import java.time.Duration;
import java.util.Optional;

final class DefaultWorkflowHandle<T> implements WorkflowHandle<T> {

    private final String id;
    private final WorkflowStore store;
    private final JsonCodec codec;
    private final WorkflowRuntime runtime;

    DefaultWorkflowHandle(String id, WorkflowStore store, JsonCodec codec, WorkflowRuntime runtime) {
        this.id = id;
        this.store = store;
        this.codec = codec;
        this.runtime = runtime;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public WorkflowStatus status() {
        return record().status();
    }

    @Override
    public T result() {
        return result(Duration.ofDays(3650));
    }

    @Override
    public T result(Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Optional<T> value = tryResult();
            if (value.isPresent()) {
                return value.get();
            }
            WorkflowRecord record = record();
            if (record.status() == WorkflowStatus.FAILED) {
                throw new WorkflowFailedException(id, record.error());
            }
            if (System.nanoTime() >= deadline) {
                throw new WorkflowTimeoutException(id, timeout);
            }
            runtime.awaitTick(Duration.ofMillis(25));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Optional<T> tryResult() {
        WorkflowRecord record = record();
        if (record.status() == WorkflowStatus.COMPLETED) {
            return Optional.ofNullable((T) codec.unwrap(record.result()));
        }
        return Optional.empty();
    }

    private WorkflowRecord record() {
        return store.find(id).orElseThrow();
    }
}
