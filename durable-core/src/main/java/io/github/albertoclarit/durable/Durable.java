package io.github.albertoclarit.durable;

import java.util.Optional;

/**
 * Entry point for durable workflows. {@link #start(String, DurableJob)} is an atomic
 * get-or-create: the workflow ID is the idempotency key.
 */
public interface Durable {

    /**
     * Starts a workflow or returns the existing handle for {@code workflowId}.
     * Does not wait for completion.
     */
    <T> WorkflowHandle<T> start(String workflowId, DurableJob<T> job);

    /**
     * Delivers an external event to a waiting (or not-yet-waiting) workflow.
     * Signals are persisted; {@link Context#await(String)} observes them later if they arrive first.
     */
    void signal(String workflowId, String signalName, Object payload);

    Optional<WorkflowHandle<?>> find(String workflowId);
}
