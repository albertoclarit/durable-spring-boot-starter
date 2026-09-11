package io.github.albertoclarit.durable;

/**
 * Optional hook to wake a worker when a workflow becomes runnable (timer, signal, retry).
 * The in-process poller is always the source of truth; this does not replace it.
 * Applications may enqueue a WorkClaim resume ticket here.
 */
@FunctionalInterface
public interface WorkflowWakePublisher {

    void wake(String workflowId);
}
