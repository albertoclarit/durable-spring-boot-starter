package io.github.albertoclarit.durable.internal;

/**
 * Control-flow signal: stop this attempt and release the worker.
 */
final class WorkflowSuspended extends RuntimeException {

    WorkflowSuspended() {
        super("workflow suspended", null, false, false);
    }
}
