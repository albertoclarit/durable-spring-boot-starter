package io.github.albertoclarit.durable;

/**
 * Same workflow ID already exists with a different job type or input.
 */
public class IncompatibleWorkflowException extends RuntimeException {

    public IncompatibleWorkflowException(String workflowId) {
        super("Workflow '" + workflowId + "' already exists with an incompatible definition or input");
    }
}
