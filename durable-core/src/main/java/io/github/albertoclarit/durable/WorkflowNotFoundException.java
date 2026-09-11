package io.github.albertoclarit.durable;

public class WorkflowNotFoundException extends RuntimeException {

    public WorkflowNotFoundException(String workflowId) {
        super("Workflow '" + workflowId + "' does not exist");
    }
}
