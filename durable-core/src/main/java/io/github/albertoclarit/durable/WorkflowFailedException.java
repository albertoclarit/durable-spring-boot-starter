package io.github.albertoclarit.durable;

public class WorkflowFailedException extends RuntimeException {

    private final String workflowId;

    public WorkflowFailedException(String workflowId, String message) {
        super("Workflow '" + workflowId + "' failed: " + message);
        this.workflowId = workflowId;
    }

    public String workflowId() {
        return workflowId;
    }
}
