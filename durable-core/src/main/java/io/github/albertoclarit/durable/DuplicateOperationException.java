package io.github.albertoclarit.durable;

public class DuplicateOperationException extends RuntimeException {

    public DuplicateOperationException(String workflowId, String name) {
        super("Operation '" + name + "' was invoked more than once in workflow '" + workflowId + "'");
    }
}
