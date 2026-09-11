package io.github.albertoclarit.durable;

import java.time.Duration;

public class WorkflowTimeoutException extends RuntimeException {

    public WorkflowTimeoutException(String workflowId, Duration timeout) {
        super("Timed out after " + timeout + " waiting for workflow '" + workflowId + "'");
    }
}
