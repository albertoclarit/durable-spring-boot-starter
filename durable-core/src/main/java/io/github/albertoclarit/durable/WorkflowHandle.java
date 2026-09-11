package io.github.albertoclarit.durable;

import java.time.Duration;
import java.util.Optional;

public interface WorkflowHandle<T> {

    String id();

    WorkflowStatus status();

    /**
     * Waits until the workflow is completed or failed. Failed workflows throw
     * {@link io.github.albertoclarit.durable.WorkflowFailedException}.
     */
    T result();

    T result(Duration timeout);

    Optional<T> tryResult();
}
