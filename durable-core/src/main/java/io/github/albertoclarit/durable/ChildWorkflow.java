package io.github.albertoclarit.durable;

/**
 * Handle to a nested durable job. {@link #result()} waits durably (the parent may suspend).
 */
public interface ChildWorkflow<T> {

    String id();

    T result();
}
