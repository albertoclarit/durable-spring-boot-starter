package io.github.albertoclarit.durable;

/**
 * A long-running workflow that survives JVM, pod, and worker failure.
 * {@code run} may be invoked again during replay; completed operations return persisted results.
 */
@FunctionalInterface
public interface DurableJob<T> {

    T run(Context ctx);
}
