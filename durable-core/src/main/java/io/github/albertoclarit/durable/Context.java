package io.github.albertoclarit.durable;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Replay-safe workflow context. Use ordinary Java control flow; the engine persists
 * operation results and skips completed work on resume.
 */
public interface Context {

    <R> R step(String name, Callable<R> body);

    <R> R step(String name, RetryPolicy retry, Callable<R> body);

    <R> R activity(String name, Callable<R> body);

    <R> R activity(String name, RetryPolicy retry, Callable<R> body);

    <R> R activity(String name, ActivityOptions options, Callable<R> body);

    <R> R activity(String name, ActivityOptions options, Function<ActivityContext, R> body);

    /**
     * Durable timer identified by call order. Use {@link #sleep(String, Duration)} inside loops.
     */
    void sleep(Duration duration);

    void sleep(String name, Duration duration);

    <R> R await(String name);

    <R> R await(String name, Class<R> type);

    <R> ChildWorkflow<R> startChild(String name, DurableJob<R> job);

    Instant now();

    UUID newId();

    double random();
}
