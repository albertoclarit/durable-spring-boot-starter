package io.github.albertoclarit.durable;

import java.time.Duration;
import java.util.Objects;

/**
 * Explicit retry policy. When omitted, operations fail fast (one attempt).
 * Policy and retry progress are persisted so a crash during backoff resumes correctly.
 */
public final class RetryPolicy {

    private static final RetryPolicy NONE = new RetryPolicy(1, Duration.ZERO);

    private final int maxAttempts;
    private final Duration initialDelay;

    private RetryPolicy(int maxAttempts, Duration initialDelay) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        this.maxAttempts = maxAttempts;
        this.initialDelay = Objects.requireNonNull(initialDelay, "initialDelay");
    }

    public static RetryPolicy none() {
        return NONE;
    }

    public static RetryPolicy exponentialBackoff(int maxAttempts, Duration initialDelay) {
        if (initialDelay.isNegative()) {
            throw new IllegalArgumentException("initialDelay must be >= 0");
        }
        return new RetryPolicy(maxAttempts, initialDelay);
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public Duration initialDelay() {
        return initialDelay;
    }

    public boolean enabled() {
        return maxAttempts > 1;
    }

    public Duration delayAfterFailure(int attempt) {
        if (attempt < 1) {
            return initialDelay;
        }
        int shift = Math.min(attempt - 1, 20);
        return initialDelay.multipliedBy(1L << shift);
    }
}
