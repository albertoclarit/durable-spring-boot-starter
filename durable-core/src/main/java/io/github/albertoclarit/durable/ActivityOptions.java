package io.github.albertoclarit.durable;

import java.util.Objects;

/**
 * Activity options. {@link #idempotencyKey(String)} is recorded for the external system;
 * this engine does not claim exactly-once I/O.
 */
public final class ActivityOptions {

    private final String idempotencyKey;
    private final RetryPolicy retry;

    private ActivityOptions(String idempotencyKey, RetryPolicy retry) {
        this.idempotencyKey = idempotencyKey;
        this.retry = retry == null ? RetryPolicy.none() : retry;
    }

    public static ActivityOptions idempotencyKey(String key) {
        return new ActivityOptions(Objects.requireNonNull(key, "idempotencyKey"), RetryPolicy.none());
    }

    public ActivityOptions retry(RetryPolicy policy) {
        return new ActivityOptions(idempotencyKey, Objects.requireNonNull(policy, "retry"));
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }

    public RetryPolicy retryPolicy() {
        return retry;
    }
}
