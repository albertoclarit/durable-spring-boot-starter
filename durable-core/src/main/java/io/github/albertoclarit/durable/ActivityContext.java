package io.github.albertoclarit.durable;

/**
 * Passed into activity bodies when the {@link java.util.function.Function} overload is used
 * so the same key can be forwarded to an external API.
 */
public final class ActivityContext {

    private final String idempotencyKey;

    public ActivityContext(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String idempotencyKey() {
        return idempotencyKey;
    }
}
