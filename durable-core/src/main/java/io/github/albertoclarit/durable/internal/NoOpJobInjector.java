package io.github.albertoclarit.durable.internal;

public final class NoOpJobInjector implements JobInjector {

    @Override
    public void inject(Object job) {
        // tests and non-Spring usage
    }
}
