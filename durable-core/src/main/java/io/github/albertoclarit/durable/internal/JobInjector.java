package io.github.albertoclarit.durable.internal;

public interface JobInjector {

    void inject(Object job);
}
