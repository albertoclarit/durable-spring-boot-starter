package io.github.albertoclarit.durable.config;

import io.github.albertoclarit.durable.internal.WorkflowRuntime;
import org.springframework.context.SmartLifecycle;

public final class DurableRuntimeLifecycle implements SmartLifecycle {

    private final WorkflowRuntime runtime;
    private volatile boolean running;

    public DurableRuntimeLifecycle(WorkflowRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public void start() {
        runtime.start();
        running = true;
    }

    @Override
    public void stop() {
        runtime.close();
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return 0;
    }
}
