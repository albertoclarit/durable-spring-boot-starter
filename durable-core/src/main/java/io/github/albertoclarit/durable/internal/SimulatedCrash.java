package io.github.albertoclarit.durable.internal;

final class SimulatedCrash extends RuntimeException {

    SimulatedCrash() {
        super("simulated worker crash", null, false, false);
    }
}
