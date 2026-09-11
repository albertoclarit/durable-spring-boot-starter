package io.github.albertoclarit.durable.internal;

enum OperationKind {
    STEP,
    ACTIVITY,
    SLEEP,
    AWAIT,
    NOW,
    RANDOM,
    NEWID,
    CHILD
}
