CREATE TABLE durable_workflow (
    id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    job_json TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    parent_id TEXT,
    child_name TEXT,
    status TEXT NOT NULL,
    result_type TEXT,
    result_json TEXT,
    error_message TEXT,
    wake_at TIMESTAMPTZ,
    waiting_signal TEXT,
    waiting_child TEXT,
    lease_until TIMESTAMPTZ,
    worker_id TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX durable_workflow_claim_idx ON durable_workflow (status, wake_at, lease_until);

CREATE TABLE durable_operation (
    workflow_id TEXT NOT NULL REFERENCES durable_workflow (id),
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT,
    result_type TEXT,
    result_json TEXT,
    error_message TEXT,
    max_attempts INTEGER,
    initial_delay_ms BIGINT,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    wake_at TIMESTAMPTZ,
    idempotency_key TEXT,
    child_workflow_id TEXT,
    PRIMARY KEY (workflow_id, name)
);

CREATE TABLE durable_signal (
    workflow_id TEXT NOT NULL REFERENCES durable_workflow (id),
    name TEXT NOT NULL,
    payload_type TEXT,
    payload_json TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (workflow_id, name)
);
