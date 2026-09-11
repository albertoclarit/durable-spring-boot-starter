package io.github.albertoclarit.durable.internal;

import io.github.albertoclarit.durable.RetryPolicy;
import io.github.albertoclarit.durable.WorkflowStatus;
import io.github.albertoclarit.durable.internal.JsonCodec.TypedValue;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public final class JdbcWorkflowStore implements WorkflowStore {

    private final JdbcTemplate jdbc;
    private final Clock clock;
    private final TransactionTemplate tx;

    public JdbcWorkflowStore(DataSource dataSource, Clock clock) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .table("durable_flyway_schema_history")
                .load()
                .migrate();
        this.jdbc = new JdbcTemplate(dataSource);
        this.clock = clock;
        this.tx = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
    }

    @Override
    public CreateResult createOrGet(
            String id,
            String jobType,
            String jobJson,
            String fingerprint,
            String parentId,
            String childName
    ) {
        Instant now = clock.instant();
        int inserted = jdbc.update(
                """
                INSERT INTO durable_workflow (
                    id, job_type, job_json, fingerprint, parent_id, child_name, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'RUNNING', ?, ?)
                ON CONFLICT (id) DO NOTHING
                """,
                id, jobType, jobJson, fingerprint, parentId, childName, ts(now), ts(now)
        );
        WorkflowRecord record = find(id).orElseThrow();
        return new CreateResult(record, inserted == 1);
    }

    @Override
    public Optional<WorkflowRecord> find(String id) {
        List<WorkflowRecord> rows = jdbc.query(
                "SELECT * FROM durable_workflow WHERE id = ?",
                workflowMapper(),
                id
        );
        return rows.stream().findFirst();
    }

    @Override
    public Optional<String> claimNext(String workerId, Duration lease, Instant now) {
        return tx.execute(status -> {
        List<String> ids = jdbc.query(
                """
                WITH cte AS (
                    SELECT w.id
                    FROM durable_workflow w
                    WHERE (
                        (w.status = 'RUNNING' AND (w.lease_until IS NULL OR w.lease_until < ?))
                        OR (
                            w.status = 'WAITING'
                            AND w.waiting_signal IS NULL
                            AND (w.lease_until IS NULL OR w.lease_until < ?)
                            AND (
                                (w.waiting_child IS NULL AND (w.wake_at IS NULL OR w.wake_at <= ?))
                                OR (
                                    w.waiting_child IS NOT NULL
                                    AND EXISTS (
                                        SELECT 1 FROM durable_workflow c
                                        WHERE c.id = w.waiting_child AND c.status IN ('COMPLETED', 'FAILED')
                                    )
                                )
                            )
                        )
                    )
                    ORDER BY w.updated_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                UPDATE durable_workflow w SET
                    status = 'RUNNING',
                    worker_id = ?,
                    lease_until = ?,
                    waiting_signal = NULL,
                    waiting_child = NULL,
                    wake_at = NULL,
                    updated_at = ?
                FROM cte
                WHERE w.id = cte.id
                RETURNING w.id
                """,
                (rs, i) -> rs.getString("id"),
                ts(now), ts(now), ts(now),
                workerId, ts(now.plus(lease)), ts(now)
        );
        return ids.stream().findFirst();
        });
    }

    @Override
    public boolean tryClaim(String id, String workerId, Duration lease, Instant now) {
        int updated = jdbc.update(
                """
                UPDATE durable_workflow SET
                    status = 'RUNNING',
                    worker_id = ?,
                    lease_until = ?,
                    waiting_signal = NULL,
                    waiting_child = NULL,
                    wake_at = NULL,
                    updated_at = ?
                WHERE id = ?
                  AND status NOT IN ('COMPLETED', 'FAILED')
                  AND (lease_until IS NULL OR lease_until < ?)
                """,
                workerId, ts(now.plus(lease)), ts(now), id, ts(now)
        );
        return updated == 1;
    }

    @Override
    public void releaseLease(String id, String workerId) {
        jdbc.update(
                """
                UPDATE durable_workflow
                SET worker_id = NULL, lease_until = NULL, updated_at = ?
                WHERE id = ? AND worker_id = ?
                """,
                ts(clock.instant()), id, workerId
        );
    }

    @Override
    public void releaseWorker(String workerId) {
        jdbc.update(
                """
                UPDATE durable_workflow
                SET worker_id = NULL, lease_until = NULL, updated_at = ?
                WHERE worker_id = ?
                """,
                ts(clock.instant()), workerId
        );
    }

    @Override
    public void markWaiting(String id, Instant wakeAt, String waitingSignal, String waitingChild) {
        Instant now = clock.instant();
        jdbc.update(
                """
                UPDATE durable_workflow SET
                    status = 'WAITING',
                    wake_at = ?,
                    waiting_signal = ?,
                    waiting_child = ?,
                    worker_id = NULL,
                    lease_until = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                ts(wakeAt), waitingSignal, waitingChild, ts(now), id
        );
    }

    @Override
    public void markRunning(String id) {
        jdbc.update(
                """
                UPDATE durable_workflow SET
                    status = 'RUNNING',
                    waiting_signal = NULL,
                    waiting_child = NULL,
                    wake_at = NULL,
                    updated_at = ?
                WHERE id = ? AND status NOT IN ('COMPLETED', 'FAILED')
                """,
                ts(clock.instant()), id
        );
    }

    @Override
    public void complete(String id, TypedValue result) {
        Instant now = clock.instant();
        jdbc.update(
                """
                UPDATE durable_workflow SET
                    status = 'COMPLETED',
                    result_type = ?,
                    result_json = ?,
                    error_message = NULL,
                    worker_id = NULL,
                    lease_until = NULL,
                    waiting_signal = NULL,
                    waiting_child = NULL,
                    wake_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                result == null ? null : result.type(),
                result == null ? null : result.json(),
                ts(now),
                id
        );
    }

    @Override
    public void fail(String id, String error) {
        jdbc.update(
                """
                UPDATE durable_workflow SET
                    status = 'FAILED',
                    error_message = ?,
                    worker_id = NULL,
                    lease_until = NULL,
                    waiting_signal = NULL,
                    waiting_child = NULL,
                    wake_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                error, ts(clock.instant()), id
        );
    }

    @Override
    public void saveOperation(String workflowId, OperationRecord operation) {
        RetryPolicy retry = operation.retry();
        jdbc.update(
                """
                INSERT INTO durable_operation (
                    workflow_id, name, kind, status, result_type, result_json, error_message,
                    max_attempts, initial_delay_ms, attempts, next_retry_at, wake_at,
                    idempotency_key, child_workflow_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (workflow_id, name) DO UPDATE SET
                    kind = EXCLUDED.kind,
                    status = EXCLUDED.status,
                    result_type = EXCLUDED.result_type,
                    result_json = EXCLUDED.result_json,
                    error_message = EXCLUDED.error_message,
                    max_attempts = EXCLUDED.max_attempts,
                    initial_delay_ms = EXCLUDED.initial_delay_ms,
                    attempts = EXCLUDED.attempts,
                    next_retry_at = EXCLUDED.next_retry_at,
                    wake_at = EXCLUDED.wake_at,
                    idempotency_key = EXCLUDED.idempotency_key,
                    child_workflow_id = EXCLUDED.child_workflow_id
                """,
                workflowId,
                operation.name(),
                operation.kind().name(),
                operation.status() == null ? null : operation.status().name(),
                operation.result() == null ? null : operation.result().type(),
                operation.result() == null ? null : operation.result().json(),
                operation.error(),
                retry == null ? null : retry.maxAttempts(),
                retry == null ? null : retry.initialDelay().toMillis(),
                operation.attempts(),
                ts(operation.nextRetryAt()),
                ts(operation.wakeAt()),
                operation.idempotencyKey(),
                operation.childWorkflowId()
        );
    }

    @Override
    public Optional<OperationRecord> findOperation(String workflowId, String name) {
        List<OperationRecord> rows = jdbc.query(
                "SELECT * FROM durable_operation WHERE workflow_id = ? AND name = ?",
                operationMapper(),
                workflowId,
                name
        );
        return rows.stream().findFirst();
    }

    @Override
    public void putSignal(String workflowId, String name, TypedValue payload) {
        jdbc.update(
                """
                INSERT INTO durable_signal (workflow_id, name, payload_type, payload_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (workflow_id, name) DO NOTHING
                """,
                workflowId,
                name,
                payload.type(),
                payload.json(),
                ts(clock.instant())
        );
    }

    @Override
    public Optional<TypedValue> findSignal(String workflowId, String name) {
        List<TypedValue> rows = jdbc.query(
                "SELECT payload_type, payload_json FROM durable_signal WHERE workflow_id = ? AND name = ?",
                (rs, i) -> new TypedValue(rs.getString("payload_type"), rs.getString("payload_json")),
                workflowId,
                name
        );
        return rows.stream().findFirst();
    }

    @Override
    public List<String> waitingOnChild(String childId) {
        return jdbc.query(
                "SELECT id FROM durable_workflow WHERE waiting_child = ?",
                (rs, i) -> rs.getString("id"),
                childId
        );
    }

    private RowMapper<WorkflowRecord> workflowMapper() {
        return (rs, i) -> {
            WorkflowRecord record = new WorkflowRecord(
                    rs.getString("id"),
                    rs.getString("job_type"),
                    rs.getString("job_json"),
                    rs.getString("fingerprint"),
                    rs.getString("parent_id"),
                    rs.getString("child_name"),
                    WorkflowStatus.valueOf(rs.getString("status"))
            );
            String resultType = rs.getString("result_type");
            if (resultType != null) {
                record.setResult(new TypedValue(resultType, rs.getString("result_json")));
            }
            record.setError(rs.getString("error_message"));
            record.setWakeAt(instant(rs, "wake_at"));
            record.setWaitingSignal(rs.getString("waiting_signal"));
            record.setWaitingChild(rs.getString("waiting_child"));
            record.setLeaseUntil(instant(rs, "lease_until"));
            record.setWorkerId(rs.getString("worker_id"));
            return record;
        };
    }

    private RowMapper<OperationRecord> operationMapper() {
        return (rs, i) -> {
            OperationRecord op = new OperationRecord(
                    rs.getString("name"),
                    OperationKind.valueOf(rs.getString("kind"))
            );
            String status = rs.getString("status");
            if (status != null) {
                op.setStatus(OperationStatus.valueOf(status));
            }
            String resultType = rs.getString("result_type");
            if (resultType != null) {
                op.setResult(new TypedValue(resultType, rs.getString("result_json")));
            }
            op.setError(rs.getString("error_message"));
            Integer maxAttempts = (Integer) rs.getObject("max_attempts");
            Long delay = (Long) rs.getObject("initial_delay_ms");
            if (maxAttempts != null) {
                Duration initial = Duration.ofMillis(delay == null ? 0 : delay);
                op.setRetry(maxAttempts <= 1 ? RetryPolicy.none() : RetryPolicy.exponentialBackoff(maxAttempts, initial));
            }
            op.setAttempts(rs.getInt("attempts"));
            op.setNextRetryAt(instant(rs, "next_retry_at"));
            op.setWakeAt(instant(rs, "wake_at"));
            op.setIdempotencyKey(rs.getString("idempotency_key"));
            op.setChildWorkflowId(rs.getString("child_workflow_id"));
            return op;
        };
    }

    private static Timestamp ts(Instant instant) {
        return instant == null ? null : Timestamp.from(instant);
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp timestamp = rs.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }
}
