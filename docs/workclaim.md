# Complementing WorkClaim

WorkClaim (`work-queue-spring-boot-starter`) is an at-least-once **job queue**. This library is a **multi-step durable workflow** with replay.

| WorkClaim | Durable workflow |
|-----------|------------------|
| One protobuf message, one `@WorkConsumer` | Many named operations, one `DurableJob` |
| New `JobId` every `enqueue` | Workflow ID is the idempotency key |
| Default retries on handler errors | Fail-fast unless `RetryPolicy` is set |
| Lease + reclaim a single job | History + replay of a logical workflow |
| Cluster cron (`@WorkClaimCron`) | Durable `sleep` / `await` |

Do **not** store workflow history in `work_queue`. Do **not** reimplement `SKIP LOCKED` claiming as the developer API.

## Retry ownership

Operation failures inside `step` / `activity` follow **this** engine’s fail-fast / `RetryPolicy`. If you enqueue a WorkClaim “resume ticket”, WorkClaim retries apply only to **delivering the wake**, not to workflow operations.

## Optional wake tickets

Implement `WorkflowWakePublisher` to `WorkQueue.enqueue` a small resume message (`workflowId` only). Consumers should call into the durable runtime (or rely on the durable poller). Duplicate wakes are safe: claim is idempotent per lease.

Use `@WorkClaimCron` to **start** batches of workflows (`durable.start(...)`), not to replace `ctx.sleep`.
