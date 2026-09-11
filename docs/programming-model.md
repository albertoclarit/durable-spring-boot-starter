# Durable workflow programming model

The engine makes a long-running workflow feel like synchronous Java. `DurableJob.run` may be invoked again after a crash; **completed operations return persisted results and do not run their bodies again**.

Package: `io.github.albertoclarit.durable`.

## Mental model

```text
Workflow
   ├── step()       → deterministic computation
   ├── activity()   → external side effect
   ├── sleep()      → durable timer (no worker thread)
   ├── await()      → durable external event
   └── startChild() → nested DurableJob
```

Do not build a DAG. Use `if` / `for` / locals.

## Starting a workflow

```java
WorkflowHandle<SettlementResult> workflow =
    durable.start("settlement-" + settlementId, new SettlementJob(settlementId));
```

`start` is **non-blocking** and an atomic **get-or-create**. The workflow ID is the idempotency key (logical instance, not a worker or pod).

| Existing state | Same ID |
|----------------|---------|
| Missing | Create and start |
| Running / waiting | Return existing handle |
| Completed | Return existing handle |
| Failed | Return existing handle; do **not** start a replacement |
| Different job type or input | Reject (`IncompatibleWorkflowException`) |

Concurrent `start` calls with the same ID create **one** instance. This is not `if (!exists) create`.

## Jobs

```java
public interface DurableJob<T> {
    T run(Context ctx);
}
```

Serializable fields are **workflow input** (identity / fingerprint). Spring collaborators must be `transient` (or `@JsonIgnore`) and are re-injected on every execution:

```java
public class SettlementJob implements DurableJob<SettlementResult> {
    private final long settlementId;

    @Autowired
    private transient SettlementService settlementService;

    public SettlementJob(long settlementId) {
        this.settlementId = settlementId;
    }

    @Override
    public SettlementResult run(Context ctx) { ... }
}
```

## Step vs activity

| | `step` | `activity` |
|--|--------|------------|
| Meaning | Computation (rules, transforms) | Side effect (HTTP, payout, Kafka, DB mutation) |
| Replay | Return persisted value | Return persisted completion |
| External exactly-once | N/A | **Not claimed** |

They are not two names for the same thing. Keep I/O in `activity`.

Outputs are ordinary Java values:

```java
var customer = ctx.step("load-customer", () -> customerService.load(customerId));
var account = ctx.activity("create-account", () -> accountService.create(customer));
```

## Replay

If A and B completed and the JVM died before C:

```text
resume → A persisted, B persisted, C executes
```

`service.a()` / `service.b()` must not run again. The workflow method is invoked from the top.

## Fail-fast and retry

With no `RetryPolicy`, an exception **fails the operation and the workflow**. Replay of `FAILED` does not start retries.

```java
ctx.activity(
    "send-payout",
    RetryPolicy.exponentialBackoff(5, Duration.ofSeconds(10)),
    () -> bank.send()
);
```

Retry policy and attempt state are persisted. A crash during backoff resumes that policy.

## Activity idempotency

- Replay: a **completed** activity is not run again (workflow-side).
- Crash **during** an activity: the body may run more than once (at-least-once).
- `ActivityOptions.idempotencyKey(...)` is for the **external** system.

```java
ctx.activity(
    "send-payout",
    ActivityOptions.idempotencyKey("payout:" + payoutId),
    act -> bankClient.sendMoney(act.idempotencyKey(), ...)
);
```

## Operation identity

Logical key: **workflow ID + operation name**. Names must be stable across replay (never `UUID.randomUUID()` in the name).

```java
for (SettlementItem item : items) {
    ctx.step("settlement-item/" + item.id(), () -> calculate(item));
}
```

Duplicate names in one `run` fail. Unnamed `sleep(Duration)` uses call order (`$sleep:1`, …). **Loops must use** `sleep(name, duration)`.

## Sleep and signals

`ctx.sleep` is not `Thread.sleep`. The timer is persisted; the worker is released.

`run` is **eager Java**, not a lazily evaluated declaration. The engine does **not** serialize a paused stack. The first time execution hits `ctx.sleep(...)`, this **invocation of `run()` ends** (internal suspend). Statements after `sleep` in the source are **not** run in that call. Later, a worker **invokes `run()` from the top again**. Finished steps return persisted results; `sleep` returns; **then** the next statement runs.

```java
ctx.sleep(Duration.ofHours(24));
// This line does not run until a later run() after the timer fires:
calculation = ctx.step("recalculate", () -> settlementService.recalculate(settlement));
```

`await` is the same: this attempt stops at the wait; a later `run()` continues after it.

```java
var confirmation = ctx.await("bank-confirmation");
durable.signal(workflowId, "bank-confirmation", confirmation);
```

A signal may arrive before `await`; it is stored and consumed when the workflow reaches `await`. Unknown workflow IDs on `signal` fail.

## Determinism

Do not use `Instant.now()`, `UUID.randomUUID()`, or `Math.random()` for workflow decisions. Use `ctx.now()`, `ctx.newId()`, `ctx.random()`.

## Child workflows

```java
var payout = ctx.startChild("payout", new PayoutJob(validation));
return payout.result();
```

Child ID is `parentId + "/" + name`. Creation is get-or-create. `result()` waits durably (parent may suspend).

## Target usage

```java
public SettlementResult run(Context ctx) {
    var settlement = ctx.step("load-settlement", () -> settlementService.load(settlementId));
    var calculation = ctx.step("calculate", () -> settlementService.calculate(settlement));
    if (calculation.isNegative()) {
        ctx.sleep(Duration.ofHours(24));
        calculation = ctx.step("recalculate", () -> settlementService.recalculate(settlement));
    }
    var payout = ctx.activity(
        "payout",
        ActivityOptions.idempotencyKey("payout:" + settlementId),
        () -> payoutService.execute(calculation)
    );
    var confirmation = ctx.await("bank-confirmation");
    ctx.step("reconcile", () -> reconcile(payout, confirmation));
    return new SettlementResult(calculation, payout);
}
```
