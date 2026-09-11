# Durable Workflow

**Durable Workflow** helps you run a **multi-step business process that must finish even if the computer running it dies**.

Think of a settlement, a payout, or an account opening: load data, calculate, wait for a bank, send money, then reconcile. That can take seconds or **hours**. Servers restart. Pods move. Workers crash. Without a durable workflow, you either lose progress or accidentally **do the same side effect twice** (pay someone twice, send two emails).

With this library, you write the process as **ordinary, top-to-bottom Java**. The engine remembers what already finished. After a crash, another worker **picks up where you left off** instead of starting from scratch.

It is a **Spring Boot starter** (JDK 25). Applications depend on `durable-spring-boot-starter`. Group: `io.github.albertoclarit`. Package: `io.github.albertoclarit.durable`.

---

## In one sentence

You describe a long-running job as a Java method. The library makes that job **survive restarts**, **run only once per business id**, and **skip work that already completed**.

---

## Who this is for

- **Product / operations:** “If we crash overnight, will settlement-123 still complete, and will we pay twice?”
- **Developers:** You want `if` / `for` / local variables, not a flowchart tool or a hand-built DAG.
- **Platform teams:** You already have (or will have) WorkClaim for *one-shot background jobs*. This library is for *multi-step processes* that wait, sleep, and call the outside world.

If the work is “process this one message and done,” use [WorkClaim](docs/workclaim.md). If the work is “several named steps, maybe wait for a bank, maybe sleep 24 hours,” use Durable Workflow.

---

## Features

| Feature | What it means in practice |
|---------|---------------------------|
| **Feels like normal Java** | `if`, loops, and variables. You do not draw a workflow graph. |
| **Durable identity** | You name the process (`settlement-123`). That name *is* the process. |
| **Safe to start twice** | Calling start three times with the same id creates **one** settlement, not three. |
| **Survives crashes** | JVM restart, pod kill, worker death: another worker continues from saved progress. |
| **Steps vs activities** | **Step** = calculation. **Activity** = talking to the outside world (bank, HTTP, email). |
| **No accidental re-run** | A finished step or activity is **not** executed again when the job resumes. |
| **Fail fast by default** | If something throws and you did not ask for retries, the job **stops**. No silent loops. |
| **Optional retries** | You can attach an explicit retry policy. That policy survives a crash during the wait. |
| **Wait without wasting a machine** | Sleep 6 hours or wait for a bank message **without** keeping a Java thread alive. |
| **External signals** | Another system can say “bank confirmed” and the job continues. |
| **Pass values naturally** | The output of one step is just a Java object you pass into the next. |
| **Nested jobs** | A settlement can start a payout job; the child has its own saved progress. |
| **Idempotency keys for banks** | The engine stops *replay* from re-calling a finished activity. The **bank** still needs your idempotency key if a crash happens *during* the call. |
| **Spring Boot friendly** | Enable it like other starters. Services are injected into your job when it runs. |

---

## Use cases

These are the problems this library is built for.

### 1. End-of-day / settlement

A settlement must load data, calculate, maybe wait overnight if the amount is negative, pay out, wait for the bank, then reconcile.

- The same `settlement-123` must never become two processes.
- If the app dies after calculate but before payout, resume must **not** calculate again and **must** still pay once (from the workflow’s point of view).
- Waiting 24 hours must not require a server to sit idle with a thread sleeping.

### 2. Paying money / calling a bank

Sending money is a **side effect**. Calculations are not.

- Put payouts in `activity()`.
- Put formulas in `step()`.
- Give the bank an idempotency key such as `payout:123` so a retry at the HTTP layer does not double-pay.

### 3. Waiting for something outside your app

A payout is submitted; hours later the bank posts a confirmation.

- The workflow **pauses** (`await`).
- Another service **signals** “bank-confirmation”.
- No worker thread is held while waiting.

### 4. Opening an account or onboarding

Load customer → create account (external) → decide account type (calculation). Each piece is named, saved, and reusable after a crash.

### 5. Work that fans out

A parent settlement starts a child payout job. Each has its own id and history. Starting the child twice with the same name does not create two payouts.

### 6. What this is *not*

| Need | Prefer |
|------|--------|
| One background item: “email this invoice” | WorkClaim queue job |
| “Every night at 2am, kick off settlements” | WorkClaim `@WorkClaimCron` that *starts* workflows |
| Exactly-once guarantee against a bank that ignores idempotency keys | Not possible in software alone — the external system must cooperate |
| A visual BPMN designer | This library is code-first |

---

## How to think about a workflow

```text
Your business process
   ├── step()       calculate, decide, transform (no outside world)
   ├── activity()   HTTP, bank, email, Kafka, database writes
   ├── sleep()      wait for a duration (saved timer)
   ├── await()      wait for an external event
   └── startChild() run another durable process
```

**Step** = thinking. **Activity** = doing something that other systems can see.

---

## Examples

### Start a process (safe to call more than once)

```java
WorkflowHandle<SettlementResult> workflow =
    durable.start(
        "settlement-" + settlementId,
        new SettlementJob(settlementId)
    );
```

Calling this again with `"settlement-123"` returns the **same** process. If it already failed, it stays failed; the library will not quietly start a second one.

### Write the process as a Java method

```java
public SettlementResult run(Context ctx) {

    var settlement =
        ctx.step("load-settlement", () -> settlementService.load(settlementId));

    var calculation =
        ctx.step("calculate", () -> settlementService.calculate(settlement));

    if (calculation.isNegative()) {
        ctx.sleep(Duration.ofHours(24));
        calculation =
            ctx.step("recalculate", () -> settlementService.recalculate(settlement));
    }

    var payout =
        ctx.activity(
            "payout",
            ActivityOptions.idempotencyKey("payout:" + settlementId),
            () -> payoutService.execute(calculation)
        );

    var confirmation = ctx.await("bank-confirmation");

    ctx.step("reconcile", () -> reconcile(payout, confirmation));

    return new SettlementResult(calculation, payout);
}
```

Someone else in the system can later tell the workflow the bank replied:

```java
durable.signal("settlement-123", "bank-confirmation", confirmation);
```

### Pass results from one step to the next

You do not manage a “state bag.” You use Java variables:

```java
var customer = ctx.step("load-customer", () -> customerService.load(customerId));
var account = ctx.activity("create-account", () -> accountService.create(customer));
var accountType = ctx.step("determine-account-type", () -> determineType(account));
```

### Crash in the middle

If **load** and **calculate** finished, and the machine died before **payout**:

- On resume, load and calculate **return saved results** (those methods do not run again).
- Payout **runs**.

### Only retry when you ask

```java
ctx.activity(
    "send-payout",
    RetryPolicy.exponentialBackoff(5, Duration.ofSeconds(10)),
    () -> bank.send()
);
```

With no retry policy, one exception **fails the whole process**. That is intentional: money movement should not retry unless you said so.

### Loops: name each item (never random ids)

```java
for (SettlementItem item : items) {
    ctx.step("settlement-item/" + item.id(), () -> calculate(item));
}
```

### Nested process

```java
var payout = ctx.startChild("payout", new PayoutJob(validation));
return payout.result();
```

### Tiny runnable demo

The `durable-example` module starts a `HelloJob`: a step that builds a greeting, then an activity that “announces” it. Run it with `durable.store=memory` for a local try-out.

---

## Quick start (for developers)

1. Depend on `io.github.albertoclarit:durable-spring-boot-starter`.
2. Enable the starter (auto-config is on the classpath; `@EnableDurableWorkflow` is explicit).
3. Inject `Durable` and call `start(workflowId, job)`.
4. Implement `DurableJob<T>` with `run(Context ctx)`.

For local / tests:

```yaml
durable:
  store: memory
```

For production (progress must survive process death), use PostgreSQL:

```yaml
durable:
  store: jdbc
  datasource:
    jdbc-url: jdbc:postgresql://localhost:5432/durable
    username: durable
    password: secret
```

Mark Spring services on the job as `transient` (or `@JsonIgnore`) so they are not treated as part of the job’s identity. They are re-injected each time the job runs.

---

## Promises and limits (plain language)

| The library **does** | The library **does not** |
|----------------------|---------------------------|
| Remember completed steps and skip them after a crash | Guarantee the bank saw a payment only once if you crash *during* the HTTP call |
| Treat the workflow id as “this business process, once” | Invent a second process when the first one failed |
| Pause for hours without occupying a worker thread | Use Spring `@Scheduled` as the recovery mechanism (it is not cluster-safe) |
| Retry only when you configure a policy | Auto-retry everything (fail-fast is the default) |

**Replay** protects *your workflow*. **Idempotency keys** protect *the external system*. You usually need both for money.

---

## Further reading

| Doc | Audience |
|-----|----------|
| [docs/programming-model.md](docs/programming-model.md) | Exact API rules: start, replay, retry, sleep, signals, children |
| [docs/implementation.md](docs/implementation.md) | Modules, properties, poller (not `@Scheduled`), memory vs JDBC |
| [docs/workclaim.md](docs/workclaim.md) | How this sits next to WorkClaim (queue vs workflow) |

Build and test: `./gradlew test` (Java 25 toolchain).
