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

### 6. Statement of Account (SOA) report generation

A period-end SOA run must **generate the statement**, **notify metrics**, **email** counterparties, and **upload the file over SFTP**. That is several side effects in a fixed order. If the process dies after email but before SFTP, resume must **not** generate the SOA again, **not** bump metrics again, **not** send a second email, and **must** still upload.

- One business id (`soa-2026-09-11` or `soa-` + partner + date) so a cron or UI cannot start two copies of the same report.
- **Generate SOA** is usually a `step` if you only build the document in memory (rows → PDF/CSV bytes). Use an `activity` if generation already writes to disk, object storage, or a report table.
- **Notify metrics**, **send email**, and **upload SFTP** are `activity` calls (externally visible). Give email and SFTP idempotency keys the downstream system can honor.
- Order in `run` is the order of work: generate → metrics → email → SFTP. A crash in the middle continues from the first unfinished activity.

### 7. What this is *not*

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

Each listing is a **complete Java class**: `package`, `import`s, `public class …`, fields, constructors, and methods. Put each class in its own file with the same name.

Spring collaborators on a job must be `transient` (or `@JsonIgnore`). They are not part of the workflow identity; they are injected again every time the job runs, including after a crash. Include a no-arg constructor so the engine can deserialize the job.

Gradle:

```gradle
dependencies {
    implementation 'io.github.albertoclarit:durable-spring-boot-starter:0.1.0'
}
```

Local config (`application.yml`) — progress is lost if the process dies:

```yaml
durable:
  store: memory
spring:
  application:
    name: settlement-app
```

Production — progress lives in PostgreSQL:

```yaml
durable:
  store: jdbc
  datasource:
    jdbc-url: jdbc:postgresql://localhost:5432/durable
    username: durable
    password: secret
```

### `SettlementJob.java` — the workflow class

This is the class you implement. It must declare `implements DurableJob<SettlementResult>`.

```java
package com.example.settlement;

import io.github.albertoclarit.durable.ActivityOptions;
import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.DurableJob;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;

public class SettlementJob implements DurableJob<SettlementResult> {

    private final long settlementId;

    @Autowired
    private transient SettlementService settlementService;

    @Autowired
    private transient PayoutService payoutService;

    public SettlementJob() {
        this(0L);
    }

    public SettlementJob(long settlementId) {
        this.settlementId = settlementId;
    }

    @Override
    public SettlementResult run(Context ctx) {
        Settlement settlement = ctx.step(
                "load-settlement",
                () -> settlementService.load(settlementId)
        );

        Calculation calculation = ctx.step(
                "calculate",
                () -> settlementService.calculate(settlement)
        );

        if (calculation.isNegative()) {
            ctx.sleep(Duration.ofHours(24));
            calculation = ctx.step(
                    "recalculate",
                    () -> settlementService.recalculate(settlement)
            );
        }

        PayoutReceipt payout = ctx.activity(
                "payout",
                ActivityOptions.idempotencyKey("payout:" + settlementId),
                () -> payoutService.execute(calculation)
        );

        BankConfirmation confirmation = ctx.await(
                "bank-confirmation",
                BankConfirmation.class
        );

        ctx.step(
                "reconcile",
                () -> settlementService.reconcile(payout, confirmation)
        );

        return new SettlementResult(calculation, payout);
    }
}
```

If **load** and **calculate** finished and the machine died before **payout**: on resume those two steps return saved results (the lambdas do not run again); **payout** runs. Values move between operations as ordinary Java locals — there is no extra state bag.

**How `sleep` (and `await`) work in Java:** `run` is **not** lazily evaluated and the JVM does **not** freeze the stack at `sleep`. The method is ordinary eager Java. The first time `ctx.sleep(...)` runs, the engine saves the wake time and **stops that call to `run()`** (internal suspend). The following lines — including `"recalculate"` — are **not** executed in that invocation. After 24 hours a worker **calls `run()` again from the top**. Completed steps return saved values; `sleep` now returns; **then** `"recalculate"` runs. Same idea for `await`: this attempt ends at the wait; the next attempt continues after it.

### `SettlementApplication.java`

```java
package com.example.settlement;

import io.github.albertoclarit.durable.annotation.EnableDurableWorkflow;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableDurableWorkflow
public class SettlementApplication {

    public static void main(String[] args) {
        SpringApplication.run(SettlementApplication.class, args);
    }
}
```

### `Settlement.java`

```java
package com.example.settlement;

import java.math.BigDecimal;

public class Settlement {

    private final long id;
    private final BigDecimal grossAmount;

    public Settlement() {
        this(0L, BigDecimal.ZERO);
    }

    public Settlement(long id, BigDecimal grossAmount) {
        this.id = id;
        this.grossAmount = grossAmount;
    }

    public long id() {
        return id;
    }

    public BigDecimal grossAmount() {
        return grossAmount;
    }
}
```

### `Calculation.java`

```java
package com.example.settlement;

import java.math.BigDecimal;

public class Calculation {

    private final BigDecimal netAmount;

    public Calculation() {
        this(BigDecimal.ZERO);
    }

    public Calculation(BigDecimal netAmount) {
        this.netAmount = netAmount;
    }

    public BigDecimal netAmount() {
        return netAmount;
    }

    public boolean isNegative() {
        return netAmount.signum() < 0;
    }
}
```

### `PayoutReceipt.java`

```java
package com.example.settlement;

public class PayoutReceipt {

    private final String bankReference;

    public PayoutReceipt() {
        this("");
    }

    public PayoutReceipt(String bankReference) {
        this.bankReference = bankReference;
    }

    public String bankReference() {
        return bankReference;
    }
}
```

### `BankConfirmation.java`

```java
package com.example.settlement;

public class BankConfirmation {

    private final String status;

    public BankConfirmation() {
        this("");
    }

    public BankConfirmation(String status) {
        this.status = status;
    }

    public String status() {
        return status;
    }
}
```

### `SettlementResult.java`

```java
package com.example.settlement;

public class SettlementResult {

    private final Calculation calculation;
    private final PayoutReceipt payout;

    public SettlementResult() {
        this(new Calculation(), new PayoutReceipt());
    }

    public SettlementResult(Calculation calculation, PayoutReceipt payout) {
        this.calculation = calculation;
        this.payout = payout;
    }

    public Calculation calculation() {
        return calculation;
    }

    public PayoutReceipt payout() {
        return payout;
    }
}
```

### `SettlementItem.java`

```java
package com.example.settlement;

import java.math.BigDecimal;

public class SettlementItem {

    private final long id;
    private final BigDecimal amount;

    public SettlementItem() {
        this(0L, BigDecimal.ZERO);
    }

    public SettlementItem(long id, BigDecimal amount) {
        this.id = id;
        this.amount = amount;
    }

    public long id() {
        return id;
    }

    public BigDecimal amount() {
        return amount;
    }
}
```

### `SettlementService.java`

```java
package com.example.settlement;

import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

@Service
public class SettlementService {

    public Settlement load(long settlementId) {
        return new Settlement(settlementId, new BigDecimal("100.00"));
    }

    public Calculation calculate(Settlement settlement) {
        return new Calculation(settlement.grossAmount());
    }

    public Calculation recalculate(Settlement settlement) {
        return new Calculation(settlement.grossAmount().abs());
    }

    public List<SettlementItem> items(Settlement settlement) {
        return List.of(
                new SettlementItem(1, new BigDecimal("40.00")),
                new SettlementItem(2, new BigDecimal("60.00"))
        );
    }

    public BigDecimal lineAmount(SettlementItem item) {
        return item.amount();
    }

    public SettlementResult reconcile(PayoutReceipt payout, BankConfirmation confirmation) {
        return new SettlementResult(new Calculation(BigDecimal.ZERO), payout);
    }
}
```

### `PayoutService.java`

```java
package com.example.settlement;

import org.springframework.stereotype.Service;

@Service
public class PayoutService {

    public PayoutReceipt execute(Calculation calculation) {
        return new PayoutReceipt("bank-ref-" + calculation.netAmount());
    }

    public PayoutReceipt send(SettlementItem item, String idempotencyKey) {
        return new PayoutReceipt("item-" + item.id() + "-" + idempotencyKey);
    }
}
```

### `SettlementController.java` — start (safe to call more than once)

```java
package com.example.settlement;

import io.github.albertoclarit.durable.Durable;
import io.github.albertoclarit.durable.WorkflowHandle;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class SettlementController {

    private final Durable durable;

    public SettlementController(Durable durable) {
        this.durable = durable;
    }

    @PostMapping("/settlements/{id}/start")
    public String start(@PathVariable long id) {
        WorkflowHandle<SettlementResult> workflow = durable.start(
                "settlement-" + id,
                new SettlementJob(id)
        );
        return workflow.id();
    }
}
```

`POST /settlements/123/start` three times still creates **one** process named `settlement-123`. If it already failed, it stays failed; the library will not quietly start a second one.

`start` does not wait for the job to finish. Use `workflow.result()` (or `result(Duration)`) if the HTTP caller must block until completion.

### `BankWebhookController.java` — signal a waiting workflow

```java
package com.example.settlement;

import io.github.albertoclarit.durable.Durable;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BankWebhookController {

    private final Durable durable;

    public BankWebhookController(Durable durable) {
        this.durable = durable;
    }

    @PostMapping("/settlements/{id}/bank-confirmation")
    public void confirmed(
            @PathVariable long id,
            @RequestBody BankConfirmation confirmation
    ) {
        durable.signal("settlement-" + id, "bank-confirmation", confirmation);
    }
}
```

If the webhook arrives **before** the job reaches `await`, the signal is stored and applied when `await` runs. Signaling an unknown workflow id fails.

### `ItemPayoutJob.java` — activity inside a loop

Give every iteration a **stable** name from a business id. Never `UUID.randomUUID()`. Reusing `"payout"` for every item fails (duplicate name).

```java
package com.example.settlement;

import io.github.albertoclarit.durable.ActivityContext;
import io.github.albertoclarit.durable.ActivityOptions;
import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.DurableJob;
import org.springframework.beans.factory.annotation.Autowired;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ItemPayoutJob implements DurableJob<List<PayoutReceipt>> {

    private final long settlementId;

    @Autowired
    private transient SettlementService settlementService;

    @Autowired
    private transient PayoutService payoutService;

    public ItemPayoutJob() {
        this(0L);
    }

    public ItemPayoutJob(long settlementId) {
        this.settlementId = settlementId;
    }

    @Override
    public List<PayoutReceipt> run(Context ctx) {
        Settlement settlement = ctx.step(
                "load",
                () -> settlementService.load(settlementId)
        );
        List<SettlementItem> items = ctx.step(
                "load-items",
                () -> settlementService.items(settlement)
        );

        List<PayoutReceipt> receipts = new ArrayList<>();
        for (SettlementItem item : items) {
            BigDecimal amount = ctx.step(
                    "line/" + item.id(),
                    () -> settlementService.lineAmount(item)
            );
            PayoutReceipt receipt = ctx.activity(
                    "payout/" + item.id(),
                    ActivityOptions.idempotencyKey(
                            "payout:" + settlementId + ":" + item.id()
                    ),
                    (ActivityContext act) -> payoutService.send(item, act.idempotencyKey())
            );
            receipts.add(receipt);
            if (amount.signum() < 0) {
                throw new IllegalStateException("negative line " + item.id());
            }
        }
        return receipts;
    }
}
```

### `RetryingPayoutJob.java` — retry only when you ask

Without a policy, one exception **fails the whole process**. That is intentional for money movement. The retry policy is saved; if the worker dies while waiting for the next attempt, recovery continues that policy.

```java
package com.example.settlement;

import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.DurableJob;
import io.github.albertoclarit.durable.RetryPolicy;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;

public class RetryingPayoutJob implements DurableJob<PayoutReceipt> {

    private final long settlementId;

    @Autowired
    private transient SettlementService settlementService;

    @Autowired
    private transient PayoutService payoutService;

    public RetryingPayoutJob() {
        this(0L);
    }

    public RetryingPayoutJob(long settlementId) {
        this.settlementId = settlementId;
    }

    @Override
    public PayoutReceipt run(Context ctx) {
        Calculation calculation = ctx.step(
                "calculate",
                () -> settlementService.calculate(settlementService.load(settlementId))
        );
        return ctx.activity(
                "send-payout",
                RetryPolicy.exponentialBackoff(5, Duration.ofSeconds(10)),
                () -> payoutService.execute(calculation)
        );
    }
}
```

### `ParentSettlementJob.java` and `PayoutChildJob.java` — nested workflow

The child id is `parentId + "/" + name` (for example `settlement-123/payout`). Starting that child twice is get-or-create. `result()` waits durably; the parent may suspend until the child finishes.

```java
package com.example.settlement;

import io.github.albertoclarit.durable.ChildWorkflow;
import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.DurableJob;
import org.springframework.beans.factory.annotation.Autowired;

public class ParentSettlementJob implements DurableJob<PayoutReceipt> {

    private final long settlementId;

    @Autowired
    private transient SettlementService settlementService;

    public ParentSettlementJob() {
        this(0L);
    }

    public ParentSettlementJob(long settlementId) {
        this.settlementId = settlementId;
    }

    @Override
    public PayoutReceipt run(Context ctx) {
        Calculation validation = ctx.step(
                "validate",
                () -> settlementService.calculate(settlementService.load(settlementId))
        );
        ChildWorkflow<PayoutReceipt> payout = ctx.startChild(
                "payout",
                new PayoutChildJob(settlementId, validation)
        );
        return payout.result();
    }
}
```

```java
package com.example.settlement;

import io.github.albertoclarit.durable.ActivityOptions;
import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.DurableJob;
import org.springframework.beans.factory.annotation.Autowired;

public class PayoutChildJob implements DurableJob<PayoutReceipt> {

    private final long settlementId;
    private final Calculation calculation;

    @Autowired
    private transient PayoutService payoutService;

    public PayoutChildJob() {
        this(0L, new Calculation());
    }

    public PayoutChildJob(long settlementId, Calculation calculation) {
        this.settlementId = settlementId;
        this.calculation = calculation;
    }

    @Override
    public PayoutReceipt run(Context ctx) {
        return ctx.activity(
                "execute",
                ActivityOptions.idempotencyKey("payout:" + settlementId),
                () -> payoutService.execute(calculation)
        );
    }
}
```

### `SoaReportJob.java` — generate SOA, metrics, email, SFTP

Start with a stable id such as `"soa-" + partnerId + "-" + reportDate`. A nightly cron should call `durable.start` with that id, not reimplement the four steps as four unrelated queue jobs.

```java
package com.example.settlement;

import io.github.albertoclarit.durable.ActivityOptions;
import io.github.albertoclarit.durable.Context;
import io.github.albertoclarit.durable.DurableJob;
import org.springframework.beans.factory.annotation.Autowired;

public class SoaReportJob implements DurableJob<SoaReportResult> {

    private final String partnerId;
    private final String reportDate;

    @Autowired
    private transient SoaGenerator soaGenerator;

    @Autowired
    private transient MetricsNotifier metricsNotifier;

    @Autowired
    private transient EmailSender emailSender;

    @Autowired
    private transient SftpUploader sftpUploader;

    public SoaReportJob() {
        this("", "");
    }

    public SoaReportJob(String partnerId, String reportDate) {
        this.partnerId = partnerId;
        this.reportDate = reportDate;
    }

    @Override
    public SoaReportResult run(Context ctx) {
        String reportKey = partnerId + "-" + reportDate;

        SoaDocument document = ctx.step(
                "generate-soa",
                () -> soaGenerator.generate(partnerId, reportDate)
        );

        ctx.activity(
                "notify-metrics",
                ActivityOptions.idempotencyKey("metrics:soa:" + reportKey),
                () -> {
                    metricsNotifier.recordSoaGenerated(partnerId, reportDate, document.sizeBytes());
                    return null;
                }
        );

        ctx.activity(
                "send-email",
                ActivityOptions.idempotencyKey("email:soa:" + reportKey),
                () -> {
                    emailSender.sendSoa(partnerId, document);
                    return null;
                }
        );

        ctx.activity(
                "upload-sftp",
                ActivityOptions.idempotencyKey("sftp:soa:" + reportKey),
                () -> sftpUploader.upload(document)
        );

        return new SoaReportResult(reportKey, document.fileName());
    }
}
```

```java
package com.example.settlement;

public class SoaDocument {

    private final String fileName;
    private final byte[] content;

    public SoaDocument() {
        this("", new byte[0]);
    }

    public SoaDocument(String fileName, byte[] content) {
        this.fileName = fileName;
        this.content = content;
    }

    public String fileName() {
        return fileName;
    }

    public byte[] content() {
        return content;
    }

    public int sizeBytes() {
        return content.length;
    }
}
```

```java
package com.example.settlement;

public class SoaReportResult {

    private final String reportKey;
    private final String fileName;

    public SoaReportResult() {
        this("", "");
    }

    public SoaReportResult(String reportKey, String fileName) {
        this.reportKey = reportKey;
        this.fileName = fileName;
    }

    public String reportKey() {
        return reportKey;
    }

    public String fileName() {
        return fileName;
    }
}
```

If the worker dies after **send-email** and before **upload-sftp**, replay returns the saved SOA document, skips metrics and email, and runs SFTP only.

### Demo in this repository

The `durable-example` module is a complete Spring Boot app with full class files: `DurableExampleApplication`, `HelloJob`, and `HelloRunner`. It uses `durable.store=memory`. Run `:durable-example` to print `HELLO DURABLE`.

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
