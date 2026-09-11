# Implementation notes

This describes the **current** runtime. The public contract is [programming-model.md](programming-model.md). Schema and class layout may change as long as that contract holds.

## Modules

| Module | Role |
|--------|------|
| `durable-core` | API + engine (`InMemoryWorkflowStore`, `JdbcWorkflowStore`, replay, poller) |
| `durable-autoconfigure` | `@EnableDurableWorkflow`, properties, Spring job injection |
| `durable-spring-boot-starter` | App dependency |
| `durable-example` | In-memory `HelloJob` demo |

Group: `io.github.albertoclarit`. Java package: `io.github.albertoclarit.durable`. Toolchain: **JDK 25** (`options.release = 25`). Spring Boot **4.1.x**.

## Replay engine

`DefaultDurable` + `ReplayContext`:

1. Persist the job type and JSON input (fingerprint = SHA-256 of type + canonical JSON).
2. A worker **claims** the workflow (lease), deserializes the job, injects collaborators, calls `run`.
3. Each `step` / `activity` / `sleep` / `await` / `now` / `newId` / `random` is keyed by name.
4. Completed operations deserialize the stored value and skip the body.
5. `sleep` / `await` / unfinished child `result()` throw an internal suspend signal: status `WAITING`, **lease released**, no thread held.
6. After crash, another worker claims and invokes `run` from the top.

Workers are a **dedicated daemon poller** plus virtual threads. This is **not** Spring `@Scheduled`.

## Persistence

- **memory**: process-local, for tests and local demos (`durable.store=memory`).
- **jdbc**: PostgreSQL (`durable.store=jdbc` or set `durable.datasource.jdbc-url` / `use-primary`). Flyway table `durable_flyway_schema_history`; workflow tables `durable_workflow`, `durable_operation`, `durable_signal`. Get-or-create uses `INSERT … ON CONFLICT DO NOTHING`. Claim uses `FOR UPDATE SKIP LOCKED`.

## Spring properties

```yaml
durable:
  enabled: true
  store: jdbc          # or memory
  poll-interval: 100ms
  lease: 30s
  worker-id: optional
  datasource:
    use-primary: false
    jdbc-url: jdbc:postgresql://localhost:5432/durable
    username: durable
    password: secret
```

`@EnableDurableWorkflow` is optional if the starter is on the classpath (auto-config imports). Collaborators: field `@Autowired` on `transient` fields.

## Tests without Spring

```java
try (InMemoryDurable env = InMemoryDurable.create()) {
    env.client().start("id", new MyJob()).result(Duration.ofSeconds(5));
}
```

`InMemoryDurable.sharedStore()` models two workers sharing history (crash/resume).

## Wake publisher

`WorkflowWakePublisher` is an optional hook when a workflow becomes runnable (timer, signal, child completion). The in-process poller remains authoritative. See [workclaim.md](workclaim.md).
