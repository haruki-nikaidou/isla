---
name: add-processor-function
description: Adds a new `impl Processor<T> for Q` block in a Rust project built on the `wakuwaku` framework + `kanau` 0.5 processor pattern, choosing the correct layer (entity / service / hook), wiring the right error type, and following the conventional shape. Use when the user asks to "add a processor", "implement Processor", create a new query/command/request/event handler, or add a `Find...`, `List...`, `Count...`, `Create...`, `Update...`, `Delete...`, or `...Request`/`...Signal`/`...Event` DTO to a service, entity, or hook module.
---

# Add Processor Function

A "processor function" in this codebase is `impl kanau::processor::Processor<Input> for SomeStruct`. It is the **only** API style used to expose async behavior across layers. This skill keeps new processors aligned with the architecture defined by the `wakuwaku` framework crate (`wakuwaku::Error`, `wakuwaku::sqlx::DatabaseProcessor`, `wakuwaku::redis::*`, `wakuwaku::amqp::*`, `wakuwaku::interval_job::*`).

> The trait uses **native async fn in trait (RPITIT)** with `type Output` / `type Error` associated types. **Do NOT use `#[async_trait]` or the legacy `Processor<I, Result<O, E>>` two-generic form** — both belong to older kanau releases and will not compile against the current `kanau 0.5` API. See [reference.md](reference.md#legacy-pitfalls).

## Step 1: Decide which layer `Q` belongs to

Pick exactly one. Layers are not interchangeable.

| Layer | `Q` (impl target) | `T` (input DTO) | `Output` | `Error` |
|---|---|---|---|---|
| **Entity / DB** | `wakuwaku::sqlx::DatabaseProcessor` | `FindX`, `ListX`, `CountX`, `CreateX`, `UpdateX`, `DeleteX` | row(s) / id / `()` | `sqlx::Error` |
| **Entity / Redis** | a `KeyValue` struct (via `KeyValueRead`/`KeyValueWrite`) — **not** `Processor` | — | — | `wakuwaku::Error` |
| **Service** | `XxxService` (a `#[derive(Clone)] struct` holding `db: DatabaseProcessor` / `redis: RedisConnection` / `mq: AmqpPool`) | `XxxRequest`, `XxxCommand`, `CreateY`, `ListY`, … | service DTO | `wakuwaku::Error` |
| **Hook** | `XxxHook` / `XxxExecutor` (a `#[derive(Clone)] struct`) | a type that implements `AmqpMessageSend` (an event / signal) | `()` | `wakuwaku::Error` |

Decision rules:

- **Has raw SQL?** → Entity / DB. SQL must never live in services or hooks.
- **Composes multiple entity calls, validates input, applies policy, paginates, computes business values?** → Service.
- **Triggered by a queued event (`AmqpMessageSend` / `IntervalJobExecutionSignal`)?** → Hook.
- **Pure key/value Redis caching?** → Use the `KeyValue` / `KeyValueRead` / `KeyValueWrite` traits from `wakuwaku::redis`, not a new `Processor` impl. Skip step 2 onward.

Conventionally, entity files live under `entities/db/` and `entities/redis/`, services live under `services/`, and hooks live under `hooks/`. Mirror whatever layout the project already uses; don't invent a new one.

## Step 2: Use the canonical impl shape

Always write the impl in this exact shape. Replace placeholders only.

```rust
use kanau::processor::Processor;
use tracing::{instrument, debug};

#[derive(Debug, Clone /* Copy/PartialEq/Eq when fields allow */)]
pub struct MyInputDto {
    pub field_a: SomeType,
    pub field_b: AnotherType,
}

// Optional: an explicit enum for non-Result outcomes (preferred over panicking on `Option`/`bool` ambiguities).
#[derive(Debug, Clone /* ... */)]
pub enum MyOutcome { Ok(Thing), NotFound }

impl Processor<MyInputDto> for SomeStruct {
    type Output = MyOutcome;       // or Vec<T>, Option<T>, i64, (), a fresh struct…
    type Error  = sqlx::Error;     // entity layer: sqlx::Error  |  service/hook layer: wakuwaku::Error

    #[instrument(skip_all, name = "SQL:MyInputDto" /* services: "MyInputDto" */, err, fields(field_a = %input.field_a))]
    async fn process(&self, input: MyInputDto) -> Result<MyOutcome, sqlx::Error> {
        // body
    }
}
```

Non-negotiable rules:

1. **No `#[async_trait]`, no `#[kanau::processor::async_trait]`.** Use `async fn process(&self, input: T) -> Result<…, …>` directly.
2. **`type Output` and `type Error` are mandatory associated types** — never use the old `Processor<T, Result<O, E>>` form.
3. **`&self`, not `&mut self`.** Mutable state goes through `.clone()` of an internal `RedisConnection`/`AmqpPool`/`DatabaseProcessor` inside `process`.
4. **One DTO per operation.** A new operation = a new input struct (`MyInputDto`), not a new method. Do not add bespoke `pub async fn do_thing(…)` APIs on services.
5. **`#[instrument(skip_all, name = "…", err)]`** on every `process`. Entity span names are prefixed `SQL:` (or `SQL-Transaction:` if it opens a tx). Service / hook span names use the DTO type name.
6. The DTO type should derive at minimum `Debug, Clone`. Add `Copy, PartialEq, Eq` when fields permit.

## Step 3: Layer-specific extras

Read the relevant subsection only.

### 3a) Entity / DB processor

- Place in an existing file under `entities/db/<table_or_topic>.rs`, or create one and register it in the parent `mod.rs`.
- Body uses `sqlx::query!`, `sqlx::query_as!`, or `sqlx::query_file_as!` against `self.db()` — the `&sqlx::PgPool` accessor on `DatabaseProcessor`.
- For multi-statement atomic work, open a transaction: `let mut tx = self.db().begin().await?;` … `tx.commit().await?;` and use `name = "SQL-Transaction:…"`.
- See [reference.md §1–§3](reference.md) for `Find`, transactional command, and custom-result-enum examples.

### 3b) Service processor

- The service struct holds infra handles, e.g. `pub db: DatabaseProcessor`, `pub redis: RedisConnection`, `pub mq: AmqpPool`. Use `DatabaseProcessor`, not raw `sqlx::PgPool`.
- All data access goes through `self.db.process(EntityDto { … }).await?`. **No `sqlx::query!` in services.**
- Errors map up to `wakuwaku::Error` — `sqlx::Error`, `redis::RedisError`, and `amqprs::error::Error` already have `#[from]` conversions on `wakuwaku::Error`, so `?` "just works".
- Cross-service work: hold the other service inside the struct and call it via `Processor::process`, not bespoke methods.
- See [reference.md §4](reference.md) for a service with multiple operations on the same struct.

### 3c) Hook processor

Hooks consume RabbitMQ events. The shape is:

```rust
impl AmqpMessageProcessor<MyEvent> for MyHook {
    const QUEUE: &'static str = "<scope>_<purpose>";   // durable, globally unique
}

impl Processor<MyEvent> for MyHook {
    type Output = ();
    type Error  = wakuwaku::Error;
    #[instrument(skip_all, name = "MyEvent", err)]
    async fn process(&self, event: MyEvent) -> Result<(), wakuwaku::Error> { /* … */ }
}
```

- `MyEvent` must already implement `AmqpMessageSend` (i.e. `AmqpRouting` + `MessageSer`). Define those alongside the event type, not inside the hook file.
- `AmqpMessageProcessor` is bound to `Processor<Message, Output = (), Error = wakuwaku::Error>`. Returning a different output or error type will not compile.
- For interval-driven / cron-like hooks, accept a type that implements `wakuwaku::interval_job::IntervalJobExecutionSignal` (which itself extends `AmqpMessageSend`) — the hook shape is identical to a normal event consumer. See [reference.md §5–§6](reference.md).

## Step 4: Wire it up

1. Re-export the new type from the parent `mod.rs` if you added a new file.
2. If it is a service or hook newly held by a server / binary, add the field to the constructor where the server is assembled so it is instantiated.
3. If it is invoked over an RPC layer, keep the handler thin — it should only call `service.process(dto).await` and translate the error.
4. Run `cargo check -p <crate-name>` and `cargo clippy -p <crate-name>` before declaring done. Many projects on `wakuwaku` set `#![forbid(clippy::unwrap_used, clippy::panic, clippy::expect_used)]`; bubble errors via `?` instead.

## Common mistakes to avoid

- Writing `#[kanau::processor::async_trait]` or `Processor<Input, Result<Output, Error>>`. Both are obsolete; `kanau 0.5` uses native async fn + associated `Output` / `Error` types.
- Putting `sqlx::query!` inside a service — it must go inside an entity processor.
- Adding a bespoke `pub async fn foo(&self, …)` on a service instead of a new `Processor<FooRequest>` impl.
- Omitting `#[instrument]`, or using `skip(self)` instead of `skip_all` (PII / secrets in inputs must not be logged).
- Returning anything other than `Result<(), wakuwaku::Error>` from an `AmqpMessageProcessor` hook — the trait bound rejects everything else.
- Reaching for `self.executor()` from older docs — only `self.db()` exists on `wakuwaku::sqlx::DatabaseProcessor`.

## Definition of done

- [ ] Correct layer chosen (entity / service / hook) per Step 1.
- [ ] Impl uses native async fn + `type Output` + `type Error`, on `&self`.
- [ ] DTO derives `Debug, Clone` (at minimum) and lives next to the impl.
- [ ] `#[instrument(skip_all, name = "…", err)]` present; entity names prefixed `SQL:` / `SQL-Transaction:`.
- [ ] Service impls call entities via `self.db.process(…)`; no raw SQL.
- [ ] Hook impls: paired `AmqpMessageProcessor<Event>` with unique `QUEUE`.
- [ ] `cargo check` passes.

## Further reading

- For longer worked examples of each layer, see [reference.md](reference.md).
- For the `wakuwaku` crate itself (error type, pool, redis, amqp, interval_job), read its source — every public item used by this skill is in `wakuwaku::{error, sqlx, redis, amqp, interval_job, pool}`.
