# Add Processor Function — Reference Examples

Copy-paste templates for each layer. Adapt names; keep the shape.

All examples assume the project depends on `wakuwaku` (which re-exports / wraps `sqlx`, `redis`, `amqprs`, and the `kanau` 0.5 `Processor` trait) and on `kanau` for the `Processor` trait itself.

## 1. Entity / DB — simple `Find` (returns `Option<T>`)

```rust
use wakuwaku::sqlx::DatabaseProcessor;
use kanau::processor::Processor;
use tracing::instrument;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindUserBalance {
    pub user_id: Uuid,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct UserBalance {
    pub user_id: Uuid,
    pub balance: rust_decimal::Decimal,
}

impl Processor<FindUserBalance> for DatabaseProcessor {
    type Output = Option<UserBalance>;
    type Error  = sqlx::Error;
    #[instrument(skip_all, name = "SQL:FindUserBalance", err)]
    async fn process(&self, input: FindUserBalance) -> Result<Option<UserBalance>, sqlx::Error> {
        sqlx::query_as!(
            UserBalance,
            r#"SELECT user_id, balance FROM "shop"."user_balance" WHERE user_id = $1 LIMIT 1"#,
            input.user_id,
        )
        .fetch_optional(self.db())
        .await
    }
}
```

## 2. Entity / DB — transactional command

```rust
impl Processor<UpdateUserBalance> for DatabaseProcessor {
    type Output = UserBalance;
    type Error  = sqlx::Error;
    #[instrument(skip_all, name = "SQL-Transaction:UpdateUserBalance", err)]
    async fn process(&self, input: UpdateUserBalance) -> Result<UserBalance, sqlx::Error> {
        let mut tx = self.db().begin().await?;

        let row = sqlx::query_as!(/* … main statement … */)
            .fetch_one(&mut *tx)
            .await?;

        sqlx::query!(/* second statement, e.g. change-log insert */)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(row)
    }
}
```

Rules: open the tx on `self.db().begin()`, pass `&mut *tx` to every statement, end with `tx.commit().await?`. Name the span `SQL-Transaction:<DtoName>`.

## 3. Entity — custom result enum instead of `Option` / `bool`

When an operation has more than two outcomes, define an enum rather than overloading `Option<T>` or returning `Result<bool, _>`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum UseEmailOtpResult { Success, NotFound }

impl Processor<UseEmailOtp> for DatabaseProcessor {
    type Output = UseEmailOtpResult;
    type Error  = sqlx::Error;
    #[instrument(skip_all, name = "SQL:UseEmailOtp", err)]
    async fn process(&self, input: UseEmailOtp) -> Result<UseEmailOtpResult, sqlx::Error> {
        let result = sqlx::query!(/* UPDATE … WHERE … */).execute(self.db()).await?;
        if result.rows_affected() == 0 {
            Ok(UseEmailOtpResult::NotFound)
        } else {
            Ok(UseEmailOtpResult::Success)
        }
    }
}
```

## 4. Service — processor composing entity calls

Note: `&self`, `redis.clone()` to get a mutable connection inside, structured `instrument` fields, and **multiple `Processor` impls on the same service struct** rather than methods.

```rust
use kanau::processor::Processor;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;
use wakuwaku::redis::RedisConnection;
use wakuwaku::amqp::AmqpPool;
use wakuwaku::Error;

#[derive(Clone)]
pub struct SessionService {
    pub db: DatabaseProcessor,
    pub redis: RedisConnection,
    pub mq: AmqpPool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSession {
    pub user_id: Uuid,
    pub ip: Option<std::net::IpAddr>,
    pub user_agent: Option<String>,
}

impl Processor<CreateSession> for SessionService {
    type Output = SessionId;
    type Error  = Error;
    #[instrument(
        skip_all,
        name = "CreateSession",
        err,
        fields(user_id = %input.user_id),
    )]
    async fn process(&self, input: CreateSession) -> Result<SessionId, Error> {
        let mut redis = self.redis.clone();

        // compose entity calls via self.db.process(...).await?
        // talk to redis via the KeyValueRead / KeyValueWrite traits
        // publish events via SomeEvent { ... }.send(&self.mq).await?

        Ok(SessionId(/* … */))
    }
}
```

A separate operation on the same service is a new DTO + a new `Processor` impl on the **same** `SessionService` — not a new method.

## 5. Hook — RabbitMQ event consumer

The two impls are always paired.

```rust
use kanau::processor::Processor;
use tracing::{info, instrument};
use uuid::Uuid;
use wakuwaku::Error;
use wakuwaku::amqp::AmqpMessageProcessor;

#[derive(Clone, Default)]
pub struct UserEventLoggerHook;

impl AmqpMessageProcessor<UserLoginEvent> for UserEventLoggerHook {
    const QUEUE: &'static str = "auth_user_login_logger";   // unique, durable
}

impl Processor<UserLoginEvent> for UserEventLoggerHook {
    type Output = ();
    type Error  = Error;
    #[instrument(
        skip_all,
        name = "UserLoginEvent",
        fields(user_id = %event.user_id),
    )]
    async fn process(&self, event: UserLoginEvent) -> Result<(), Error> {
        info!(user_id = %event.user_id, "User logged in");
        Ok(())
    }
}
```

A single hook struct can implement `Processor<Event>` for many different events; bind each one to a distinct queue with its own `AmqpMessageProcessor` impl.

The event type itself (`UserLoginEvent`) must implement `AmqpMessageSend` (i.e. `AmqpRouting + MessageSer`) — define that alongside the event, not inside the hook file. For example:

```rust
use wakuwaku::amqp::{AmqpRouting, AmqpExchangeType, AmqpMessageSend};
use kanau::message::{MessageSer, MessageDe};

#[derive(Debug, Clone /* + MessageSer / MessageDe derives or impls */)]
pub struct UserLoginEvent { pub user_id: uuid::Uuid }

impl AmqpRouting for UserLoginEvent {
    const EXCHANGE:      &'static str        = "auth.events";
    const EXCHANGE_TYPE: AmqpExchangeType    = AmqpExchangeType::Topic;
    const ROUTING_KEY:   &'static str        = "user.login";
}
impl AmqpMessageSend for UserLoginEvent {}
```

## 6. Hook — interval-job-driven processor composing entities

Interval signals are just AMQP messages that implement `IntervalJobExecutionSignal` (which extends `AmqpMessageSend`), so the hook shape is identical to a normal event consumer.

```rust
use kanau::processor::Processor;
use wakuwaku::Error;
use wakuwaku::amqp::AmqpMessageProcessor;
use wakuwaku::sqlx::DatabaseProcessor;
use wakuwaku::redis::RedisConnection;

#[derive(Clone)]
pub struct AuthCronExecutor {
    pub config_store: RedisConnection,
    pub db: DatabaseProcessor,
}

impl AmqpMessageProcessor<EmailOtpAndLinksCleanupSignal> for AuthCronExecutor {
    const QUEUE: &'static str = "auth_cron_executor";
}

impl Processor<EmailOtpAndLinksCleanupSignal> for AuthCronExecutor {
    type Output = ();
    type Error  = Error;
    async fn process(&self, input: EmailOtpAndLinksCleanupSignal) -> Result<(), Error> {
        let otp_clean  = self.db.process(DeleteEmailOtpsBefore        { before: /* … */ });
        let link_clean = self.db.process(DeleteEmailVerifyLinkBefore  { before: /* … */ });
        tokio::try_join!(otp_clean, link_clean)?;
        Ok(())
    }
}
```

Where `EmailOtpAndLinksCleanupSignal` implements `wakuwaku::interval_job::IntervalJobExecutionSignal` (and therefore `AmqpMessageSend`).

## 7. Redis key/value — NOT a `Processor` impl

Pure key-value caching uses the `KeyValue` family of traits from `wakuwaku::redis` instead of defining a `Processor`:

```rust
use kanau::message::{MessageDe, MessageSer};
use wakuwaku::redis::{KeyValue, KeyValueRead, KeyValueWrite, RedisKey};

pub struct SudoToken {
    pub key: RedisKey,    // e.g. derived from a user id
    pub value: Vec<u8>,   // anything that implements MessageSer + MessageDe
}

impl KeyValue for SudoToken {
    type Key   = RedisKey;
    type Value = Vec<u8>;
    fn key(&self)             -> Self::Key   { self.key.clone() }
    fn value(&self)           -> Self::Value { self.value.clone() }
    fn into_value(self)       -> Self::Value { self.value }
    fn new(key: Self::Key, value: Self::Value) -> Self { Self { key, value } }
}

impl KeyValueRead  for SudoToken where Vec<u8>: MessageDe {}
impl KeyValueWrite for SudoToken where Vec<u8>: MessageSer {}
```

Read with `SudoToken::read(&mut conn, key).await?`, write with `token.write(&mut conn).await?` or `token.write_with_ttl(&mut conn, ttl).await?`. There is no `process` method on these — they are not `Processor`s.

## Legacy pitfalls

Older versions of `kanau` (and stale docs / blog posts referring to them) used either of these two patterns. Neither compiles against `kanau 0.5` / `wakuwaku`:

```rust
// WRONG — `async_trait` is not exported for `Processor` in kanau 0.5
#[kanau::processor::async_trait]
impl Processor<ListUsersRequest, Result<ListUsersResponse, Error>> for UserService { /* … */ }
```

```rust
// RIGHT — native async fn in trait + associated Output / Error types
impl Processor<ListUsersRequest> for UserService {
    type Output = ListUsersResponse;
    type Error  = Error;
    async fn process(&self, input: ListUsersRequest) -> Result<ListUsersResponse, Error> { /* … */ }
}
```

When in doubt, mirror live code in the workspace's `services/` / `entities/` / `hooks/` directories rather than any documentation that still uses the two-generic / `#[async_trait]` form.

## Quick file-pick cheatsheet

| Goal | Layer | Section |
|---|---|---|
| Add a `Find` / `List` / `Count` query | Entity / DB | §1, §3 |
| Add a transactional command | Entity / DB | §2 |
| Add multiple operations to a service | Service | §4 |
| Add an event-driven hook | Hook | §5 |
| Add an interval-job-triggered hook | Hook | §6 |
| Cache something in Redis (no `Processor`) | Entity / Redis | §7 |
