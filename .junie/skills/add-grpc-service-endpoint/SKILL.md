---
name: add-grpc-service-endpoint
description: Implements a full vertical slice through the Isla layering — a new or extended gRPC endpoint (rpc) backed by service processors that call entity processors running raw SQL — including re-running `cargo sqlx prepare` and handling errors with the `wakuwaku` error type. Use when the user asks to "implement the `Xxx` gRPC service", "add an endpoint that …", "expose `Yyy` over gRPC", or add a query/command that hits the database and is reachable over gRPC.
---

# Add gRPC Service Endpoint (rpc → service → entity, with sqlx prepare)

Use this skill when a task requires a **full vertical slice** through the Isla
layering: a new (or extended) **gRPC endpoint** backed by one or more
**service** processors that call **entity** processors which run **raw SQL**,
where the work also requires re-running `cargo sqlx prepare`, and where errors
are handled with the **`wakuwaku`** error type.

Trigger phrases: "implement the `Xxx` gRPC service", "add an endpoint that …",
"expose `Yyy` over gRPC", "add a query/command that hits the database and is
reachable over gRPC".

> This skill assumes the per-operation `Processor` pattern is already
> understood. If you are unsure how to shape a single `impl Processor<T> for Q`,
> read the **`add-processor-function`** skill first; this skill focuses on
> wiring the three layers together plus the sqlx-offline workflow.

## The layering contract (never violate)

```
proto (.proto)  ──build.rs──▶  proto.rs module  ──▶  rpc/  ──▶  services/  ──▶  entities/db/  ──▶  PostgreSQL
                                                  (gRPC)     (business logic)   (raw SQL only)
```

- **`entities/db/*.rs`** — the **only** place raw SQL (`sqlx::query!` /
  `sqlx::query_as!`) is allowed. `impl Processor<Dto> for DatabaseProcessor`,
  `type Error = sqlx::Error`.
- **`services/*.rs`** — business logic. A `#[derive(Clone)]` `XxxService` struct
  holding `db: DatabaseProcessor` (and possibly other services). Calls entities
  via `self.db.process(EntityDto { … }).await?`. `type Error = wakuwaku::Error`.
- **`rpc/*.rs`** — thin `XxxGrpcService` implementing the generated `tonic`
  trait. Resolves the caller, maps proto ⇄ domain types, calls
  `service.process(dto).await?`, wraps the result in `Response`. **Never** calls
  the DB directly.

Naming: `XxxEntity` (rows), `FindX`/`ListX`/`CreateX`/`UpdateX`/`DeleteX`
(entity DTOs), `XxxService`, `XxxRequest`/`XxxCommand` (service DTOs),
`XxxGrpcService` (rpc). See [reference.md](reference.md) for full worked code.

## Step 1 — Define / extend the proto contract

1. Add or edit the `.proto` under `modules/<module>/proto/`. One `service` block
   with one `rpc` per operation; one request + one response message per `rpc`
   (even when empty — keep them so the wire shape can evolve).
2. Register **every** proto file in `build.rs`:

   ```rust
   fn main() -> Result<(), Box<dyn std::error::Error>> {
       tonic_prost_build::compile_protos("proto/preauth.proto")?;
       tonic_prost_build::compile_protos("proto/account.proto")?; // new file → new line
       Ok(())
   }
   ```

3. Expose the generated code in `src/proto.rs`, matching the proto `package`:

   ```rust
   pub mod account {
       #![allow(clippy::all)]
       #![allow(clippy::pedantic)]
       tonic::include_proto!("isla.auth.account"); // == the `package` in the .proto
   }
   ```

> Adding a brand-new `.proto` but forgetting the `build.rs` line (or using the
> wrong `package` string in `include_proto!`) is the most common failure — the
> generated module silently won't exist.

## Step 2 — Entity layer (raw SQL)

For each new data operation, add an entity processor in
`entities/db/<topic>.rs`. SQL lives **only** here.

```rust
#[derive(Debug, Clone)]
pub struct ChangePassword { pub id: Uuid, pub password_hash: String }

impl Processor<ChangePassword> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;
    #[instrument(skip_all, name = "SQL:ChangePassword", err, fields(id = %input.id))]
    async fn process(&self, input: ChangePassword) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"UPDATE auth.account SET password = $2 WHERE id = $1"#,
            input.id, input.password_hash,
        ).execute(self.db()).await?;
        Ok(())
    }
}
```

- Use `self.db()` (the `&PgPool` accessor on `DatabaseProcessor`).
- Span name prefixed `SQL:` (`SQL-Transaction:` if it opens a `begin()`/`commit()`).
- Re-export from the parent `mod.rs` if you created a new file.

**Any new or changed `sqlx::query!` macro invocation requires re-running
`cargo sqlx prepare`** — see Step 5.

## Step 3 — Service layer (business logic)

```rust
#[derive(Debug, Clone)]
pub struct ChangePasswordService { pub db: DatabaseProcessor }

#[derive(Debug, Clone)]
pub struct ChangePasswordRequest {
    pub user_id: Uuid,
    pub current_password: String,
    pub new_password: String,
}

// Prefer an explicit enum over a bare bool when an operation has named outcomes.
#[derive(Debug, Clone)]
pub enum ChangePasswordResult { Success, InvalidCurrentPassword }

impl Processor<ChangePasswordRequest> for ChangePasswordService {
    type Output = ChangePasswordResult;
    type Error = Error; // wakuwaku::error::Error
    #[instrument(skip_all, name = "ChangePassword", err, fields(user_id = %input.user_id))]
    async fn process(&self, input: ChangePasswordRequest) -> Result<Self::Output, Self::Error> {
        let account = self.db.process(FindAccountById { id: input.user_id })
            .await?
            .ok_or(Error::NotFound)?;
        if !verify_password(&input.current_password, &account.password) {
            return Ok(ChangePasswordResult::InvalidCurrentPassword);
        }
        let password_hash = hash_password(&input.new_password)
            .map_err(|e| Error::BusinessPanic(anyhow::anyhow!(e.to_string())))?;
        self.db.process(ChangePassword { id: account.id, password_hash }).await?;
        Ok(ChangePasswordResult::Success)
    }
}
```

- `sqlx::Error` auto-converts into `wakuwaku::Error` via `#[from]`, so `?` on
  `self.db.process(...)` "just works".
- Use the semantic variants (`Error::NotFound`, `Error::PermissionsDenied`,
  `Error::InvalidInput`) for expected business failures, `Error::BusinessPanic`
  for unexpected non-retryable ones, `Error::Io` for retryable ones.

## Step 4 — gRPC (rpc) layer (thin adapter)

```rust
use crate::proto::account as pb;
use crate::proto::account::account_server::Account;

#[derive(Debug, Clone)]
pub struct AccountGrpcService { /* services held by value */ }

#[tonic::async_trait]
impl Account for AccountGrpcService {
    async fn change_password(
        &self,
        request: Request<pb::ChangePasswordRequest>,
    ) -> Result<Response<pb::ChangePasswordResponse>, Status> {
        let caller = caller(&request)?;            // resolve identity from middleware
        let pb::ChangePasswordRequest { current_password, new_password } = request.into_inner();
        let result = self.change_password_service
            .process(ChangePasswordRequest { user_id: caller.user_id, current_password, new_password })
            .await?;                               // wakuwaku::Error → Status via From
        let success = matches!(result, ChangePasswordResult::Success);
        Ok(Response::new(pb::ChangePasswordResponse { success }))
    }
}
```

- Handlers stay **thin**: resolve caller → destructure request → call
  `service.process(dto).await?` → map outcome → `Response::new(...)`.
- `wakuwaku::Error` already implements `From<Error> for tonic::Status`
  (see embedded definition below), so `?` converts service errors to the right
  gRPC status automatically — do **not** hand-map every error.
- Convert at the boundary only: proto enums/strings ⇄ domain types (e.g. parse a
  `String` UUID; map `pb::AccountRole` ⇄ `AccountRole`).
- Register a new rpc file in `rpc/mod.rs` (`pub mod account;`).

## Step 5 — Re-run `cargo sqlx prepare`

The project builds **offline** against the checked-in `.sqlx/` query cache. Any
new/changed `sqlx::query!`/`query_as!` invocation must be re-prepared, otherwise
an offline build fails with "no cached data for this query".

**Assume the database is always ready** (a configured Postgres reachable via the
`DATABASE_URL` in `.env`). Do **not** add readiness checks. Run:

```bash
cargo sqlx prepare --workspace
```

- On success it writes/updates JSON files under `.sqlx/`; commit them.
- Then confirm an offline build still works:

  ```bash
  SQLX_OFFLINE=true cargo build -p <crate>
  ```

- **If `cargo sqlx prepare` fails with an unexpected database error** (cannot
  connect, auth failure, missing schema/migrations, etc.), **abort the task**
  and tell the user to check that the database is correctly configured (running,
  `DATABASE_URL` correct, migrations applied). Do not try to spin up or
  reconfigure the database yourself.
- If `cargo sqlx prepare` is unavailable (`sqlx-cli` not installed), install it
  with `cargo install sqlx-cli --no-default-features --features rustls,postgres`
  or report the gap — do not fake the cache files by hand.

## Step 6 — Verify

```bash
cargo build -p <crate>
SQLX_OFFLINE=true cargo build -p <crate>   # proves the .sqlx cache is current
cargo clippy -p <crate>                    # forbid(unwrap_used/expect_used/panic) — bubble with ?
```

Never submit with a failing build, stale `.sqlx` cache, or `unwrap`/`expect`/
`panic` added to satisfy the borrow checker.

## `wakuwaku::error::Error` (embedded reference)

Copied verbatim so you do **not** need to open the crate source. The variants
and the `tonic::Status` mapping below are the contract the service and rpc
layers rely on.

```rust
/// Unified error type used across crate modules.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    /// Serialize Error by kanau
    SerializeError(#[from] kanau::message::SerializeError),

    #[error("{0}")]
    /// Deserialize Error
    DeserializeError(#[from] kanau::message::DeserializeError),

    #[cfg(feature = "amqprs")]
    /// AMQP Error
    #[error("{0}")]
    AmqpError(#[from] amqprs::error::Error),

    #[cfg(feature = "redis")]
    /// Redis Error
    #[error("{0}")]
    RedisError(#[from] redis::RedisError),

    #[cfg(feature = "sqlx")]
    #[error("{0}")]
    /// Database Error
    DatabaseError(#[from] sqlx::Error),

    #[error("{0}")]
    /// Error occurred in business logic. This kind of business error can not be solved by retrying.
    BusinessPanic(anyhow::Error),

    #[error("{0}")]
    /// IO Error occurred in business logic. This kind of error can be solved by just retrying.
    Io(anyhow::Error),

    #[error("Permission is not enough")]
    /// Trying to do some operation that requires higher permission
    PermissionsDenied,

    #[error("Invalid input")]
    /// Input payload or arguments are invalid.
    InvalidInput,

    #[error("Trying to access a resource that does not exist")]
    /// Requested resource does not exist.
    NotFound,
}

impl From<&Error> for tonic::Status {
    fn from(value: &Error) -> Self {
        // ...
    }
}

#[cfg(feature = "tonic")]
impl From<Error> for tonic::Status {
    fn from(value: Error) -> Self {
        (&value).into()
    }
}
```

How to use it:

- Import as `use wakuwaku::error::Error;` (or `use wakuwaku::Error;`) and set
  `type Error = Error;` on every **service**/**hook** processor.
- Entity processors keep `type Error = sqlx::Error;` — the `#[from]` on
  `DatabaseError` lifts it into `wakuwaku::Error` when a service uses `?`.
- In rpc handlers, `?` on a `wakuwaku::Error` becomes a `tonic::Status` via the
  `From` impls above — pick service-layer variants so the resulting status code
  is correct (`NotFound` → `not_found`, `PermissionsDenied` → `permission_denied`,
  `InvalidInput`/serialize → `invalid_argument`, everything else → `internal`).
- Choosing the right variant: expected "no such row / not yours" → `NotFound`;
  caller lacks rights → `PermissionsDenied`; bad/garbled input → `InvalidInput`;
  unexpected unrecoverable internal fault → `BusinessPanic`; transient,
  retry-able fault → `Io`.

> This block is documentation only and is never compiled, so it cannot cause a
> compile error on its own. If the live `wakuwaku` crate has drifted from this
> copy (a variant renamed/removed, a feature gate changed) the code you write
> against it may fail to compile — in that case warn the user that this embedded
> reference is out of date and reconcile against the actual crate source.

## Definition of done

- [ ] `.proto` updated; file registered in `build.rs`; `proto.rs` exposes the
      module with the matching `package` string.
- [ ] Entity processors hold all raw SQL; `type Error = sqlx::Error`; spans
      prefixed `SQL:`.
- [ ] Services compose entities via `self.db.process(...)`; `type Error =
      wakuwaku::Error`; no raw SQL; named-outcome enums where useful.
- [ ] gRPC handlers are thin, resolve the caller, map proto ⇄ domain, and rely
      on the `wakuwaku::Error → tonic::Status` conversion.
- [ ] `cargo sqlx prepare --workspace` re-run; `.sqlx/` changes committed
      (aborted with a DB-config warning if it failed for DB reasons).
- [ ] `cargo build`, `SQLX_OFFLINE=true cargo build`, and `cargo clippy` all
      pass for the crate; no `unwrap`/`expect`/`panic`.

## Further reading

- Single-processor shape & layer decision table: **`add-processor-function`** skill.
- Longer worked examples (entity, service, rpc, proto, error mapping):
  [reference.md](reference.md).
