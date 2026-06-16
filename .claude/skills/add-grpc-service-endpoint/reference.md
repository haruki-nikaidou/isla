# Reference — worked examples for a gRPC → service → entity slice

These are end-to-end examples for the `auth` module's `account` service, the
canonical instance of this pattern. Adapt names/module/package to your task.

## §1 Proto contract (`modules/auth/proto/account.proto`)

```proto
syntax = "proto3";

package isla.auth.account; // ← must match tonic::include_proto! in proto.rs

// Every call is authenticated through the session middleware, which resolves
// the caller from the `x-session-id` header. Requests do not carry identity.
service Account {
  rpc Logout(LogoutRequest) returns (LogoutResponse);
  rpc ListSessions(ListSessionsRequest) returns (ListSessionsResponse);
  rpc TerminateSession(TerminateSessionRequest) returns (TerminateSessionResponse);
  rpc ChangePassword(ChangePasswordRequest) returns (ChangePasswordResponse);
}

message LogoutRequest {}
message LogoutResponse { bool success = 1; }

message ChangePasswordRequest {
  string current_password = 1;
  string new_password = 2;
}
message ChangePasswordResponse { bool success = 1; }
```

Wiring (do all three):

```rust
// build.rs
tonic_prost_build::compile_protos("proto/account.proto")?;

// src/proto.rs
pub mod account {
    #![allow(clippy::all)]
    #![allow(clippy::pedantic)]
    tonic::include_proto!("isla.auth.account");
}
```

## §2 Entity processor — simple `Find` (`entities/db/accounts.rs`)

```rust
use kanau::processor::Processor;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone, Copy)]
pub struct FindAccountById { pub id: Uuid }

impl Processor<FindAccountById> for DatabaseProcessor {
    type Output = Option<AccountEntity>;
    type Error = sqlx::Error;
    #[instrument(skip_all, name = "SQL:FindAccountById", err, fields(id = %input.id))]
    async fn process(&self, input: FindAccountById) -> Result<Self::Output, sqlx::Error> {
        let row = sqlx::query_as!(
            AccountEntity,
            r#"SELECT id, username, password, role AS "role: AccountRole"
               FROM auth.account WHERE id = $1"#,
            input.id,
        )
        .fetch_optional(self.db())
        .await?;
        Ok(row)
    }
}
```

## §3 Entity processor — write (`UPDATE`)

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
        )
        .execute(self.db())
        .await?;
        Ok(())
    }
}
```

> Both §2 and §3 use the `sqlx::query!`/`query_as!` macros, so adding them means
> `cargo sqlx prepare --workspace` must be re-run (SKILL.md Step 5).

## §4 Service with several operations on one struct

A service struct may hold sibling services to compose cross-cutting flows
(here `InviteService` holds a `SessionService`). Each operation is its own
`Processor` impl with its own DTO — never a bespoke `pub async fn`.

```rust
#[derive(Debug, Clone)]
pub struct SessionService { pub db: DatabaseProcessor }

#[derive(Debug, Clone)]
pub struct LogoutRequest { pub session_id: String }

impl Processor<LogoutRequest> for SessionService {
    type Output = bool; // true when a session existed and was terminated
    type Error = wakuwaku::Error;
    #[instrument(skip_all, name = "Logout", err)]
    async fn process(&self, input: LogoutRequest) -> Result<bool, wakuwaku::Error> {
        let Some(session) = self.db
            .process(FindSessionById { session_id: input.session_id })
            .await?
        else { return Ok(false); };
        self.db.process(TerminateSession { session_serial: session.serial }).await?;
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub struct TerminateUserSessionRequest { pub user_id: Uuid, pub serial: i64 }

impl Processor<TerminateUserSessionRequest> for SessionService {
    type Output = bool;
    type Error = wakuwaku::Error;
    #[instrument(skip_all, name = "TerminateUserSession", err,
                 fields(user_id = %input.user_id, serial = input.serial))]
    async fn process(&self, input: TerminateUserSessionRequest) -> Result<bool, wakuwaku::Error> {
        let Some(session) = self.db
            .process(FindSessionBySerial { serial: input.serial })
            .await?
        else { return Ok(false); };
        if session.user_id != input.user_id { return Ok(false); } // ownership check
        self.db.process(TerminateSession { session_serial: session.serial }).await?;
        Ok(true)
    }
}
```

Returning a named enum instead of a bare `bool` when outcomes carry meaning:

```rust
#[derive(Debug, Clone, Copy)]
pub enum InvalidateInviteResult { Success, NotFound }

impl Processor<InvalidateInviteRequest> for InviteService {
    type Output = InvalidateInviteResult;
    type Error = wakuwaku::Error;
    #[instrument(skip_all, name = "InvalidateInvite", err,
                 fields(user_id = %input.user_id, token = %input.token))]
    async fn process(&self, input: InvalidateInviteRequest) -> Result<Self::Output, Self::Error> {
        let Some(invite) = self.db
            .process(FindInvitationByToken { token: input.token })
            .await?
        else { return Ok(InvalidateInviteResult::NotFound); };
        if invite.send_by != input.user_id {
            return Ok(InvalidateInviteResult::NotFound); // not yours == not found
        }
        self.db.process(InvalidInvitation { pk: input.token }).await?;
        Ok(InvalidateInviteResult::Success)
    }
}
```

## §5 gRPC service — assembly + caller resolution + handlers

```rust
use crate::proto::account as pb;
use crate::proto::account::account_server::Account;
use crate::rpc::middleware::UserSessionInfo;
use kanau::processor::Processor;
use tonic::{Request, Response, Status};

#[derive(Debug, Clone)]
pub struct AccountGrpcService {
    session_service: SessionService,
    invite_service: InviteService,
    change_password_service: ChangePasswordService,
}

impl AccountGrpcService {
    pub fn new(db: DatabaseProcessor) -> Self {
        let session_service = SessionService { db: db.clone() };
        Self {
            invite_service: InviteService {
                db: db.clone(),
                session_service: session_service.clone(),
            },
            change_password_service: ChangePasswordService { db },
            session_service,
        }
    }
}

/// Pull the authenticated caller injected by the auth middleware.
fn caller<T>(request: &Request<T>) -> Result<UserSessionInfo, Status> {
    request
        .extensions()
        .get::<UserSessionInfo>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("missing or invalid session"))
}

#[tonic::async_trait]
impl Account for AccountGrpcService {
    async fn logout(
        &self,
        request: Request<pb::LogoutRequest>,
    ) -> Result<Response<pb::LogoutResponse>, Status> {
        let caller = caller(&request)?;
        let success = self.session_service
            .process(LogoutRequest { session_id: caller.session_id })
            .await?; // wakuwaku::Error -> Status via From
        Ok(Response::new(pb::LogoutResponse { success }))
    }

    async fn change_password(
        &self,
        request: Request<pb::ChangePasswordRequest>,
    ) -> Result<Response<pb::ChangePasswordResponse>, Status> {
        let caller = caller(&request)?;
        let pb::ChangePasswordRequest { current_password, new_password } = request.into_inner();
        let result = self.change_password_service
            .process(ChangePasswordRequest {
                user_id: caller.user_id,
                current_password,
                new_password,
            })
            .await?;
        let success = matches!(result, ChangePasswordResult::Success);
        Ok(Response::new(pb::ChangePasswordResponse { success }))
    }
}
```

Register the module in `rpc/mod.rs`:

```rust
pub mod account;
pub mod manage;
pub mod middleware;
pub mod preauth;
```

## §6 Boundary type conversions

Keep proto ⇄ domain mapping in the rpc layer, in small free functions:

```rust
fn role_to_proto(role: AccountRole) -> pb::AccountRole {
    match role {
        AccountRole::Owner => pb::AccountRole::Owner,
        AccountRole::Member => pb::AccountRole::Member,
    }
}

fn proto_to_role(role: i32) -> AccountRole {
    match pb::AccountRole::try_from(role).unwrap_or(pb::AccountRole::Unspecified) {
        pb::AccountRole::Owner => AccountRole::Owner,
        pb::AccountRole::Member | pb::AccountRole::Unspecified => AccountRole::Member,
    }
}
```

Parse user-supplied strings (e.g. UUIDs) at the boundary and translate failures
into a sensible response/`Status` rather than propagating a parse error:

```rust
let Ok(token) = Uuid::parse_str(&token) else {
    return Ok(Response::new(pb::InvalidateInviteResponse { success: false }));
};
```

## §7 sqlx prepare — expected output & failure handling

```bash
$ cargo sqlx prepare --workspace
   Compiling ...
query data written to .sqlx in the current directory; please check this into version control
$ git status --short .sqlx
 D .sqlx/query-<oldhash>.json
?? .sqlx/query-<newhash1>.json
?? .sqlx/query-<newhash2>.json
$ SQLX_OFFLINE=true cargo build -p auth
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

If instead `cargo sqlx prepare` errors with something like
`error communicating with database` / `Connection refused` / `password
authentication failed` / `relation "auth.account" does not exist`, **stop**:
the database is not configured as this skill assumes. Report to the user and ask
them to verify the database is running, `DATABASE_URL` is correct, and
migrations have been applied — do not attempt to provision it yourself.
