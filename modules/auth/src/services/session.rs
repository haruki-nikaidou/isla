use crate::config::AuthModuleConfig;
use crate::entities::db::accounts::AccountRole;
use crate::entities::db::session::{
    CreateSession, FindSessionById, FindSessionBySerial, FindUserFromSession, ListUserSessions,
    SessionEntity, TerminateSession, TouchSession, generate_session_id,
};
use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use vault::entities::db::config::FindConfig;
use wakuwaku::error::Error;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
pub struct SessionService {
    pub db: DatabaseProcessor,
}

#[derive(Debug, Clone)]
pub struct SessionCheckRequest {
    pub token: String,
}

#[derive(Debug, Clone)]
pub enum SessionCheckResult {
    Invalid,
    Valid {
        user_id: Uuid,
        session_id: String,
        role: AccountRole,
    },
}

impl Processor<SessionCheckRequest> for SessionService {
    type Output = SessionCheckResult;
    type Error = Error;
    #[instrument(skip_all, name = "SessionCheck", err)]
    async fn process(&self, input: SessionCheckRequest) -> Result<Self::Output, Self::Error> {
        let config_fut = self.db.process(FindConfig::<AuthModuleConfig>::new());
        let session_fut = self.db.process(FindUserFromSession {
            session_id: input.token,
        });
        let (config, maybe_session_entity) = tokio::try_join!(config_fut, session_fut)?;
        let Some(session_entity) = maybe_session_entity else {
            return Ok(SessionCheckResult::Invalid);
        };
        let now = time::OffsetDateTime::now_utc();
        let new_expire = now + time::Duration::seconds(config.session.ttl as i64);
        self.db
            .process(TouchSession {
                serial: session_entity.session_serial,
                expires: PrimitiveDateTime::new(new_expire.date(), new_expire.time()),
            })
            .await?;
        Ok(SessionCheckResult::Valid {
            user_id: session_entity.user_id,
            session_id: session_entity.session_id,
            role: session_entity.user_role,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateSessionRequest {
    pub user_id: Uuid,
}

impl Processor<CreateSessionRequest> for SessionService {
    type Output = String;
    type Error = Error;
    #[instrument(skip_all, name = "CreateSession", err, fields(user_id = %input.user_id))]
    async fn process(&self, input: CreateSessionRequest) -> Result<Self::Output, Self::Error> {
        let config = self
            .db
            .process(FindConfig::<AuthModuleConfig>::new())
            .await?;

        let now = time::OffsetDateTime::now_utc();
        let expires = now + time::Duration::seconds(config.session.ttl as i64);

        let session_id = generate_session_id();

        self.db
            .process(CreateSession {
                user_id: input.user_id,
                session_id: session_id.clone(),
                created_at: PrimitiveDateTime::new(now.date(), now.time()),
                expires: PrimitiveDateTime::new(expires.date(), expires.time()),
            })
            .await?;

        Ok(session_id)
    }
}

#[derive(Debug, Clone)]
pub struct ListSessionsOnUserRequest {
    pub user_id: Uuid,
}

impl Processor<ListSessionsOnUserRequest> for SessionService {
    type Output = Vec<SessionEntity>;
    type Error = Error;
    #[instrument(skip_all, name = "ListSessionsOnUser", err, fields(user_id = %input.user_id))]
    async fn process(&self, input: ListSessionsOnUserRequest) -> Result<Self::Output, Self::Error> {
        let ListSessionsOnUserRequest { user_id } = input;
        let query_result = self.db.process(ListUserSessions { user_id }).await?;
        Ok(query_result)
    }
}

/// Terminate the session a request was made with, i.e. log the caller out.
#[derive(Debug, Clone)]
pub struct LogoutRequest {
    pub session_id: String,
}

impl Processor<LogoutRequest> for SessionService {
    /// `true` when a matching session existed and was terminated.
    type Output = bool;
    type Error = Error;
    #[instrument(skip_all, name = "Logout", err)]
    async fn process(&self, input: LogoutRequest) -> Result<Self::Output, Self::Error> {
        let Some(session) = self
            .db
            .process(FindSessionById {
                session_id: input.session_id,
            })
            .await?
        else {
            return Ok(false);
        };
        self.db
            .process(TerminateSession {
                session_serial: session.serial,
            })
            .await?;
        Ok(true)
    }
}

/// Terminate one of a user's sessions, addressed by its serial. The session
/// must belong to the requesting user.
#[derive(Debug, Clone)]
pub struct TerminateUserSessionRequest {
    pub user_id: Uuid,
    pub serial: i64,
}

impl Processor<TerminateUserSessionRequest> for SessionService {
    /// `true` when a matching session owned by the user was terminated.
    type Output = bool;
    type Error = Error;
    #[instrument(
        skip_all,
        name = "TerminateUserSession",
        err,
        fields(user_id = %input.user_id, serial = input.serial)
    )]
    async fn process(
        &self,
        input: TerminateUserSessionRequest,
    ) -> Result<Self::Output, Self::Error> {
        let Some(session) = self
            .db
            .process(FindSessionBySerial {
                serial: input.serial,
            })
            .await?
        else {
            return Ok(false);
        };
        if session.user_id != input.user_id {
            return Ok(false);
        }
        self.db
            .process(TerminateSession {
                session_serial: session.serial,
            })
            .await?;
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub struct TerminateSessionOnUserRequest {
    pub user_id: Uuid,
    pub session_id: String,
    pub current_session_id: String,
}

pub enum TerminateSessionResult {
    Success,
    NotFound,
    /// The current session is terminated so the user must log in again.
    ReLogin,
}

impl Processor<TerminateSessionOnUserRequest> for SessionService {
    type Output = TerminateSessionResult;
    type Error = Error;
    #[instrument(
        skip_all,
        name = "TerminateSessionOnUser",
        err,
        fields(user_id = %input.user_id, session_id = %input.session_id)
    )]
    async fn process(
        &self,
        input: TerminateSessionOnUserRequest,
    ) -> Result<Self::Output, Self::Error> {
        let to_terminate = self
            .db
            .process(FindSessionById {
                session_id: input.current_session_id.clone(),
            })
            .await?;
        if let Some(to_terminate) = to_terminate
            && to_terminate.user_id == input.user_id
        {
            self.db
                .process(TerminateSession {
                    session_serial: to_terminate.serial,
                })
                .await?;
            if to_terminate.session_id == input.current_session_id {
                Ok(TerminateSessionResult::ReLogin)
            } else {
                Ok(TerminateSessionResult::NotFound)
            }
        } else {
            Ok(TerminateSessionResult::NotFound)
        }
    }
}
