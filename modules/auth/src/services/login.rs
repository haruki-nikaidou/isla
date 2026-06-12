use crate::entities::db::accounts::{ChangePassword, FindAccountById, FindUserByUsername};
use crate::services::session::{CreateSessionRequest, SessionService};
use crate::utils::password::{hash_password, verify_password};
use kanau::processor::Processor;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::error::Error;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
pub struct LoginService {
    pub db: DatabaseProcessor,
}

#[derive(Debug, Clone)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub enum LoginResult {
    Success { session_token: String },
    InvalidCredentials,
}

impl Processor<LoginRequest> for LoginService {
    type Output = LoginResult;
    type Error = Error;

    #[instrument(skip_all, name = "Login", err, fields(username = %input.username))]
    async fn process(&self, input: LoginRequest) -> Result<Self::Output, Self::Error> {
        let account = self
            .db
            .process(FindUserByUsername {
                username: input.username,
            })
            .await?;

        let Some(account) = account else {
            return Ok(LoginResult::InvalidCredentials);
        };

        if !verify_password(&input.password, &account.password) {
            return Ok(LoginResult::InvalidCredentials);
        }

        let session_service = SessionService {
            db: self.db.clone(),
        };
        let session_id = session_service
            .process(CreateSessionRequest {
                user_id: account.id,
            })
            .await?;

        Ok(LoginResult::Success {
            session_token: session_id,
        })
    }
}

/// Change the password of an account after verifying its current password.
#[derive(Debug, Clone)]
pub struct ChangePasswordService {
    pub db: DatabaseProcessor,
}

#[derive(Debug, Clone)]
pub struct ChangePasswordRequest {
    pub user_id: Uuid,
    pub current_password: String,
    pub new_password: String,
}

#[derive(Debug, Clone)]
pub enum ChangePasswordResult {
    Success,
    InvalidCurrentPassword,
}

impl Processor<ChangePasswordRequest> for ChangePasswordService {
    type Output = ChangePasswordResult;
    type Error = Error;

    #[instrument(skip_all, name = "ChangePassword", err, fields(user_id = %input.user_id))]
    async fn process(&self, input: ChangePasswordRequest) -> Result<Self::Output, Self::Error> {
        let account = self
            .db
            .process(FindAccountById { id: input.user_id })
            .await?
            .ok_or(Error::NotFound)?;

        if !verify_password(&input.current_password, &account.password) {
            return Ok(ChangePasswordResult::InvalidCurrentPassword);
        }

        let password_hash = hash_password(&input.new_password)
            .map_err(|e| Error::BusinessPanic(anyhow::anyhow!(e.to_string())))?;

        self.db
            .process(ChangePassword {
                id: account.id,
                password_hash,
            })
            .await?;

        Ok(ChangePasswordResult::Success)
    }
}
