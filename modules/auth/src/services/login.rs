use crate::entities::db::accounts::FindUserByUsername;
use crate::services::session::{CreateSessionRequest, SessionService};
use crate::utils::password::verify_password;
use kanau::processor::Processor;
use tracing::instrument;
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
