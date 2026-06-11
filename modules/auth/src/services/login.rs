use crate::config::AuthModuleConfig;
use crate::entities::db::accounts::FindUserByUsername;
use crate::entities::db::session::{CreateSession, generate_session_id};
use argon2::{Argon2, PasswordHash, PasswordVerifier};
use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use vault::entities::db::config::FindConfig;
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
        let account_fut = self.db.process(FindUserByUsername {
            username: input.username,
        });
        let config_fut = self.db.process(FindConfig::<AuthModuleConfig>::new());

        let (account, config) = tokio::try_join!(account_fut, config_fut)?;

        let Some(account) = account else {
            return Ok(LoginResult::InvalidCredentials);
        };

        let Ok(parsed_hash) = PasswordHash::new(&account.password) else {
            return Ok(LoginResult::InvalidCredentials);
        };

        if Argon2::default()
            .verify_password(input.password.as_bytes(), &parsed_hash)
            .is_err()
        {
            return Ok(LoginResult::InvalidCredentials);
        }

        let now = time::OffsetDateTime::now_utc();
        let expires = now + time::Duration::seconds(config.session.ttl as i64);

        let session_id = generate_session_id();

        self.db
            .process(CreateSession {
                user_id: account.id,
                session_id: session_id.clone(),
                created_at: PrimitiveDateTime::new(now.date(), now.time()),
                expires: PrimitiveDateTime::new(expires.date(), expires.time()),
            })
            .await?;

        Ok(LoginResult::Success {
            session_token: session_id,
        })
    }
}
