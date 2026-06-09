use crate::config::AuthModuleConfig;
use crate::entities::db::session::{FindSessionById, TouchSession};
use kanau::processor::Processor;
use time::PrimitiveDateTime;
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
    Valid { user_id: Uuid, session_id: String },
}

impl Processor<SessionCheckRequest> for SessionService {
    type Output = SessionCheckResult;
    type Error = Error;
    async fn process(&self, input: SessionCheckRequest) -> Result<Self::Output, Self::Error> {
        let config_fut = self.db.process(FindConfig::<AuthModuleConfig>::new());
        let session_fut = self.db.process(FindSessionById {
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
                serial: session_entity.serial,
                expires: PrimitiveDateTime::new(new_expire.date(), new_expire.time()),
            })
            .await?;
        Ok(SessionCheckResult::Valid {
            user_id: session_entity.user_id,
            session_id: session_entity.session_id,
        })
    }
}
