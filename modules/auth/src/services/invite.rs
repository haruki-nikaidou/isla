use crate::entities::db::accounts::RegisterUserViaInvite;
use crate::entities::db::invitation::{
    CountInvitationUsedTimes, FindInvitationByToken, InvitationEntity, InvitationUsedTimes,
};
use crate::services::session::SessionService;
use kanau::processor::Processor;
use time::{OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
pub struct InviteService {
    pub db: DatabaseProcessor,
    pub session_service: SessionService,
}

#[derive(Debug, Clone)]
pub struct ListUserInvitesRequest {
    pub user_id: Uuid,
    pub offset: i64,
    pub limit: i64,
}

impl Processor<ListUserInvitesRequest> for InviteService {
    type Output = Vec<InvitationEntity>;
    type Error = wakuwaku::Error;
    async fn process(&self, _input: ListUserInvitesRequest) -> Result<Self::Output, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct CheckInviteTokenRequest {
    pub token: Uuid,
}

impl Processor<CheckInviteTokenRequest> for InviteService {
    type Output = Option<InvitationEntity>;
    type Error = wakuwaku::Error;
    async fn process(&self, _input: CheckInviteTokenRequest) -> Result<Self::Output, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct UserRegisterRequest {
    pub username: String,
    pub password_plaintext: String,
    pub use_invite: Uuid,
    pub register_with_login: bool,
}

#[derive(Debug, Clone)]
pub enum UserRegisterResponse {
    Success,
    Login { token: String },
    InvalidInvite,
}

fn check_invite(invite: &InvitationEntity, invite_used_count: InvitationUsedTimes) -> bool {
    let now = OffsetDateTime::now_utc();
    let now = PrimitiveDateTime::new(now.date(), now.time());
    if now > invite.expire_at {
        return false;
    }
    if let Some(use_limit) = invite_used_count.limit
        && invite_used_count.current >= use_limit
    {
        return false;
    }
    true
}

impl Processor<UserRegisterRequest> for InviteService {
    type Output = UserRegisterResponse;
    type Error = wakuwaku::Error;
    async fn process(&self, input: UserRegisterRequest) -> Result<Self::Output, Self::Error> {
        // find and check the invitation
        let Some(invite_entity) = self
            .db
            .process(FindInvitationByToken {
                token: input.use_invite,
            })
            .await?
        else {
            return Ok(UserRegisterResponse::InvalidInvite);
        };
        let inv_count = self
            .db
            .process(CountInvitationUsedTimes {
                pk: invite_entity.token,
            })
            .await?;
        if !check_invite(&invite_entity, inv_count) {
            return Ok(UserRegisterResponse::InvalidInvite);
        }

        // register the user
        let hashed_password = crate::utils::password::hash_password(&input.password_plaintext)
            .map_err(|e| wakuwaku::Error::BusinessPanic(anyhow::anyhow!(e.to_string())))?;
        let user = self
            .db
            .process(RegisterUserViaInvite {
                username: input.username,
                password_hash: hashed_password,
                invite_token: input.use_invite,
            })
            .await?;

        // login the user if needed
        if input.register_with_login {
            todo!()
        } else {
            Ok(UserRegisterResponse::Success)
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateInviteRequest {
    pub user_id: Uuid,
    pub expire_at: PrimitiveDateTime,
    pub max_use_count: Option<i64>,
}

impl Processor<CreateInviteRequest> for InviteService {
    type Output = InvitationEntity;
    type Error = wakuwaku::Error;
    async fn process(&self, _input: CreateInviteRequest) -> Result<Self::Output, Self::Error> {
        todo!()
    }
}
