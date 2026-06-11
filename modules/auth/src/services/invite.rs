use crate::entities::db::invitation::InvitationEntity;
use kanau::processor::Processor;
use time::PrimitiveDateTime;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
pub struct InviteService {
    pub db: DatabaseProcessor,
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

impl Processor<UserRegisterRequest> for InviteService {
    type Output = UserRegisterResponse;
    type Error = wakuwaku::Error;
    async fn process(&self, _input: UserRegisterRequest) -> Result<Self::Output, Self::Error> {
        todo!()
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
