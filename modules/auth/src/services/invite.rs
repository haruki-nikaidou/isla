use crate::entities::db::invitation::InvitationEntity;
use kanau::processor::Processor;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
pub struct InviteService {
    pub db: DatabaseProcessor,
}

#[derive(Debug, Clone)]
pub struct ListUserInvites {
    pub user_id: Uuid,
}

impl Processor<ListUserInvites> for InviteService {
    type Output = Vec<InvitationEntity>;
    type Error = wakuwaku::Error;
    async fn process(&self, input: ListUserInvites) -> Result<Self::Output, Self::Error> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct CheckInviteToken {
    pub token: Uuid,
}

impl Processor<CheckInviteToken> for InviteService {
    type Output = Option<InvitationEntity>;
    type Error = wakuwaku::Error;
    async fn process(&self, input: CheckInviteToken) -> Result<Self::Output, Self::Error> {
        todo!()
    }
}
