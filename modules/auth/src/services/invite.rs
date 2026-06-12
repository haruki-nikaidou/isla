use crate::entities::db::accounts::{AccountRole, FindAccountById, RegisterUserViaInvite};
use crate::entities::db::invitation::{
    CountInvitationUsedTimes, CreateInvitation, FindInvitationByToken, InvalidInvitation,
    InvitationEntity, InvitationUsedTimes, ListInvitationsByUser,
};
use crate::services::session::{CreateSessionRequest, SessionService};
use kanau::processor::Processor;
use time::{OffsetDateTime, PrimitiveDateTime};
use tracing::instrument;
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
    #[instrument(skip_all, name = "ListUserInvites", err, fields(user_id = %input.user_id))]
    async fn process(&self, input: ListUserInvitesRequest) -> Result<Self::Output, Self::Error> {
        let ListUserInvitesRequest {
            user_id,
            offset,
            limit,
        } = input;
        let invites = self
            .db
            .process(ListInvitationsByUser {
                user_id,
                limit,
                offset,
            })
            .await?;
        Ok(invites)
    }
}

/// Invalidate an invitation issued by the requesting user so that it can no
/// longer be used to register.
#[derive(Debug, Clone, Copy)]
pub struct InvalidateInviteRequest {
    pub user_id: Uuid,
    pub token: Uuid,
}

#[derive(Debug, Clone, Copy)]
pub enum InvalidateInviteResult {
    Success,
    NotFound,
}

impl Processor<InvalidateInviteRequest> for InviteService {
    type Output = InvalidateInviteResult;
    type Error = wakuwaku::Error;
    #[instrument(skip_all, name = "InvalidateInvite", err, fields(user_id = %input.user_id, token = %input.token))]
    async fn process(&self, input: InvalidateInviteRequest) -> Result<Self::Output, Self::Error> {
        let Some(invite) = self
            .db
            .process(FindInvitationByToken { token: input.token })
            .await?
        else {
            return Ok(InvalidateInviteResult::NotFound);
        };
        if invite.send_by != input.user_id {
            return Ok(InvalidateInviteResult::NotFound);
        }
        self.db
            .process(InvalidInvitation { pk: input.token })
            .await?;
        Ok(InvalidateInviteResult::Success)
    }
}

#[derive(Debug, Clone)]
pub struct CheckInviteTokenRequest {
    pub token: Uuid,
}

impl Processor<CheckInviteTokenRequest> for InviteService {
    type Output = Option<InvitationEntity>;
    type Error = wakuwaku::Error;
    #[instrument(skip_all, name = "CheckInviteToken", err, fields(token = %input.token))]
    async fn process(&self, input: CheckInviteTokenRequest) -> Result<Self::Output, Self::Error> {
        let Some(invite_entity) = self
            .db
            .process(FindInvitationByToken { token: input.token })
            .await?
        else {
            return Ok(None);
        };
        let inv_count = self
            .db
            .process(CountInvitationUsedTimes {
                pk: invite_entity.token,
            })
            .await?;
        if check_invite(&invite_entity, inv_count) {
            Ok(Some(invite_entity))
        } else {
            Ok(None)
        }
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
            let token = self
                .session_service
                .process(CreateSessionRequest { user_id: user.id })
                .await?;
            Ok(UserRegisterResponse::Login { token })
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
    /// The role assigned to the invitee after registration.
    pub role: AccountRole,
}

impl Processor<CreateInviteRequest> for InviteService {
    type Output = InvitationEntity;
    type Error = wakuwaku::Error;
    #[instrument(skip_all, name = "CreateInvite", err, fields(user_id = %input.user_id))]
    async fn process(&self, input: CreateInviteRequest) -> Result<Self::Output, Self::Error> {
        // load the inviting account to determine its privileges
        let account = self
            .db
            .process(FindAccountById {
                id: input.user_id,
            })
            .await?
            .ok_or(wakuwaku::Error::NotFound)?;

        // a member cannot grant a role higher than its own; only an owner may
        // create an invitation that makes the invitee an owner
        if input.role == AccountRole::Owner && account.role != AccountRole::Owner {
            return Err(wakuwaku::Error::PermissionsDenied);
        }

        self.db
            .process(CreateInvitation {
                user_id: input.user_id,
                expire_at: input.expire_at,
                max_accept_account: input.max_use_count,
                role: input.role,
            })
            .await
            .map_err(wakuwaku::Error::from)
    }
}
