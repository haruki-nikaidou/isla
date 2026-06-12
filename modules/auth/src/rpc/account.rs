//! gRPC surface for authenticated users managing their own account.
//!
//! These endpoints let a logged-in user manage their sessions, the invitations
//! they have issued, and their password. The caller is resolved from the
//! session middleware (see [`AuthLayer`](crate::rpc::middleware::AuthLayer)),
//! which injects a [`UserSessionInfo`] into the request extensions. Wrap
//! [`AccountGrpcService`] in
//! [`AccountServer`](crate::proto::account::account_server::AccountServer) and
//! mount it behind the auth middleware on a `tonic` router.

use crate::entities::db::accounts::AccountRole;
use crate::proto::account as pb;
use crate::proto::account::account_server::Account;
use crate::rpc::middleware::UserSessionInfo;
use crate::services::invite::{
    CreateInviteRequest, InvalidateInviteRequest, InvalidateInviteResult, InviteService,
    ListUserInvitesRequest,
};
use crate::services::login::{ChangePasswordRequest, ChangePasswordResult, ChangePasswordService};
use crate::services::session::{
    ListSessionsOnUserRequest, LogoutRequest, SessionService, TerminateUserSessionRequest,
};
use kanau::processor::Processor;
use time::{Duration, OffsetDateTime, PrimitiveDateTime};
use tonic::{Request, Response, Status};
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

/// gRPC service exposing the account-management endpoints to authenticated
/// first-party user clients (`webui`, `dashboard`).
#[derive(Debug, Clone)]
pub struct AccountGrpcService {
    session_service: SessionService,
    invite_service: InviteService,
    change_password_service: ChangePasswordService,
}

impl AccountGrpcService {
    /// Build the service from a database handle, wiring up the underlying
    /// business services.
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

/// Extract the authenticated caller injected by the session middleware.
fn caller<T>(request: &Request<T>) -> Result<UserSessionInfo, Status> {
    request
        .extensions()
        .get::<UserSessionInfo>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("missing or invalid session"))
}

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

#[tonic::async_trait]
impl Account for AccountGrpcService {
    async fn logout(
        &self,
        request: Request<pb::LogoutRequest>,
    ) -> Result<Response<pb::LogoutResponse>, Status> {
        let caller = caller(&request)?;
        let success = self
            .session_service
            .process(LogoutRequest {
                session_id: caller.session_id,
            })
            .await?;
        Ok(Response::new(pb::LogoutResponse { success }))
    }

    async fn list_sessions(
        &self,
        request: Request<pb::ListSessionsRequest>,
    ) -> Result<Response<pb::ListSessionsResponse>, Status> {
        let caller = caller(&request)?;
        let sessions = self
            .session_service
            .process(ListSessionsOnUserRequest {
                user_id: caller.user_id,
            })
            .await?;
        let sessions = sessions
            .into_iter()
            .map(|session| pb::SessionInfo {
                serial: session.serial,
                last_refreshed: session.last_refreshed.to_string(),
                expires: session.expires.to_string(),
                current: session.session_id == caller.session_id,
            })
            .collect();
        Ok(Response::new(pb::ListSessionsResponse { sessions }))
    }

    async fn terminate_session(
        &self,
        request: Request<pb::TerminateSessionRequest>,
    ) -> Result<Response<pb::TerminateSessionResponse>, Status> {
        let caller = caller(&request)?;
        let pb::TerminateSessionRequest { serial } = request.into_inner();
        let success = self
            .session_service
            .process(TerminateUserSessionRequest {
                user_id: caller.user_id,
                serial,
            })
            .await?;
        Ok(Response::new(pb::TerminateSessionResponse { success }))
    }

    async fn send_invite(
        &self,
        request: Request<pb::SendInviteRequest>,
    ) -> Result<Response<pb::SendInviteResponse>, Status> {
        let caller = caller(&request)?;
        let pb::SendInviteRequest {
            role,
            expire_in_seconds,
            max_use_count,
        } = request.into_inner();
        let expire = OffsetDateTime::now_utc() + Duration::seconds(expire_in_seconds);
        let expire_at = PrimitiveDateTime::new(expire.date(), expire.time());
        let invite = self
            .invite_service
            .process(CreateInviteRequest {
                user_id: caller.user_id,
                expire_at,
                max_use_count,
                role: proto_to_role(role),
            })
            .await?;
        Ok(Response::new(pb::SendInviteResponse {
            token: invite.token.to_string(),
        }))
    }

    async fn list_invites(
        &self,
        request: Request<pb::ListInvitesRequest>,
    ) -> Result<Response<pb::ListInvitesResponse>, Status> {
        let caller = caller(&request)?;
        let pb::ListInvitesRequest { limit, offset } = request.into_inner();
        let invites = self
            .invite_service
            .process(ListUserInvitesRequest {
                user_id: caller.user_id,
                offset,
                limit,
            })
            .await?;
        let invites = invites
            .into_iter()
            .map(|invite| pb::InviteInfo {
                token: invite.token.to_string(),
                created_at: invite.created_at.to_string(),
                expire_at: invite.expire_at.to_string(),
                max_accept_count: invite.max_accept_count,
                role: role_to_proto(invite.role) as i32,
            })
            .collect();
        Ok(Response::new(pb::ListInvitesResponse { invites }))
    }

    async fn invalidate_invite(
        &self,
        request: Request<pb::InvalidateInviteRequest>,
    ) -> Result<Response<pb::InvalidateInviteResponse>, Status> {
        let caller = caller(&request)?;
        let pb::InvalidateInviteRequest { token } = request.into_inner();
        let Ok(token) = Uuid::parse_str(&token) else {
            return Ok(Response::new(pb::InvalidateInviteResponse { success: false }));
        };
        let result = self
            .invite_service
            .process(InvalidateInviteRequest {
                user_id: caller.user_id,
                token,
            })
            .await?;
        let success = matches!(result, InvalidateInviteResult::Success);
        Ok(Response::new(pb::InvalidateInviteResponse { success }))
    }

    async fn change_password(
        &self,
        request: Request<pb::ChangePasswordRequest>,
    ) -> Result<Response<pb::ChangePasswordResponse>, Status> {
        let caller = caller(&request)?;
        let pb::ChangePasswordRequest {
            current_password,
            new_password,
        } = request.into_inner();
        let result = self
            .change_password_service
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
