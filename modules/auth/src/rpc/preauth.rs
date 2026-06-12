//! gRPC surface for unauthenticated users.
//!
//! These endpoints are reachable before a user holds a session: logging in,
//! validating an invitation token, and registering a new account. Wrap
//! [`PreauthGrpcService`] in [`PreauthServer`](crate::proto::preauth::preauth_server::PreauthServer)
//! to mount it on a `tonic` router.

use crate::entities::db::accounts::AccountRole;
use crate::proto::preauth as pb;
use crate::proto::preauth::preauth_server::Preauth;
use crate::services::invite::{
    CheckInviteTokenRequest, InviteService, UserRegisterRequest, UserRegisterResponse,
};
use crate::services::login::{LoginRequest, LoginResult, LoginService};
use crate::services::session::SessionService;
use kanau::processor::Processor;
use tonic::{Request, Response, Status};
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

/// gRPC service exposing the preauth endpoints to first-party user clients
/// (`webui`, `dashboard`).
#[derive(Debug, Clone)]
pub struct PreauthGrpcService {
    login_service: LoginService,
    invite_service: InviteService,
}

impl PreauthGrpcService {
    /// Build the service from a database handle, wiring up the underlying
    /// business services.
    pub fn new(db: DatabaseProcessor) -> Self {
        let session_service = SessionService { db: db.clone() };
        Self {
            login_service: LoginService { db: db.clone() },
            invite_service: InviteService {
                db,
                session_service,
            },
        }
    }
}

fn role_to_proto(role: AccountRole) -> pb::AccountRole {
    match role {
        AccountRole::Owner => pb::AccountRole::Owner,
        AccountRole::Member => pb::AccountRole::Member,
    }
}

#[tonic::async_trait]
impl Preauth for PreauthGrpcService {
    async fn login(
        &self,
        request: Request<pb::LoginRequest>,
    ) -> Result<Response<pb::LoginResponse>, Status> {
        let pb::LoginRequest { username, password } = request.into_inner();
        let result = self
            .login_service
            .process(LoginRequest { username, password })
            .await?;
        let response = match result {
            LoginResult::Success { session_token } => pb::LoginResponse {
                success: true,
                session_token: Some(session_token),
            },
            LoginResult::InvalidCredentials => pb::LoginResponse {
                success: false,
                session_token: None,
            },
        };
        Ok(Response::new(response))
    }

    async fn check_invite(
        &self,
        request: Request<pb::CheckInviteRequest>,
    ) -> Result<Response<pb::CheckInviteResponse>, Status> {
        let pb::CheckInviteRequest { token } = request.into_inner();
        let Ok(token) = Uuid::parse_str(&token) else {
            return Ok(Response::new(pb::CheckInviteResponse {
                valid: false,
                role: None,
            }));
        };
        let invite = self
            .invite_service
            .process(CheckInviteTokenRequest { token })
            .await?;
        let response = match invite {
            Some(invite) => pb::CheckInviteResponse {
                valid: true,
                role: Some(role_to_proto(invite.role) as i32),
            },
            None => pb::CheckInviteResponse {
                valid: false,
                role: None,
            },
        };
        Ok(Response::new(response))
    }

    async fn register(
        &self,
        request: Request<pb::RegisterRequest>,
    ) -> Result<Response<pb::RegisterResponse>, Status> {
        let pb::RegisterRequest {
            username,
            password,
            invite_token,
            login_after_register,
        } = request.into_inner();
        let Ok(use_invite) = Uuid::parse_str(&invite_token) else {
            return Ok(Response::new(pb::RegisterResponse {
                status: pb::RegisterStatus::InvalidInvite as i32,
                session_token: None,
            }));
        };
        let result = self
            .invite_service
            .process(UserRegisterRequest {
                username,
                password_plaintext: password,
                use_invite,
                register_with_login: login_after_register,
            })
            .await?;
        let response = match result {
            UserRegisterResponse::Success => pb::RegisterResponse {
                status: pb::RegisterStatus::Success as i32,
                session_token: None,
            },
            UserRegisterResponse::Login { token } => pb::RegisterResponse {
                status: pb::RegisterStatus::LoggedIn as i32,
                session_token: Some(token),
            },
            UserRegisterResponse::InvalidInvite => pb::RegisterResponse {
                status: pb::RegisterStatus::InvalidInvite as i32,
                session_token: None,
            },
        };
        Ok(Response::new(response))
    }
}
