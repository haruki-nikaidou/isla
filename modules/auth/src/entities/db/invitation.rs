use crate::entities::db::accounts::AccountRole;
use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
/// Invitation for new members to join the team.
///
/// - Schema: `auth`
/// - Table Name: `invitation`
pub struct InvitationEntity {
    /// Primary key
    pub token: Uuid,

    pub created_at: PrimitiveDateTime,

    /// All invitation must have an expiry time, after which the token is invalid.
    pub expire_at: PrimitiveDateTime,

    /// The number of user can accept this invitation. If `None`, there is no limit.
    pub max_accept_count: Option<i64>,

    /// The role assigned to user after registration
    pub role: AccountRole,

    /// Foreign key to [AccountEntity](super::accounts::AccountEntity)
    pub send_by: Uuid
}

/// Find an [`InvitationEntity`] by its primary-key token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindInvitationByToken {
    pub token: Uuid,
}

impl Processor<FindInvitationByToken> for DatabaseProcessor {
    type Output = Option<InvitationEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindInvitationByToken", err, fields(token = %input.token))]
    async fn process(
        &self,
        input: FindInvitationByToken,
    ) -> Result<Option<InvitationEntity>, sqlx::Error> {
        sqlx::query_as!(
            InvitationEntity,
            r#"
            SELECT
                token,
                created_at,
                expire_at,
                max_accept_count,
                role AS "role: AccountRole",
                send_by
            FROM auth.invitation
            WHERE token = $1
            LIMIT 1
            "#,
            input.token,
        )
        .fetch_optional(self.db())
        .await
    }
}
