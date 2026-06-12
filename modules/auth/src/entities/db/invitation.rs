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
    pub send_by: Uuid,
}

#[derive(Debug, Clone)]
/// Invitation relation between two accounts
///
/// - Schema: `auth`
/// - Table Name: `invitation_relation`
pub struct InvitationRelationEntity {
    pub id: i64,

    /// The token of [InvitationEntity]
    pub invite_via: Uuid,

    /// The user registered by this invitation, foreign key to [AccountEntity](super::accounts::AccountEntity)
    pub invitee: Uuid,

    /// The time when the invitation is accepted
    pub accepted_at: PrimitiveDateTime,
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

#[derive(Debug, Clone)]
pub struct ListInvitationsByUser {
    pub user_id: Uuid,
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListInvitationsByUser> for DatabaseProcessor {
    type Output = Vec<InvitationEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListInvitationsByUser", err)]
    async fn process(&self, input: ListInvitationsByUser) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            InvitationEntity,
            r#"
            SELECT
                token,
                created_at,
                expire_at,
                max_accept_count,
                role AS  "role: AccountRole",
                send_by
            FROM auth.invitation
            WHERE send_by = $1
            LIMIT $2
            OFFSET $3
            "#,
            input.user_id,
            input.limit,
            input.offset
        )
        .fetch_all(self.db())
        .await
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CountInvitationUsedTimes {
    pub pk: Uuid,
}

#[derive(Debug, Clone, Copy)]
pub struct InvitationUsedTimes {
    pub pk: Uuid,
    pub current: i64,
    pub limit: Option<i64>,
}

impl Processor<CountInvitationUsedTimes> for DatabaseProcessor {
    type Output = InvitationUsedTimes;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CountInvitationUsedTimes", err, fields(token = %input.pk))]
    async fn process(&self, input: CountInvitationUsedTimes) -> Result<Self::Output, Self::Error> {
        let row = sqlx::query!(
            r#"
            SELECT
                i.max_accept_count AS "limit",
                COUNT(r.id) AS "current!"
            FROM auth.invitation AS i
            LEFT JOIN auth.invitation_relation AS r ON r.invite_via = i.token
            WHERE i.token = $1
            GROUP BY i.max_accept_count
            "#,
            input.pk,
        )
        .fetch_one(self.db())
        .await?;

        Ok(InvitationUsedTimes {
            pk: input.pk,
            current: row.current,
            limit: row.limit,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateInvitation {
    pub user_id: Uuid,
    pub expire_at: PrimitiveDateTime,
    pub max_accept_account: Option<i64>,
    pub role: AccountRole,
}

impl Processor<CreateInvitation> for DatabaseProcessor {
    type Output = InvitationEntity;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateInvitation", err)]
    async fn process(&self, input: CreateInvitation) -> Result<Self::Output, Self::Error> {
        sqlx::query_as!(
            InvitationEntity,
            r#"
            INSERT INTO auth.invitation (token, expire_at, max_accept_count, role, send_by)
            VALUES (gen_random_uuid(), $1, $2, $3, $4)
            RETURNING
                token,
                created_at,
                expire_at,
                max_accept_count,
                role AS "role: AccountRole",
                send_by
            "#,
            input.expire_at,
            input.max_accept_account,
            input.role as AccountRole,
            input.user_id,
        )
        .fetch_one(self.db())
        .await
    }
}

#[derive(Debug, Clone, Copy)]
/// Invalid invitation by setting `max_accept_account` to 0.
pub struct InvalidInvitation {
    pub pk: Uuid,
}

impl Processor<InvalidInvitation> for DatabaseProcessor {
    type Output = ();
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:InvalidInvitation", err, fields(token = %input.pk))]
    async fn process(&self, input: InvalidInvitation) -> Result<Self::Output, Self::Error> {
        sqlx::query!(
            r#"
            UPDATE auth.invitation
            SET max_accept_count = 0
            WHERE token = $1
            "#,
            input.pk,
        )
        .execute(self.db())
        .await?;

        Ok(())
    }
}
