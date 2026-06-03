//! Platform-specific contact records.

use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

/// A contact as they appear on a specific platform.
///
/// The same person may have multiple `ContactEntity` records (one per platform),
/// but they share a single [`ContactIdentityEntity`](super::contact_identity::ContactIdentityEntity)
/// that represents the real-world identity Isla has recognized.
#[derive(Debug, Clone)]
pub struct ContactEntity {
    /// Unique identifier for this contact record.
    pub id: i64,

    /// The name shown for this contact on the platform.
    pub display_name: String,

    /// Platform-specific user identifier.
    pub user_id: String,

    /// Platform name (e.g., `discord`, `telegram`, `slack`).
    pub platform: String,

    /// Foreign key to the
    /// [`ContactIdentityEntity`](super::contact_identity::ContactIdentityEntity)
    /// this contact is linked to.
    pub identity: Uuid,

    /// When this contact was first seen.
    pub created_at: PrimitiveDateTime,

    /// When this contact record was last updated.
    pub updated_at: PrimitiveDateTime,
}

/// Find a [`ContactEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindContactById {
    pub id: i64,
}

impl Processor<FindContactById> for DatabaseProcessor {
    type Output = Option<ContactEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindContactById", err, fields(id = input.id))]
    async fn process(&self, input: FindContactById) -> Result<Option<ContactEntity>, sqlx::Error> {
        sqlx::query_as!(
            ContactEntity,
            r#"
            SELECT id, display_name, user_id, platform, identity, created_at, updated_at
            FROM memory.contact
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Look up a contact by the unique (platform, user_id) pair.
#[derive(Debug, Clone)]
pub struct FindContactByPlatformUser {
    pub platform: String,
    pub user_id: String,
}

impl Processor<FindContactByPlatformUser> for DatabaseProcessor {
    type Output = Option<ContactEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindContactByPlatformUser", err,
        fields(platform = %input.platform, user_id = %input.user_id))]
    async fn process(
        &self,
        input: FindContactByPlatformUser,
    ) -> Result<Option<ContactEntity>, sqlx::Error> {
        sqlx::query_as!(
            ContactEntity,
            r#"
            SELECT id, display_name, user_id, platform, identity, created_at, updated_at
            FROM memory.contact
            WHERE platform = $1 AND user_id = $2
            LIMIT 1
            "#,
            input.platform,
            input.user_id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new platform-specific contact record.
#[derive(Debug, Clone)]
pub struct CreateContact {
    pub display_name: String,
    pub user_id: String,
    pub platform: String,
    pub identity: Uuid,
}

impl Processor<CreateContact> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateContact", err,
        fields(platform = %input.platform, user_id = %input.user_id))]
    async fn process(&self, input: CreateContact) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.contact (display_name, user_id, platform, identity)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
            input.display_name,
            input.user_id,
            input.platform,
            input.identity,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the display name and linked identity of an existing contact.
#[derive(Debug, Clone)]
pub struct UpdateContact {
    pub id: i64,
    pub display_name: String,
    pub identity: Uuid,
}

impl Processor<UpdateContact> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateContact", err, fields(id = input.id))]
    async fn process(&self, input: UpdateContact) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.contact
            SET display_name = $2, identity = $3, updated_at = (now() AT TIME ZONE 'utc')
            WHERE id = $1
            "#,
            input.id,
            input.display_name,
            input.identity,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete a contact record by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteContact {
    pub id: i64,
}

impl Processor<DeleteContact> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteContact", err, fields(id = input.id))]
    async fn process(&self, input: DeleteContact) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!("DELETE FROM memory.contact WHERE id = $1", input.id,)
            .execute(self.db())
            .await?
            .rows_affected();
        Ok(rows > 0)
    }
}

/// All contact records linked to a given identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListContactsByIdentity {
    pub identity: Uuid,
}

impl Processor<ListContactsByIdentity> for DatabaseProcessor {
    type Output = Vec<ContactEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListContactsByIdentity", err, fields(identity = %input.identity))]
    async fn process(
        &self,
        input: ListContactsByIdentity,
    ) -> Result<Vec<ContactEntity>, sqlx::Error> {
        sqlx::query_as!(
            ContactEntity,
            r#"
            SELECT id, display_name, user_id, platform, identity, created_at, updated_at
            FROM memory.contact
            WHERE identity = $1
            ORDER BY id ASC
            "#,
            input.identity,
        )
        .fetch_all(self.db())
        .await
    }
}
