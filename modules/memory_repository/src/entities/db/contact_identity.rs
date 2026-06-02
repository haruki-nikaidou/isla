//! Cross-platform identity and relationship tracking.

use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// A unified identity representing a real person across platforms.
///
/// Multiple [`ContactEntity`](super::contact::ContactEntity) records from
/// different platforms can link to the same identity, allowing Isla to
/// maintain a consistent understanding of relationships.
#[derive(Debug, Clone)]
pub struct ContactIdentityEntity {
    /// Unique identifier for this identity.
    pub id: Uuid,

    /// The name Isla uses to refer to this person internally.
    pub identify_name: String,

    /// Notes about this person (interests, context, etc.).
    pub description: String,

    /// Current relationship status with this person.
    pub relationship: Relationship,

    /// When Isla first interacted with this person.
    pub first_meet_at: PrimitiveDateTime,

    /// When the relationship status was last changed.
    pub relationship_updated_at: PrimitiveDateTime,

    /// Audience visibility for everything we know about this identity (the
    /// identity record itself; individual [`ContactStoryEntity`] rows carry
    /// their own flag).
    pub privacy: PrivacyControlFlag,
}

/// The type of relationship Isla has with a contact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "memory.relationship")]
pub enum Relationship {
    /// Unknown person with no established relationship.
    Stranger,
    /// The primary user who owns/controls this Isla instance.
    Master,
    /// Someone Isla has interacted with but doesn't know well.
    Acquaintance,
    /// A friend or close contact.
    Dude,
    /// A contact that should be deprioritized or filtered.
    Ignored,
}

/// Find a [`ContactIdentityEntity`] by its UUID primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindContactIdentityById {
    pub id: Uuid,
}

impl Processor<FindContactIdentityById> for DatabaseProcessor {
    type Output = Option<ContactIdentityEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindContactIdentityById", err, fields(id = %input.id))]
    async fn process(
        &self,
        input: FindContactIdentityById,
    ) -> Result<Option<ContactIdentityEntity>, sqlx::Error> {
        sqlx::query_as!(
            ContactIdentityEntity,
            r#"
            SELECT
                id,
                identify_name,
                description,
                relationship AS "relationship: Relationship",
                first_meet_at,
                relationship_updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.contact_identity
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new cross-platform identity. The caller supplies the `id`.
#[derive(Debug, Clone)]
pub struct CreateContactIdentity {
    pub id: Uuid,
    pub identify_name: String,
    pub description: String,
    pub relationship: Relationship,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateContactIdentity> for DatabaseProcessor {
    type Output = Uuid;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateContactIdentity", err, fields(id = %input.id))]
    async fn process(&self, input: CreateContactIdentity) -> Result<Uuid, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.contact_identity (id, identify_name, description, relationship, privacy)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
            input.id,
            input.identify_name,
            input.description,
            input.relationship as Relationship,
            input.privacy as PrivacyControlFlag,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the name and description of an identity (not the relationship).
#[derive(Debug, Clone)]
pub struct UpdateContactIdentity {
    pub id: Uuid,
    pub identify_name: String,
    pub description: String,
}

impl Processor<UpdateContactIdentity> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateContactIdentity", err, fields(id = %input.id))]
    async fn process(&self, input: UpdateContactIdentity) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.contact_identity
            SET identify_name = $2, description = $3
            WHERE id = $1
            "#,
            input.id,
            input.identify_name,
            input.description,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Transition the relationship status of an identity and stamp `relationship_updated_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateContactIdentityRelationship {
    pub id: Uuid,
    pub relationship: Relationship,
}

impl Processor<UpdateContactIdentityRelationship> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateContactIdentityRelationship", err, fields(id = %input.id))]
    async fn process(
        &self,
        input: UpdateContactIdentityRelationship,
    ) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.contact_identity
            SET relationship = $2, relationship_updated_at = (now() AT TIME ZONE 'utc')
            WHERE id = $1
            "#,
            input.id,
            input.relationship as Relationship,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete an identity (cascades to all linked contacts and stories).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteContactIdentity {
    pub id: Uuid,
}

impl Processor<DeleteContactIdentity> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteContactIdentity", err, fields(id = %input.id))]
    async fn process(&self, input: DeleteContactIdentity) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.contact_identity WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Reassign the [`PrivacyControlFlag`] of an identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateContactIdentityPrivacy {
    pub id: Uuid,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateContactIdentityPrivacy> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateContactIdentityPrivacy", err, fields(id = %input.id))]
    async fn process(&self, input: UpdateContactIdentityPrivacy) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.contact_identity
            SET privacy = $2
            WHERE id = $1
            "#,
            input.id,
            input.privacy as PrivacyControlFlag,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Paginated list of all identities ordered alphabetically by `identify_name`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListContactIdentities {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListContactIdentities> for DatabaseProcessor {
    type Output = Vec<ContactIdentityEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListContactIdentities", err)]
    async fn process(&self, input: ListContactIdentities) -> Result<Vec<ContactIdentityEntity>, sqlx::Error> {
        sqlx::query_as!(
            ContactIdentityEntity,
            r#"
            SELECT
                id,
                identify_name,
                description,
                relationship AS "relationship: Relationship",
                first_meet_at,
                relationship_updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.contact_identity
            ORDER BY identify_name ASC
            LIMIT $1 OFFSET $2
            "#,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}
