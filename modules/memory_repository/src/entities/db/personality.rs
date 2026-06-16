//! Personality facets — the building blocks of the agent's character.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// One facet of the agent's character.
///
/// The system prompt is assembled from many facets. Which facets are surfaced
/// depends on the current peer: `is_core` facets are always included, while the
/// rest are gated by [`PrivacyControlFlag`] against the peer's
/// [`Relationship`](super::contact_identity::Relationship) — the same audience
/// matrix used for memories.
#[derive(Debug, Clone)]
pub struct PersonalityFacetEntity {
    /// Unique identifier for this facet.
    pub id: i64,

    /// Short human-readable name (e.g. `core-identity`, `playful-with-master`).
    pub name: String,

    /// The instruction text contributed to the system prompt.
    pub content: String,

    /// Ordering weight; higher-priority facets appear earlier in the prompt.
    pub priority: i32,

    /// Whether this facet is part of the invariant character base and is always
    /// included regardless of audience.
    pub is_core: bool,

    /// Audience gate for non-core facets.
    pub privacy: PrivacyControlFlag,

    /// Unix timestamp when this facet was created.
    pub created_at: i64,

    /// Unix timestamp of the last update to this facet.
    pub updated_at: i64,
}

/// Find a [`PersonalityFacetEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindPersonalityFacetById {
    pub id: i64,
}

impl Processor<FindPersonalityFacetById> for DatabaseProcessor {
    type Output = Option<PersonalityFacetEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindPersonalityFacetById", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindPersonalityFacetById,
    ) -> Result<Option<PersonalityFacetEntity>, sqlx::Error> {
        sqlx::query_as!(
            PersonalityFacetEntity,
            r#"
            SELECT
                id,
                name,
                content,
                priority,
                is_core,
                privacy AS "privacy: PrivacyControlFlag",
                created_at,
                updated_at
            FROM memory.personality_facet
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// List every personality facet ordered by descending priority.
///
/// Audience filtering is intentionally *not* done here; it is business logic
/// applied by the service layer (see
/// [`PersonalityService`](crate::services::personality::PersonalityService)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListPersonalityFacets;

impl Processor<ListPersonalityFacets> for DatabaseProcessor {
    type Output = Vec<PersonalityFacetEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListPersonalityFacets", err)]
    async fn process(
        &self,
        _input: ListPersonalityFacets,
    ) -> Result<Vec<PersonalityFacetEntity>, sqlx::Error> {
        sqlx::query_as!(
            PersonalityFacetEntity,
            r#"
            SELECT
                id,
                name,
                content,
                priority,
                is_core,
                privacy AS "privacy: PrivacyControlFlag",
                created_at,
                updated_at
            FROM memory.personality_facet
            ORDER BY priority DESC, id ASC
            "#,
        )
        .fetch_all(self.db())
        .await
    }
}

/// Insert a new personality facet.
#[derive(Debug, Clone)]
pub struct CreatePersonalityFacet {
    pub name: String,
    pub content: String,
    pub priority: i32,
    pub is_core: bool,
    pub privacy: PrivacyControlFlag,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Processor<CreatePersonalityFacet> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreatePersonalityFacet", err)]
    async fn process(&self, input: CreatePersonalityFacet) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.personality_facet
                (name, content, priority, is_core, privacy, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
            input.name,
            input.content,
            input.priority,
            input.is_core,
            input.privacy as PrivacyControlFlag,
            input.created_at,
            input.updated_at,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the mutable fields of a personality facet.
#[derive(Debug, Clone)]
pub struct UpdatePersonalityFacet {
    pub id: i64,
    pub name: String,
    pub content: String,
    pub priority: i32,
    pub is_core: bool,
    pub privacy: PrivacyControlFlag,
    pub updated_at: i64,
}

impl Processor<UpdatePersonalityFacet> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdatePersonalityFacet", err, fields(id = input.id))]
    async fn process(&self, input: UpdatePersonalityFacet) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.personality_facet
            SET name = $2, content = $3, priority = $4, is_core = $5, privacy = $6, updated_at = $7
            WHERE id = $1
            "#,
            input.id,
            input.name,
            input.content,
            input.priority,
            input.is_core,
            input.privacy as PrivacyControlFlag,
            input.updated_at,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete a personality facet by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletePersonalityFacet {
    pub id: i64,
}

impl Processor<DeletePersonalityFacet> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeletePersonalityFacet", err, fields(id = input.id))]
    async fn process(&self, input: DeletePersonalityFacet) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.personality_facet WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}
