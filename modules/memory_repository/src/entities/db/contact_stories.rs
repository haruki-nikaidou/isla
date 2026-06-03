//! Memorable events and interactions with contacts.

use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// A notable event or interaction involving a contact.
///
/// Stories capture significant moments in Isla's relationship with a person,
/// such as first meetings, relationship changes, or memorable conversations.
#[derive(Debug, Clone)]
pub struct ContactStoryEntity {
    /// Unique identifier for this story.
    pub id: i64,

    /// ID of the [`ContactIdentityEntity`](super::contact_identity::ContactIdentityEntity) this story is about.
    pub identity: Uuid,

    /// Classification of what kind of event this story represents.
    pub story_type: StoryType,

    /// Short title for this story.
    pub story_name: String,

    /// Brief summary of what happened.
    pub story_summary: String,

    /// Full narrative of the event.
    pub story_text: String,

    /// When this event occurred.
    pub happened_at: PrimitiveDateTime,

    /// Optional link to the conversation where this story originated.
    pub related_conversation: Option<i64>,

    /// Audience visibility for this story.
    pub privacy: PrivacyControlFlag,
}

/// Classification of what kind of event a story represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "memory.story_type")]
pub enum StoryType {
    /// The relationship became closer or more trusting.
    RelationshipUpgrade,
    /// The relationship became more distant or strained.
    RelationshipDowngrade,
    /// The first interaction with this person.
    FirstMeeting,
    /// Isla's understanding of this person changed significantly.
    ImpressionChanged,
    /// Any other notable event.
    Other,
}

/// Find a [`ContactStoryEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindContactStoryById {
    pub id: i64,
}

impl Processor<FindContactStoryById> for DatabaseProcessor {
    type Output = Option<ContactStoryEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindContactStoryById", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindContactStoryById,
    ) -> Result<Option<ContactStoryEntity>, sqlx::Error> {
        sqlx::query_as!(
            ContactStoryEntity,
            r#"
            SELECT
                id,
                identity,
                story_type AS "story_type: StoryType",
                story_name,
                story_summary,
                story_text,
                happened_at,
                related_conversation,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.contact_story
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new story for a contact identity.
#[derive(Debug, Clone)]
pub struct CreateContactStory {
    pub identity: Uuid,
    pub story_type: StoryType,
    pub story_name: String,
    pub story_summary: String,
    pub story_text: String,
    pub happened_at: PrimitiveDateTime,
    pub related_conversation: Option<i64>,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateContactStory> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateContactStory", err, fields(identity = %input.identity))]
    async fn process(&self, input: CreateContactStory) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.contact_story
                (identity, story_type, story_name, story_summary, story_text,
                 happened_at, related_conversation, privacy)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            "#,
            input.identity,
            input.story_type as StoryType,
            input.story_name,
            input.story_summary,
            input.story_text,
            input.happened_at,
            input.related_conversation,
            input.privacy as PrivacyControlFlag,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the narrative content of an existing story.
#[derive(Debug, Clone)]
pub struct UpdateContactStory {
    pub id: i64,
    pub story_name: String,
    pub story_summary: String,
    pub story_text: String,
}

impl Processor<UpdateContactStory> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateContactStory", err, fields(id = input.id))]
    async fn process(&self, input: UpdateContactStory) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.contact_story
            SET story_name = $2, story_summary = $3, story_text = $4
            WHERE id = $1
            "#,
            input.id,
            input.story_name,
            input.story_summary,
            input.story_text,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete a story by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteContactStory {
    pub id: i64,
}

impl Processor<DeleteContactStory> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteContactStory", err, fields(id = input.id))]
    async fn process(&self, input: DeleteContactStory) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!("DELETE FROM memory.contact_story WHERE id = $1", input.id,)
            .execute(self.db())
            .await?
            .rows_affected();
        Ok(rows > 0)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a story.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateContactStoryPrivacy {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateContactStoryPrivacy> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateContactStoryPrivacy", err, fields(id = input.id))]
    async fn process(&self, input: UpdateContactStoryPrivacy) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.contact_story
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

/// Paginated list of stories for an identity ordered by occurrence time (newest first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListContactStoriesByIdentity {
    pub identity: Uuid,
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListContactStoriesByIdentity> for DatabaseProcessor {
    type Output = Vec<ContactStoryEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListContactStoriesByIdentity", err, fields(identity = %input.identity))]
    async fn process(
        &self,
        input: ListContactStoriesByIdentity,
    ) -> Result<Vec<ContactStoryEntity>, sqlx::Error> {
        sqlx::query_as!(
            ContactStoryEntity,
            r#"
            SELECT
                id,
                identity,
                story_type AS "story_type: StoryType",
                story_name,
                story_summary,
                story_text,
                happened_at,
                related_conversation,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.contact_story
            WHERE identity = $1
            ORDER BY happened_at DESC
            LIMIT $2 OFFSET $3
            "#,
            input.identity,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}
