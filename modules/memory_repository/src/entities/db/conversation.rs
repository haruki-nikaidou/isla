//! Conversation sessions and their summaries.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// A conversation session between the user and the agent.
///
/// Conversations are the top-level container for a series of messages.
/// They maintain summaries for quick context retrieval without loading
/// the full message history.
#[derive(Debug, Clone)]
pub struct ConversationEntity {
    /// Unique identifier for this conversation.
    pub id: i64,

    /// AI-generated summary of how the conversation started.
    pub opening_summary: String,

    /// AI-generated summary of the conversation outcome (set when conversation ends).
    pub closing_summary: Option<String>,

    /// Unix timestamp when the conversation started.
    pub created_at: i64,

    /// Unix timestamp of the last activity in this conversation.
    pub updated_at: i64,

    /// Audience visibility for this conversation; gates whether the agent
    /// may load it into the LLM context when chatting with a given peer.
    pub privacy: PrivacyControlFlag,
}

/// Find a [`ConversationEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindConversationById {
    pub id: i64,
}

impl Processor<FindConversationById> for DatabaseProcessor {
    type Output = Option<ConversationEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindConversationById", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindConversationById,
    ) -> Result<Option<ConversationEntity>, sqlx::Error> {
        sqlx::query_as!(
            ConversationEntity,
            r#"
            SELECT
                id,
                opening_summary,
                closing_summary,
                created_at,
                updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.conversation
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new conversation with its opening summary and Unix timestamps.
#[derive(Debug, Clone)]
pub struct CreateConversation {
    pub opening_summary: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateConversation> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateConversation", err)]
    async fn process(&self, input: CreateConversation) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.conversation (opening_summary, created_at, updated_at, privacy)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
            input.opening_summary,
            input.created_at,
            input.updated_at,
            input.privacy as PrivacyControlFlag,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Overwrite the opening summary of a conversation.
#[derive(Debug, Clone)]
pub struct UpdateConversationOpeningSummary {
    pub id: i64,
    pub opening_summary: String,
}

impl Processor<UpdateConversationOpeningSummary> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateConversationOpeningSummary", err, fields(id = input.id))]
    async fn process(
        &self,
        input: UpdateConversationOpeningSummary,
    ) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.conversation
            SET opening_summary = $2
            WHERE id = $1
            "#,
            input.id,
            input.opening_summary,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Set the closing summary and bump `updated_at` when a conversation ends.
#[derive(Debug, Clone)]
pub struct CloseConversation {
    pub id: i64,
    pub closing_summary: String,
    pub updated_at: i64,
}

impl Processor<CloseConversation> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CloseConversation", err, fields(id = input.id))]
    async fn process(&self, input: CloseConversation) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.conversation
            SET closing_summary = $2, updated_at = $3
            WHERE id = $1
            "#,
            input.id,
            input.closing_summary,
            input.updated_at,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Update `updated_at` to reflect recent activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TouchConversation {
    pub id: i64,
    pub updated_at: i64,
}

impl Processor<TouchConversation> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:TouchConversation", err, fields(id = input.id))]
    async fn process(&self, input: TouchConversation) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "UPDATE memory.conversation SET updated_at = $2 WHERE id = $1",
            input.id,
            input.updated_at,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete a conversation (cascades to messages and content).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteConversation {
    pub id: i64,
}

impl Processor<DeleteConversation> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteConversation", err, fields(id = input.id))]
    async fn process(&self, input: DeleteConversation) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.conversation WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateConversationPrivacy {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateConversationPrivacy> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateConversationPrivacy", err, fields(id = input.id))]
    async fn process(&self, input: UpdateConversationPrivacy) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.conversation
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

/// Paginated list of conversations ordered by most recent activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListRecentConversations {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListRecentConversations> for DatabaseProcessor {
    type Output = Vec<ConversationEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListRecentConversations", err)]
    async fn process(&self, input: ListRecentConversations) -> Result<Vec<ConversationEntity>, sqlx::Error> {
        sqlx::query_as!(
            ConversationEntity,
            r#"
            SELECT
                id,
                opening_summary,
                closing_summary,
                created_at,
                updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.conversation
            ORDER BY updated_at DESC
            LIMIT $1 OFFSET $2
            "#,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}
