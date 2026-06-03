//! Multimodal content blocks within conversation messages.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::sqlx::DatabaseProcessor;

/// A single content block within a conversation message.
///
/// Messages can contain multiple content blocks of different modalities
/// (text, images, audio, etc.). Each block is stored separately to support
/// efficient retrieval and multimodal search.
#[derive(Debug, Clone)]
pub struct ConversationContentEntity {
    /// Unique identifier for this content block.
    pub id: i64,

    /// ID of the parent [`ConversationMessageEntity`](super::conversation_message::ConversationMessageEntity).
    pub message_id: i64,

    /// Order of this block within the message (0-indexed).
    pub position: i32,

    /// The type of content in this block.
    pub modality: ContentModality,

    /// Text content (for [`ContentModality::Text`]).
    pub text: Option<String>,

    /// Reference to stored object (for non-text modalities).
    /// The DB enforces `octet_length(object_hash) = 32` via a CHECK constraint.
    pub object_hash: Option<Vec<u8>>,

    /// Image detail level hint (e.g., `low`, `high`, `auto`).
    pub image_detail: Option<String>,

    /// Audio encoding format (e.g., `mp3`, `wav`, `opus`).
    pub audio_format: Option<String>,
}

/// The type of content in a message block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "memory.content_modality")]
pub enum ContentModality {
    /// Plain text content.
    Text,
    /// Image content (stored in object storage).
    Image,
    /// Audio content (stored in object storage).
    Audio,
    /// Generic file attachment (stored in object storage).
    File,
    /// Video content (stored in object storage).
    Video,
}

/// Find a [`ConversationContentEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindConversationContentById {
    pub id: i64,
}

impl Processor<FindConversationContentById> for DatabaseProcessor {
    type Output = Option<ConversationContentEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindConversationContentById", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindConversationContentById,
    ) -> Result<Option<ConversationContentEntity>, sqlx::Error> {
        sqlx::query_as!(
            ConversationContentEntity,
            r#"
            SELECT
                id,
                message_id,
                position,
                modality AS "modality: ContentModality",
                text,
                object_hash,
                image_detail,
                audio_format
            FROM memory.conversation_content
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a single content block for a message.
#[derive(Debug, Clone)]
pub struct CreateConversationContent {
    pub message_id: i64,
    pub position: i32,
    pub modality: ContentModality,
    pub text: Option<String>,
    pub object_hash: Option<Vec<u8>>,
    pub image_detail: Option<String>,
    pub audio_format: Option<String>,
}

impl Processor<CreateConversationContent> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateConversationContent", err,
        fields(message_id = input.message_id, position = input.position))]
    async fn process(&self, input: CreateConversationContent) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.conversation_content
                (message_id, position, modality, text, object_hash, image_detail, audio_format)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
            input.message_id,
            input.position,
            input.modality as ContentModality,
            input.text,
            input.object_hash,
            input.image_detail,
            input.audio_format,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Delete a content block by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteConversationContent {
    pub id: i64,
}

impl Processor<DeleteConversationContent> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteConversationContent", err, fields(id = input.id))]
    async fn process(&self, input: DeleteConversationContent) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.conversation_content WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// All content blocks for a message ordered by position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListConversationContentByMessage {
    pub message_id: i64,
}

impl Processor<ListConversationContentByMessage> for DatabaseProcessor {
    type Output = Vec<ConversationContentEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListConversationContentByMessage", err,
        fields(message_id = input.message_id))]
    async fn process(
        &self,
        input: ListConversationContentByMessage,
    ) -> Result<Vec<ConversationContentEntity>, sqlx::Error> {
        sqlx::query_as!(
            ConversationContentEntity,
            r#"
            SELECT
                id,
                message_id,
                position,
                modality AS "modality: ContentModality",
                text,
                object_hash,
                image_detail,
                audio_format
            FROM memory.conversation_content
            WHERE message_id = $1
            ORDER BY position ASC
            "#,
            input.message_id,
        )
        .fetch_all(self.db())
        .await
    }
}
