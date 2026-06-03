//! Conversation service — lifecycle management, message appending, and content retrieval.

use kanau::processor::Processor;
use tracing::instrument;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::{
    PrivacyControlFlag,
    conversation::{
        CloseConversation, ConversationEntity, CreateConversation, DeleteConversation,
        FindConversationById, ListRecentConversations, TouchConversation,
        UpdateConversationOpeningSummary, UpdateConversationPrivacy,
    },
    conversation_content::{
        ConversationContentEntity, DeleteConversationContent, FindConversationContentById,
        ListConversationContentByMessage,
    },
    conversation_message::{
        AppendMessageTx, ConversationMessageEntity, DeleteConversationMessage,
        FindConversationMessageById, ListConversationMessages, MessageRole, NewContent,
        SetConversationMessageBranch,
    },
};

#[derive(Debug, Clone)]
pub struct ConversationService {
    pub database: DatabaseProcessor,
}

/// Returns the current Unix timestamp in seconds.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

// ─── Conversation lifecycle ───────────────────────────────────────────────────

/// Start a new conversation with an opening summary.
#[derive(Debug, Clone)]
pub struct CreateConversationRequest {
    pub opening_summary: String,
    /// Audience visibility for the new conversation.
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateConversationRequest> for ConversationService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "CreateConversationRequest", err)]
    async fn process(&self, input: CreateConversationRequest) -> Result<i64, Error> {
        let ts = now_unix();
        Ok(self
            .database
            .process(CreateConversation {
                opening_summary: input.opening_summary,
                created_at: ts,
                updated_at: ts,
                privacy: input.privacy,
            })
            .await?)
    }
}

/// Retrieve a conversation by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindConversationRequest {
    pub id: i64,
}

impl Processor<FindConversationRequest> for ConversationService {
    type Output = Option<ConversationEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindConversationRequest", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindConversationRequest,
    ) -> Result<Option<ConversationEntity>, Error> {
        Ok(self
            .database
            .process(FindConversationById { id: input.id })
            .await?)
    }
}

/// Overwrite the opening summary of a conversation.
#[derive(Debug, Clone)]
pub struct UpdateOpeningSummaryRequest {
    pub id: i64,
    pub opening_summary: String,
}

impl Processor<UpdateOpeningSummaryRequest> for ConversationService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateOpeningSummaryRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateOpeningSummaryRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(UpdateConversationOpeningSummary {
                id: input.id,
                opening_summary: input.opening_summary,
            })
            .await?)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a conversation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateConversationPrivacyRequest {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateConversationPrivacyRequest> for ConversationService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateConversationPrivacyRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateConversationPrivacyRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(UpdateConversationPrivacy {
                id: input.id,
                privacy: input.privacy,
            })
            .await?)
    }
}

/// Mark a conversation as finished by providing its closing summary.
#[derive(Debug, Clone)]
pub struct CloseConversationRequest {
    pub id: i64,
    pub closing_summary: String,
}

impl Processor<CloseConversationRequest> for ConversationService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "CloseConversationRequest", err, fields(id = input.id))]
    async fn process(&self, input: CloseConversationRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(CloseConversation {
                id: input.id,
                closing_summary: input.closing_summary,
                updated_at: now_unix(),
            })
            .await?)
    }
}

/// Bump the `updated_at` timestamp to record recent activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TouchConversationRequest {
    pub id: i64,
}

impl Processor<TouchConversationRequest> for ConversationService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "TouchConversationRequest", err, fields(id = input.id))]
    async fn process(&self, input: TouchConversationRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(TouchConversation {
                id: input.id,
                updated_at: now_unix(),
            })
            .await?)
    }
}

/// Delete a conversation (cascades to all messages and content blocks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteConversationRequest {
    pub id: i64,
}

impl Processor<DeleteConversationRequest> for ConversationService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteConversationRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteConversationRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteConversation { id: input.id })
            .await?)
    }
}

/// Paginated list of conversations ordered by most recent activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListRecentConversationsRequest {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListRecentConversationsRequest> for ConversationService {
    type Output = Vec<ConversationEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListRecentConversationsRequest", err)]
    async fn process(
        &self,
        input: ListRecentConversationsRequest,
    ) -> Result<Vec<ConversationEntity>, Error> {
        Ok(self
            .database
            .process(ListRecentConversations {
                limit: input.limit,
                offset: input.offset,
            })
            .await?)
    }
}

// ─── Message operations ───────────────────────────────────────────────────────

/// Atomically append a new message with its content blocks to a conversation.
///
/// Uses a database transaction to insert the message row, all content blocks,
/// and update `conversation.updated_at` as a single atomic unit.
/// Returns the new message's primary key.
#[derive(Debug, Clone)]
pub struct AppendMessageRequest {
    pub conversation_id: i64,
    pub before: Option<i64>,
    pub role: MessageRole,
    pub contents: Vec<NewContent>,
}

impl Processor<AppendMessageRequest> for ConversationService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "AppendMessageRequest", err,
        fields(conversation_id = input.conversation_id))]
    async fn process(&self, input: AppendMessageRequest) -> Result<i64, Error> {
        Ok(self
            .database
            .process(AppendMessageTx {
                conversation_id: input.conversation_id,
                before: input.before,
                role: input.role,
                contents: input.contents,
                conversation_updated_at: now_unix(),
            })
            .await?)
    }
}

/// Retrieve a single message by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindMessageRequest {
    pub id: i64,
}

impl Processor<FindMessageRequest> for ConversationService {
    type Output = Option<ConversationMessageEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindMessageRequest", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindMessageRequest,
    ) -> Result<Option<ConversationMessageEntity>, Error> {
        Ok(self
            .database
            .process(FindConversationMessageById { id: input.id })
            .await?)
    }
}

/// Paginated list of messages in a conversation.
///
/// Set `only_current_branch = true` to exclude messages from discarded branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListMessagesRequest {
    pub conversation_id: i64,
    pub only_current_branch: bool,
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListMessagesRequest> for ConversationService {
    type Output = Vec<ConversationMessageEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListMessagesRequest", err,
        fields(conversation_id = input.conversation_id))]
    async fn process(
        &self,
        input: ListMessagesRequest,
    ) -> Result<Vec<ConversationMessageEntity>, Error> {
        Ok(self
            .database
            .process(ListConversationMessages {
                conversation_id: input.conversation_id,
                only_current_branch: input.only_current_branch,
                limit: input.limit,
                offset: input.offset,
            })
            .await?)
    }
}

/// Flip the active-branch flag on a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwitchBranchRequest {
    pub message_id: i64,
    pub is_current_branch: bool,
}

impl Processor<SwitchBranchRequest> for ConversationService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "SwitchBranchRequest", err, fields(message_id = input.message_id))]
    async fn process(&self, input: SwitchBranchRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(SetConversationMessageBranch {
                id: input.message_id,
                is_current_branch: input.is_current_branch,
            })
            .await?)
    }
}

/// Delete a message (cascades to its content blocks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteMessageRequest {
    pub id: i64,
}

impl Processor<DeleteMessageRequest> for ConversationService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteMessageRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteMessageRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteConversationMessage { id: input.id })
            .await?)
    }
}

// ─── Content operations ───────────────────────────────────────────────────────

/// All content blocks for a message, ordered by position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListContentsForMessageRequest {
    pub message_id: i64,
}

impl Processor<ListContentsForMessageRequest> for ConversationService {
    type Output = Vec<ConversationContentEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListContentsForMessageRequest", err,
        fields(message_id = input.message_id))]
    async fn process(
        &self,
        input: ListContentsForMessageRequest,
    ) -> Result<Vec<ConversationContentEntity>, Error> {
        Ok(self
            .database
            .process(ListConversationContentByMessage {
                message_id: input.message_id,
            })
            .await?)
    }
}

/// Retrieve a content block by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindContentRequest {
    pub id: i64,
}

impl Processor<FindContentRequest> for ConversationService {
    type Output = Option<ConversationContentEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindContentRequest", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindContentRequest,
    ) -> Result<Option<ConversationContentEntity>, Error> {
        Ok(self
            .database
            .process(FindConversationContentById { id: input.id })
            .await?)
    }
}

/// Delete an individual content block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteContentRequest {
    pub id: i64,
}

impl Processor<DeleteContentRequest> for ConversationService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteContentRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteContentRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteConversationContent { id: input.id })
            .await?)
    }
}
