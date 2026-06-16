//! PostgreSQL-backed entity definitions for persistent memory storage.
//!
//! This module contains all database entities grouped by domain:
//!
//! - **Object storage**: [`bucket`], [`object_storage`] — S3-compatible blob storage references
//! - **Conversations**: [`conversation`], [`conversation_message`], [`conversation_content`] — chat history
//! - **Calendar**: [`calender`], [`calender_event`], [`calender_daily_event`], [`calender_task`] — scheduling
//! - **Contacts**: [`contact`], [`contact_identity`], [`contact_stories`] — relationship tracking
//! - **Diary**: [`diary`] — daily journal entries
//! - **Personality**: [`personality`] — character facets surfaced into the system prompt

pub mod bucket;
pub mod calender;
pub mod calender_daily_event;
pub mod calender_event;
pub mod calender_task;
pub mod contact;
pub mod contact_identity;
pub mod contact_stories;
pub mod conversation;
pub mod conversation_content;
pub mod conversation_message;
pub mod conversation_message_metric;
pub mod conversation_summary_node;
pub mod diary;
pub mod embedding;
pub mod object_storage;
pub mod personality;

/// Audience-based visibility flag for stored memory entities.
///
/// The flag is consulted by the service layer when it assembles LLM input
/// context for a conversation: rows whose [`PrivacyControlFlag`] excludes
/// the current peer's [`Relationship`](contact_identity::Relationship) must
/// be omitted so private context never leaks across audiences.
///
/// Visibility matrix (✓ = visible):
///
/// | Flag        | Master | Dude | Acquaintance | Stranger / Ignored |
/// | ----------- | :----: | :--: | :----------: | :----------------: |
/// | `Private`   |   ✓    |      |              |                    |
/// | `Protected` |   ✓    |  ✓   |      ✓       |                    |
/// | `Public`    |   ✓    |  ✓   |      ✓       |         ✓          |
///
/// Stored in PostgreSQL as the `memory.privacy_control` enum. Newly created
/// rows default to [`PrivacyControlFlag::Protected`] at the database level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(type_name = "memory.privacy_control")]
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub enum PrivacyControlFlag {
    /// Visible only to the Master.
    Private,
    /// Visible to the Master and to known relationships (Dude, Acquaintance).
    #[default]
    Protected,
    /// Visible to everyone, including strangers.
    Public,
}

impl PrivacyControlFlag {
    /// Returns `true` if a memory tagged with this flag may be surfaced to a
    /// peer in the given [`Relationship`](contact_identity::Relationship).
    #[must_use]
    pub const fn allows(self, relationship: contact_identity::Relationship) -> bool {
        use contact_identity::Relationship;
        match (self, relationship) {
            (_, Relationship::Master) => true,
            (Self::Private, _) => false,
            (Self::Protected, Relationship::Dude | Relationship::Acquaintance) => true,
            (Self::Protected, _) => false,
            (Self::Public, _) => true,
        }
    }
}
