//! The unified RAG embedding index.
//!
//! One row per source entry. The entry type is a **product of nullable foreign
//! keys** ([`EntryRef`]) rather than an enum discriminant: exactly one ref is
//! set. The vector is `halfvec(2048)` searched by cosine distance; `privacy` is
//! inherited from the source entry (kept in sync by a hook).

use kanau::processor::Processor;
use pgvector::HalfVector;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// Reference to the source entry an embedding indexes.
///
/// A product of optional foreign keys; construct via the helpers so exactly one
/// is set. Stored as four nullable columns with a DB `CHECK` enforcing the
/// "exactly one" invariant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct EntryRef {
    pub calender_event: Option<i64>,
    pub conversation: Option<i64>,
    pub contact: Option<Uuid>,
    pub diary: Option<i64>,
}

impl EntryRef {
    /// Reference a calendar event.
    #[must_use]
    pub fn calender_event(id: i64) -> Self {
        Self {
            calender_event: Some(id),
            ..Self::default()
        }
    }
    /// Reference a conversation.
    #[must_use]
    pub fn conversation(id: i64) -> Self {
        Self {
            conversation: Some(id),
            ..Self::default()
        }
    }
    /// Reference a contact identity.
    #[must_use]
    pub fn contact(id: Uuid) -> Self {
        Self {
            contact: Some(id),
            ..Self::default()
        }
    }
    /// Reference a diary entry.
    #[must_use]
    pub fn diary(id: i64) -> Self {
        Self {
            diary: Some(id),
            ..Self::default()
        }
    }

    /// Whether exactly one ref is set (the table's invariant).
    #[must_use]
    pub fn is_exactly_one(&self) -> bool {
        let set = usize::from(self.calender_event.is_some())
            + usize::from(self.conversation.is_some())
            + usize::from(self.contact.is_some())
            + usize::from(self.diary.is_some());
        set == 1
    }
}

/// A nearest-neighbour hit from [`SearchEmbeddings`].
#[derive(Debug, Clone)]
pub struct EmbeddingHit {
    pub id: i64,
    pub calender_event: Option<i64>,
    pub conversation: Option<i64>,
    pub contact: Option<Uuid>,
    pub diary: Option<i64>,
    pub privacy: PrivacyControlFlag,
    pub content: String,
    /// Cosine distance to the query vector (smaller is closer).
    pub distance: f64,
}

impl EmbeddingHit {
    /// The source reference this hit points at.
    #[must_use]
    pub fn reference(&self) -> EntryRef {
        EntryRef {
            calender_event: self.calender_event,
            conversation: self.conversation,
            contact: self.contact,
            diary: self.diary,
        }
    }
}

/// Insert or replace the embedding row for a source entry.
#[derive(Debug, Clone)]
pub struct UpsertEmbeddingByRef {
    pub reference: EntryRef,
    pub privacy: PrivacyControlFlag,
    pub content: String,
    /// The computed vector, or `None` to store the row before embedding.
    pub embedding: Option<HalfVector>,
    pub updated_at: i64,
}

impl Processor<UpsertEmbeddingByRef> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpsertEmbeddingByRef", err)]
    async fn process(&self, input: UpsertEmbeddingByRef) -> Result<i64, sqlx::Error> {
        sqlx::query_file_scalar!(
            "sql/embedding_upsert.sql",
            input.reference.calender_event,
            input.reference.conversation,
            input.reference.contact,
            input.reference.diary,
            input.privacy as PrivacyControlFlag,
            input.content,
            input.embedding as Option<HalfVector>,
            input.updated_at,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the inherited privacy of an entry's embedding row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateEmbeddingPrivacyByRef {
    pub reference: EntryRef,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateEmbeddingPrivacyByRef> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateEmbeddingPrivacyByRef", err)]
    async fn process(&self, input: UpdateEmbeddingPrivacyByRef) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query_file!(
            "sql/embedding_update_privacy.sql",
            input.reference.calender_event,
            input.reference.conversation,
            input.reference.contact,
            input.reference.diary,
            input.privacy as PrivacyControlFlag,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete an entry's embedding row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteEmbeddingByRef {
    pub reference: EntryRef,
}

impl Processor<DeleteEmbeddingByRef> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteEmbeddingByRef", err)]
    async fn process(&self, input: DeleteEmbeddingByRef) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query_file!(
            "sql/embedding_delete.sql",
            input.reference.calender_event,
            input.reference.conversation,
            input.reference.contact,
            input.reference.diary,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Nearest embeddings to a query vector within the allowed privacy levels.
#[derive(Debug, Clone)]
pub struct SearchEmbeddings {
    pub query: HalfVector,
    pub allowed: Vec<PrivacyControlFlag>,
    pub limit: i64,
}

impl Processor<SearchEmbeddings> for DatabaseProcessor {
    type Output = Vec<EmbeddingHit>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:SearchEmbeddings", err, fields(limit = input.limit))]
    async fn process(&self, input: SearchEmbeddings) -> Result<Vec<EmbeddingHit>, sqlx::Error> {
        sqlx::query_file_as!(
            EmbeddingHit,
            "sql/embedding_search.sql",
            input.query as HalfVector,
            &input.allowed as &[PrivacyControlFlag],
            input.limit,
        )
        .fetch_all(self.db())
        .await
    }
}
