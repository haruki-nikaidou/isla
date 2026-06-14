//! RAG indexing and semantic search over memory entries.
//!
//! Embeddings are produced through the [`Embedder`] seam (LLM-backed in
//! production, [`MockEmbedder`] in tests — mirroring the `LlmProvider` seam in
//! `ai_caller`). Search filters by audience using the same
//! [`PrivacyControlFlag::allows`] matrix as the rest of memory.

use kanau::processor::Processor;
use pgvector::HalfVector;
use tracing::instrument;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::PrivacyControlFlag;
use crate::entities::db::contact_identity::Relationship;
use crate::entities::db::embedding::{
    EmbeddingHit, EntryRef, SearchEmbeddings, UpsertEmbeddingByRef,
};

/// The embedding dimensionality of the index (`halfvec(2048)`).
pub const EMBEDDING_DIMS: usize = 2048;

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// The privacy levels visible to a peer in `relationship`.
fn allowed_privacy(relationship: Relationship) -> Vec<PrivacyControlFlag> {
    [
        PrivacyControlFlag::Private,
        PrivacyControlFlag::Protected,
        PrivacyControlFlag::Public,
    ]
    .into_iter()
    .filter(|p| p.allows(relationship))
    .collect()
}

// ─── Seam ─────────────────────────────────────────────────────────────────────

/// Turns text into an embedding vector. The single seam to any embedding model.
pub trait Embedder: Processor<EmbedRequest, Output = HalfVector, Error = Error> {}
impl<T> Embedder for T where T: Processor<EmbedRequest, Output = HalfVector, Error = Error> {}

/// Input to the embedder.
#[derive(Debug, Clone)]
pub struct EmbedRequest {
    pub text: String,
}

/// A deterministic, network-free embedder for tests: hashes the text into a
/// fixed [`EMBEDDING_DIMS`]-wide vector.
#[derive(Debug, Clone, Default)]
pub struct MockEmbedder;

impl Processor<EmbedRequest> for MockEmbedder {
    type Output = HalfVector;
    type Error = Error;

    async fn process(&self, input: EmbedRequest) -> Result<HalfVector, Error> {
        let mut data = vec![0.0f32; EMBEDDING_DIMS];
        for (i, byte) in input.text.bytes().enumerate() {
            data[i % EMBEDDING_DIMS] += f32::from(byte);
        }
        Ok(HalfVector::from_f32_slice(&data))
    }
}

// ─── Service ──────────────────────────────────────────────────────────────────

/// Indexes memory entries and answers semantic search.
#[derive(Debug, Clone)]
pub struct EmbeddingService<E> {
    pub database: DatabaseProcessor,
    pub embedder: E,
}

/// Embed an entry's text and upsert it into the index.
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub reference: EntryRef,
    pub privacy: PrivacyControlFlag,
    pub content: String,
}

impl<E> Processor<IndexEntry> for EmbeddingService<E>
where
    E: Embedder + Sync,
{
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "IndexEntry", err)]
    async fn process(&self, input: IndexEntry) -> Result<i64, Error> {
        if !input.reference.is_exactly_one() {
            return Err(Error::InvalidInput);
        }
        let embedding = self
            .embedder
            .process(EmbedRequest {
                text: input.content.clone(),
            })
            .await?;
        let id = self
            .database
            .process(UpsertEmbeddingByRef {
                reference: input.reference,
                privacy: input.privacy,
                content: input.content,
                embedding: Some(embedding),
                updated_at: now_unix(),
            })
            .await?;
        Ok(id)
    }
}

/// Semantic search restricted to what `relationship` is allowed to see.
#[derive(Debug, Clone)]
pub struct SearchMemory {
    pub query: String,
    pub relationship: Relationship,
    pub limit: i64,
}

impl<E> Processor<SearchMemory> for EmbeddingService<E>
where
    E: Embedder + Sync,
{
    type Output = Vec<EmbeddingHit>;
    type Error = Error;

    #[instrument(skip_all, name = "SearchMemory", err, fields(limit = input.limit))]
    async fn process(&self, input: SearchMemory) -> Result<Vec<EmbeddingHit>, Error> {
        if input.limit <= 0 {
            return Err(Error::InvalidInput);
        }
        let query = self
            .embedder
            .process(EmbedRequest { text: input.query })
            .await?;
        let hits = self
            .database
            .process(SearchEmbeddings {
                query,
                allowed: allowed_privacy(input.relationship),
                limit: input.limit,
            })
            .await?;
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn master_sees_all_privacy_levels() {
        assert_eq!(allowed_privacy(Relationship::Master).len(), 3);
    }

    #[test]
    fn stranger_sees_only_public() {
        assert_eq!(
            allowed_privacy(Relationship::Stranger),
            vec![PrivacyControlFlag::Public]
        );
    }

    #[test]
    fn acquaintance_sees_protected_and_public() {
        assert_eq!(
            allowed_privacy(Relationship::Acquaintance),
            vec![PrivacyControlFlag::Protected, PrivacyControlFlag::Public]
        );
    }

    #[tokio::test]
    async fn mock_embedder_produces_correct_dimensionality() {
        let v = MockEmbedder
            .process(EmbedRequest {
                text: "hello".into(),
            })
            .await
            .expect("embed");
        assert_eq!(v.to_vec().len(), EMBEDDING_DIMS);
    }
}
