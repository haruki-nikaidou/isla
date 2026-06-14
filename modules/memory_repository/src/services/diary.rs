//! Diary service — CRUD for daily journal entries.

use dynamic_message_extension::dynamic_context::{ContextRef, RedisPool};
use kanau::processor::Processor;
use time::Date;
use tracing::instrument;
use wakuwaku::amqp::AmqpPool;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::embedding::EntryRef;
use crate::entities::db::{
    PrivacyControlFlag,
    diary::{
        CreateDiary, DeleteDiary, DiaryEntity, FindDiaryByDate, FindDiaryById, ListDiaries,
        UpdateDiary, UpdateDiaryPrivacy,
    },
};
use crate::events::publish::{EntryPrivacyChanged, EntryRemoved, EntryUpserted};

#[derive(Clone)]
pub struct DiaryService {
    pub database: DatabaseProcessor,
    pub mq: AmqpPool,
    pub redis: RedisPool,
}

impl DiaryService {
    /// Publish an [`EntryUpserted`] for a diary so the RAG index (re)embeds it.
    async fn publish_upsert(&self, id: i64) -> Result<(), Error> {
        if let Some(d) = self.database.process(FindDiaryById { id }).await? {
            ContextRef::publish(
                &self.redis,
                &self.mq,
                EntryUpserted {
                    reference: EntryRef::diary(id),
                    privacy: d.privacy,
                    content: format!("{}\n{}\n{}", d.title, d.summary, d.content),
                },
            )
            .await?;
        }
        Ok(())
    }
}

/// Create a new diary entry for `date`. Fails if `title` is blank.
#[derive(Debug, Clone)]
pub struct CreateDiaryRequest {
    pub title: String,
    pub date: Date,
    pub summary: String,
    pub content: String,
    /// Audience visibility; pass [`PrivacyControlFlag::default`] for the
    /// `Protected` default if no explicit choice is needed.
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateDiaryRequest> for DiaryService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "CreateDiaryRequest", err, fields(date = %input.date))]
    async fn process(&self, input: CreateDiaryRequest) -> Result<i64, Error> {
        if input.title.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        let id = self
            .database
            .process(CreateDiary {
                title: input.title,
                date: input.date,
                summary: input.summary,
                content: input.content,
                privacy: input.privacy,
            })
            .await?;
        self.publish_upsert(id).await?;
        Ok(id)
    }
}

/// Retrieve a diary entry by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindDiaryRequest {
    pub id: i64,
}

impl Processor<FindDiaryRequest> for DiaryService {
    type Output = Option<DiaryEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindDiaryRequest", err, fields(id = input.id))]
    async fn process(&self, input: FindDiaryRequest) -> Result<Option<DiaryEntity>, Error> {
        Ok(self
            .database
            .process(FindDiaryById { id: input.id })
            .await?)
    }
}

/// Retrieve the diary entry for a specific date.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindDiaryByDateRequest {
    pub date: Date,
}

impl Processor<FindDiaryByDateRequest> for DiaryService {
    type Output = Option<DiaryEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindDiaryByDateRequest", err, fields(date = %input.date))]
    async fn process(&self, input: FindDiaryByDateRequest) -> Result<Option<DiaryEntity>, Error> {
        Ok(self
            .database
            .process(FindDiaryByDate { date: input.date })
            .await?)
    }
}

/// Update the text content of an existing diary entry. Fails if `title` is blank.
#[derive(Debug, Clone)]
pub struct UpdateDiaryRequest {
    pub id: i64,
    pub title: String,
    pub summary: String,
    pub content: String,
}

impl Processor<UpdateDiaryRequest> for DiaryService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateDiaryRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateDiaryRequest) -> Result<bool, Error> {
        if input.title.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        let updated = self
            .database
            .process(UpdateDiary {
                id: input.id,
                title: input.title,
                summary: input.summary,
                content: input.content,
            })
            .await?;
        if updated {
            self.publish_upsert(input.id).await?;
        }
        Ok(updated)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a diary entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateDiaryPrivacyRequest {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateDiaryPrivacyRequest> for DiaryService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateDiaryPrivacyRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateDiaryPrivacyRequest) -> Result<bool, Error> {
        let updated = self
            .database
            .process(UpdateDiaryPrivacy {
                id: input.id,
                privacy: input.privacy,
            })
            .await?;
        if updated {
            ContextRef::publish(
                &self.redis,
                &self.mq,
                EntryPrivacyChanged {
                    reference: EntryRef::diary(input.id),
                    privacy: input.privacy,
                },
            )
            .await?;
        }
        Ok(updated)
    }
}

/// Delete a diary entry by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteDiaryRequest {
    pub id: i64,
}

impl Processor<DeleteDiaryRequest> for DiaryService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteDiaryRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteDiaryRequest) -> Result<bool, Error> {
        let deleted = self.database.process(DeleteDiary { id: input.id }).await?;
        if deleted {
            ContextRef::publish(
                &self.redis,
                &self.mq,
                EntryRemoved {
                    reference: EntryRef::diary(input.id),
                },
            )
            .await?;
        }
        Ok(deleted)
    }
}

/// Paginated list of diary entries ordered by date (most recent first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListDiariesRequest {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListDiariesRequest> for DiaryService {
    type Output = Vec<DiaryEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListDiariesRequest", err)]
    async fn process(&self, input: ListDiariesRequest) -> Result<Vec<DiaryEntity>, Error> {
        Ok(self
            .database
            .process(ListDiaries {
                limit: input.limit,
                offset: input.offset,
            })
            .await?)
    }
}
