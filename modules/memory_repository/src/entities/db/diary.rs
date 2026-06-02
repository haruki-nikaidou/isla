//! Daily journal entries for long-term memory.

use kanau::processor::Processor;
use time::{Date, PrimitiveDateTime};
use tracing::instrument;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// A daily diary entry summarizing events and reflections.
///
/// Diaries provide a high-level record of each day, helping Isla maintain
/// continuity and recall past events without loading full conversation histories.
#[derive(Debug, Clone)]
pub struct DiaryEntity {
    /// Unique identifier for this diary entry.
    pub id: i64,

    /// Title or headline for this day's entry.
    pub title: String,

    /// The date this entry covers.
    pub date: Date,

    /// Brief summary of the day's events.
    pub summary: String,

    /// Full diary content with reflections and details.
    pub content: String,

    /// When this entry was first written.
    pub created_at: PrimitiveDateTime,

    /// When this entry was last edited.
    pub updated_at: PrimitiveDateTime,

    /// Audience visibility for this entry; gates whether the agent may
    /// surface it as context to a given peer.
    pub privacy: PrivacyControlFlag,
}

/// Find a [`DiaryEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindDiaryById {
    pub id: i64,
}

impl Processor<FindDiaryById> for DatabaseProcessor {
    type Output = Option<DiaryEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindDiaryById", err, fields(id = input.id))]
    async fn process(&self, input: FindDiaryById) -> Result<Option<DiaryEntity>, sqlx::Error> {
        sqlx::query_as!(
            DiaryEntity,
            r#"
            SELECT
                id, title, date, summary, content, created_at, updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.diary
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Look up the diary entry for a specific date (the `date` column is UNIQUE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindDiaryByDate {
    pub date: Date,
}

impl Processor<FindDiaryByDate> for DatabaseProcessor {
    type Output = Option<DiaryEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindDiaryByDate", err, fields(date = %input.date))]
    async fn process(&self, input: FindDiaryByDate) -> Result<Option<DiaryEntity>, sqlx::Error> {
        sqlx::query_as!(
            DiaryEntity,
            r#"
            SELECT
                id, title, date, summary, content, created_at, updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.diary
            WHERE date = $1
            LIMIT 1
            "#,
            input.date,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new diary entry.
#[derive(Debug, Clone)]
pub struct CreateDiary {
    pub title: String,
    pub date: Date,
    pub summary: String,
    pub content: String,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateDiary> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateDiary", err, fields(date = %input.date))]
    async fn process(&self, input: CreateDiary) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.diary (title, date, summary, content, privacy)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
            "#,
            input.title,
            input.date,
            input.summary,
            input.content,
            input.privacy as PrivacyControlFlag,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the text content of an existing diary entry.
#[derive(Debug, Clone)]
pub struct UpdateDiary {
    pub id: i64,
    pub title: String,
    pub summary: String,
    pub content: String,
}

impl Processor<UpdateDiary> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateDiary", err, fields(id = input.id))]
    async fn process(&self, input: UpdateDiary) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.diary
            SET title = $2, summary = $3, content = $4,
                updated_at = (now() AT TIME ZONE 'utc')
            WHERE id = $1
            "#,
            input.id,
            input.title,
            input.summary,
            input.content,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete a diary entry by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteDiary {
    pub id: i64,
}

impl Processor<DeleteDiary> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteDiary", err, fields(id = input.id))]
    async fn process(&self, input: DeleteDiary) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.diary WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a diary entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateDiaryPrivacy {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateDiaryPrivacy> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateDiaryPrivacy", err, fields(id = input.id))]
    async fn process(&self, input: UpdateDiaryPrivacy) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.diary
            SET privacy = $2,
                updated_at = (now() AT TIME ZONE 'utc')
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

/// Paginated list of diary entries ordered by date descending (most recent first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListDiaries {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListDiaries> for DatabaseProcessor {
    type Output = Vec<DiaryEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListDiaries", err)]
    async fn process(&self, input: ListDiaries) -> Result<Vec<DiaryEntity>, sqlx::Error> {
        sqlx::query_as!(
            DiaryEntity,
            r#"
            SELECT
                id, title, date, summary, content, created_at, updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.diary
            ORDER BY date DESC
            LIMIT $1 OFFSET $2
            "#,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}