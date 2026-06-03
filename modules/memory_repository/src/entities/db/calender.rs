//! Calendar containers for organizing events and tasks.

use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

/// A calendar that groups related events and tasks.
///
/// Calendars provide organizational structure and access control for
/// scheduling data. Multiple calendars can coexist (e.g., work, personal).
#[derive(Debug, Clone)]
pub struct CalenderEntity {
    /// Unique identifier for this calendar.
    pub id: Uuid,

    /// Display name for this calendar.
    pub name: String,

    /// Description of the calendar's purpose.
    pub description: String,

    /// When this calendar was created.
    pub created_at: PrimitiveDateTime,

    /// Permission scopes that can access this calendar.
    pub scopes: Vec<String>,
}

/// Find a [`CalenderEntity`] by its UUID primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindCalenderById {
    pub id: Uuid,
}

impl Processor<FindCalenderById> for DatabaseProcessor {
    type Output = Option<CalenderEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindCalenderById", err, fields(id = %input.id))]
    async fn process(
        &self,
        input: FindCalenderById,
    ) -> Result<Option<CalenderEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderEntity,
            r#"
            SELECT id, name, description, created_at, scopes
            FROM memory.calender
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new calendar. The caller supplies the `id` (typically `Uuid::new_v4()`).
#[derive(Debug, Clone)]
pub struct CreateCalender {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub scopes: Vec<String>,
}

impl Processor<CreateCalender> for DatabaseProcessor {
    type Output = Uuid;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateCalender", err, fields(id = %input.id))]
    async fn process(&self, input: CreateCalender) -> Result<Uuid, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.calender (id, name, description, scopes)
            VALUES ($1, $2, $3, $4)
            RETURNING id
            "#,
            input.id,
            input.name,
            input.description,
            &input.scopes,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the mutable fields of an existing calendar.
#[derive(Debug, Clone)]
pub struct UpdateCalender {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub scopes: Vec<String>,
}

impl Processor<UpdateCalender> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateCalender", err, fields(id = %input.id))]
    async fn process(&self, input: UpdateCalender) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.calender
            SET name = $2, description = $3, scopes = $4
            WHERE id = $1
            "#,
            input.id,
            input.name,
            input.description,
            &input.scopes,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete a calendar by its UUID (cascades to all events and tasks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteCalender {
    pub id: Uuid,
}

impl Processor<DeleteCalender> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteCalender", err, fields(id = %input.id))]
    async fn process(&self, input: DeleteCalender) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!("DELETE FROM memory.calender WHERE id = $1", input.id,)
            .execute(self.db())
            .await?
            .rows_affected();
        Ok(rows > 0)
    }
}

/// Paginated list of all calendars ordered by creation time (newest first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCalenders {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListCalenders> for DatabaseProcessor {
    type Output = Vec<CalenderEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListCalenders", err)]
    async fn process(&self, input: ListCalenders) -> Result<Vec<CalenderEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderEntity,
            r#"
            SELECT id, name, description, created_at, scopes
            FROM memory.calender
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            "#,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}
