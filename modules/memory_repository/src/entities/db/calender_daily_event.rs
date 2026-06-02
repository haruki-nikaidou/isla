//! All-day calendar events without specific times.

use kanau::processor::Processor;
use time::{Date, PrimitiveDateTime};
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// An all-day event that spans an entire date (no specific time).
///
/// Used for holidays, birthdays, deadlines, and other date-based
/// events that don't have a specific start/end time.
#[derive(Debug, Clone)]
pub struct CalenderDailyEventEntity {
    /// Unique identifier for this event.
    pub id: i64,

    /// ID of the parent [`CalenderEntity`](super::calender::CalenderEntity).
    pub calendar_id: Uuid,

    /// Event title.
    pub title: String,

    /// Detailed description of the event.
    pub description: String,

    /// The date of this event (or first occurrence if repeating).
    pub date: Date,

    /// Recurrence pattern for this event.
    pub repeat: DailyEventRepeat,

    /// End date for recurring events (inclusive).
    pub repeat_until: Option<Date>,

    /// When this event was created.
    pub created: PrimitiveDateTime,

    /// When this event was last modified.
    pub updated: PrimitiveDateTime,

    /// Audience visibility for this event.
    pub privacy: PrivacyControlFlag,
}

/// Recurrence pattern for all-day events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "memory.daily_event_repeat")]
pub enum DailyEventRepeat {
    /// Single occurrence, no repetition.
    NoRepeat,
    /// Repeats on the same day each month.
    EveryMonth,
    /// Repeats on the same date each year (e.g., birthdays).
    EveryYear,
    /// Repeats every weekday (Monday through Friday).
    EveryWeekday,
}

/// Find a [`CalenderDailyEventEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindCalenderDailyEventById {
    pub id: i64,
}

impl Processor<FindCalenderDailyEventById> for DatabaseProcessor {
    type Output = Option<CalenderDailyEventEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindCalenderDailyEventById", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindCalenderDailyEventById,
    ) -> Result<Option<CalenderDailyEventEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderDailyEventEntity,
            r#"
            SELECT
                id,
                calendar_id,
                title,
                description,
                date,
                repeat AS "repeat: DailyEventRepeat",
                repeat_until,
                created,
                updated,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.calender_daily_event
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new all-day event into a calendar.
#[derive(Debug, Clone)]
pub struct CreateCalenderDailyEvent {
    pub calendar_id: Uuid,
    pub title: String,
    pub description: String,
    pub date: Date,
    pub repeat: DailyEventRepeat,
    pub repeat_until: Option<Date>,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateCalenderDailyEvent> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateCalenderDailyEvent", err, fields(calendar_id = %input.calendar_id))]
    async fn process(&self, input: CreateCalenderDailyEvent) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.calender_daily_event
                (calendar_id, title, description, date, repeat, repeat_until, privacy)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
            input.calendar_id,
            input.title,
            input.description,
            input.date,
            input.repeat as DailyEventRepeat,
            input.repeat_until,
            input.privacy as PrivacyControlFlag,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the mutable fields of an all-day calendar event.
#[derive(Debug, Clone)]
pub struct UpdateCalenderDailyEvent {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub date: Date,
    pub repeat: DailyEventRepeat,
    pub repeat_until: Option<Date>,
}

impl Processor<UpdateCalenderDailyEvent> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateCalenderDailyEvent", err, fields(id = input.id))]
    async fn process(&self, input: UpdateCalenderDailyEvent) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.calender_daily_event
            SET title = $2, description = $3, date = $4, repeat = $5, repeat_until = $6,
                updated = (now() AT TIME ZONE 'utc')
            WHERE id = $1
            "#,
            input.id,
            input.title,
            input.description,
            input.date,
            input.repeat as DailyEventRepeat,
            input.repeat_until,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete an all-day calendar event by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteCalenderDailyEvent {
    pub id: i64,
}

impl Processor<DeleteCalenderDailyEvent> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteCalenderDailyEvent", err, fields(id = input.id))]
    async fn process(&self, input: DeleteCalenderDailyEvent) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.calender_daily_event WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Reassign the [`PrivacyControlFlag`] of an all-day event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateCalenderDailyEventPrivacy {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateCalenderDailyEventPrivacy> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateCalenderDailyEventPrivacy", err, fields(id = input.id))]
    async fn process(
        &self,
        input: UpdateCalenderDailyEventPrivacy,
    ) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.calender_daily_event
            SET privacy = $2,
                updated = (now() AT TIME ZONE 'utc')
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

/// All-day events within a calendar that fall within the given date range (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCalenderDailyEventsInRange {
    pub calendar_id: Uuid,
    pub from: Date,
    pub to: Date,
}

impl Processor<ListCalenderDailyEventsInRange> for DatabaseProcessor {
    type Output = Vec<CalenderDailyEventEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListCalenderDailyEventsInRange", err, fields(calendar_id = %input.calendar_id))]
    async fn process(
        &self,
        input: ListCalenderDailyEventsInRange,
    ) -> Result<Vec<CalenderDailyEventEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderDailyEventEntity,
            r#"
            SELECT
                id,
                calendar_id,
                title,
                description,
                date,
                repeat AS "repeat: DailyEventRepeat",
                repeat_until,
                created,
                updated,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.calender_daily_event
            WHERE calendar_id = $1
              AND date >= $2
              AND date <= $3
            ORDER BY date ASC
            "#,
            input.calendar_id,
            input.from,
            input.to,
        )
        .fetch_all(self.db())
        .await
    }
}
