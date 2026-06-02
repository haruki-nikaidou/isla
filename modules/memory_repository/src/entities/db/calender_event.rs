//! Time-specific calendar events.

use kanau::processor::Processor;
use time::{Date, OffsetDateTime, PrimitiveDateTime};
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// A calendar event with a specific start time.
///
/// Used for meetings, appointments, reminders, and other events
/// that occur at a particular moment.
#[derive(Debug, Clone)]
pub struct CalenderEventEntity {
    /// Unique identifier for this event.
    pub id: i64,

    /// ID of the parent [`CalenderEntity`](super::calender::CalenderEntity).
    pub calendar_id: Uuid,

    /// Event title.
    pub title: String,

    /// Detailed description of the event.
    pub description: String,

    /// Start time of the event (with timezone).
    pub time: OffsetDateTime,

    /// Recurrence pattern for this event.
    pub repeat: CalenderEventRepeat,

    /// End date for recurring events (inclusive).
    pub repeat_until: Option<Date>,

    /// When this event was created.
    pub created_at: PrimitiveDateTime,

    /// When this event was last modified.
    pub updated_at: PrimitiveDateTime,

    /// Audience visibility for this event.
    pub privacy: PrivacyControlFlag,
}

/// Recurrence pattern for time-specific events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "memory.calender_event_repeat")]
pub enum CalenderEventRepeat {
    /// Single occurrence, no repetition.
    NoRepeat,
    /// Repeats at the same time every day.
    EveryDay,
    /// Repeats on the same day and time each month.
    EveryMonth,
    /// Repeats at the same time every weekday.
    EveryWeekday,
}

/// Find a [`CalenderEventEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindCalenderEventById {
    pub id: i64,
}

impl Processor<FindCalenderEventById> for DatabaseProcessor {
    type Output = Option<CalenderEventEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindCalenderEventById", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindCalenderEventById,
    ) -> Result<Option<CalenderEventEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderEventEntity,
            r#"
            SELECT
                id,
                calendar_id,
                title,
                description,
                time,
                repeat AS "repeat: CalenderEventRepeat",
                repeat_until,
                created_at,
                updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.calender_event
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new time-specific event into a calendar.
#[derive(Debug, Clone)]
pub struct CreateCalenderEvent {
    pub calendar_id: Uuid,
    pub title: String,
    pub description: String,
    pub time: OffsetDateTime,
    pub repeat: CalenderEventRepeat,
    pub repeat_until: Option<Date>,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateCalenderEvent> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateCalenderEvent", err, fields(calendar_id = %input.calendar_id))]
    async fn process(&self, input: CreateCalenderEvent) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.calender_event
                (calendar_id, title, description, time, repeat, repeat_until, privacy)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
            input.calendar_id,
            input.title,
            input.description,
            input.time,
            input.repeat as CalenderEventRepeat,
            input.repeat_until,
            input.privacy as PrivacyControlFlag,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update the mutable fields of a time-specific calendar event.
#[derive(Debug, Clone)]
pub struct UpdateCalenderEvent {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub time: OffsetDateTime,
    pub repeat: CalenderEventRepeat,
    pub repeat_until: Option<Date>,
}

impl Processor<UpdateCalenderEvent> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateCalenderEvent", err, fields(id = input.id))]
    async fn process(&self, input: UpdateCalenderEvent) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.calender_event
            SET title = $2, description = $3, time = $4, repeat = $5, repeat_until = $6,
                updated_at = (now() AT TIME ZONE 'utc')
            WHERE id = $1
            "#,
            input.id,
            input.title,
            input.description,
            input.time,
            input.repeat as CalenderEventRepeat,
            input.repeat_until,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete a time-specific calendar event by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteCalenderEvent {
    pub id: i64,
}

impl Processor<DeleteCalenderEvent> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteCalenderEvent", err, fields(id = input.id))]
    async fn process(&self, input: DeleteCalenderEvent) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.calender_event WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a time-specific event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateCalenderEventPrivacy {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateCalenderEventPrivacy> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateCalenderEventPrivacy", err, fields(id = input.id))]
    async fn process(&self, input: UpdateCalenderEventPrivacy) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.calender_event
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

/// Paginated list of events belonging to a calendar, ordered by time ascending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCalenderEventsByCalendar {
    pub calendar_id: Uuid,
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListCalenderEventsByCalendar> for DatabaseProcessor {
    type Output = Vec<CalenderEventEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListCalenderEventsByCalendar", err, fields(calendar_id = %input.calendar_id))]
    async fn process(
        &self,
        input: ListCalenderEventsByCalendar,
    ) -> Result<Vec<CalenderEventEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderEventEntity,
            r#"
            SELECT
                id,
                calendar_id,
                title,
                description,
                time,
                repeat AS "repeat: CalenderEventRepeat",
                repeat_until,
                created_at,
                updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.calender_event
            WHERE calendar_id = $1
            ORDER BY time ASC
            LIMIT $2 OFFSET $3
            "#,
            input.calendar_id,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}

/// Events within a calendar that overlap the given time window.
#[derive(Debug, Clone, Copy)]
pub struct ListCalenderEventsInRange {
    pub calendar_id: Uuid,
    pub from: OffsetDateTime,
    pub to: OffsetDateTime,
}

impl Processor<ListCalenderEventsInRange> for DatabaseProcessor {
    type Output = Vec<CalenderEventEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListCalenderEventsInRange", err, fields(calendar_id = %input.calendar_id))]
    async fn process(
        &self,
        input: ListCalenderEventsInRange,
    ) -> Result<Vec<CalenderEventEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderEventEntity,
            r#"
            SELECT
                id,
                calendar_id,
                title,
                description,
                time,
                repeat AS "repeat: CalenderEventRepeat",
                repeat_until,
                created_at,
                updated_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.calender_event
            WHERE calendar_id = $1
              AND time >= $2
              AND time <= $3
            ORDER BY time ASC
            "#,
            input.calendar_id,
            input.from,
            input.to,
        )
        .fetch_all(self.db())
        .await
    }
}
