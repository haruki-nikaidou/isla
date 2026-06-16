//! Calendar service — CRUD for calendars, events, all-day events, tasks, and task dependencies.

use std::sync::Arc;

use dynamic_message_extension::cluster_authorized::ClusterMessageSender;
use dynamic_message_extension::dynamic_context::{ContextRef, RedisPool};
use kanau::processor::Processor;
use time::{Date, OffsetDateTime, PrimitiveDateTime};
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::{Error, sqlx::DatabaseProcessor};

use crate::entities::db::embedding::EntryRef;
use crate::entities::db::{
    PrivacyControlFlag,
    calender::{
        CalenderEntity, CreateCalender, DeleteCalender, FindCalenderById, ListCalenders,
        UpdateCalender,
    },
    calender_daily_event::{
        CalenderDailyEventEntity, CreateCalenderDailyEvent, DailyEventRepeat,
        DeleteCalenderDailyEvent, FindCalenderDailyEventById, ListCalenderDailyEventsInRange,
        UpdateCalenderDailyEvent, UpdateCalenderDailyEventPrivacy,
    },
    calender_event::{
        CalenderEventEntity, CalenderEventRepeat, CreateCalenderEvent, DeleteCalenderEvent,
        FindCalenderEventById, ListCalenderEventsByCalendar, ListCalenderEventsInRange,
        UpdateCalenderEvent, UpdateCalenderEventPrivacy,
    },
    calender_task::{
        CalenderTaskDependencyEntity, CalenderTaskEntity, CalenderTaskStatus, CreateCalenderTask,
        CreateCalenderTaskDependency, DeleteCalenderTask, DeleteCalenderTaskDependency,
        FindCalenderTaskById, ListCalenderTaskDependenciesByBlocked, ListCalenderTasksByCalendar,
        UpdateCalenderTask, UpdateCalenderTaskPrivacy, UpdateCalenderTaskStatus,
    },
};
use crate::entities::redis::EntryContent;
use crate::events::publish::EntryUpserted;

#[derive(Clone)]
pub struct CalenderService {
    pub database: DatabaseProcessor,
    pub sender: Arc<ClusterMessageSender>,
    pub redis: RedisPool,
}

/// Publish an [`EntryUpserted`] for a calendar event so the RAG index
/// (re)embeds it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishCalenderEventUpsertRequest {
    pub id: i64,
}

impl Processor<PublishCalenderEventUpsertRequest> for CalenderService {
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "PublishCalenderEventUpsertRequest", err, fields(id = input.id))]
    async fn process(&self, input: PublishCalenderEventUpsertRequest) -> Result<(), Error> {
        let id = input.id;
        if let Some(e) = self.database.process(FindCalenderEventById { id }).await? {
            let text = format!("{}\n{}", e.title, e.description);
            let content = ContextRef::store(
                &self.redis,
                EntryContent {
                    reference: EntryRef::calender_event(id),
                    privacy: e.privacy,
                    content: text,
                },
            )
            .await?;
            ContextRef::publish(
                &self.redis,
                &self.sender,
                EntryUpserted {
                    reference: EntryRef::calender_event(id),
                    privacy: e.privacy,
                    content,
                },
            )
            .await?;
        }
        Ok(())
    }
}

// ─── Calendar CRUD ────────────────────────────────────────────────────────────

/// Create a new calendar. A fresh `Uuid` is generated automatically.
#[derive(Debug, Clone)]
pub struct CreateCalenderRequest {
    pub name: String,
    pub description: String,
    pub scopes: Vec<String>,
}

impl Processor<CreateCalenderRequest> for CalenderService {
    type Output = Uuid;
    type Error = Error;

    #[instrument(skip_all, name = "CreateCalenderRequest", err)]
    async fn process(&self, input: CreateCalenderRequest) -> Result<Uuid, Error> {
        if input.name.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        let id = Uuid::new_v4();
        self.database
            .process(CreateCalender {
                id,
                name: input.name,
                description: input.description,
                scopes: input.scopes,
            })
            .await?;
        Ok(id)
    }
}

/// Retrieve a calendar by its UUID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindCalenderRequest {
    pub id: Uuid,
}

impl Processor<FindCalenderRequest> for CalenderService {
    type Output = Option<CalenderEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindCalenderRequest", err, fields(id = %input.id))]
    async fn process(&self, input: FindCalenderRequest) -> Result<Option<CalenderEntity>, Error> {
        Ok(self
            .database
            .process(FindCalenderById { id: input.id })
            .await?)
    }
}

/// Update the name, description, and scopes of an existing calendar.
#[derive(Debug, Clone)]
pub struct UpdateCalenderRequest {
    pub id: Uuid,
    pub name: String,
    pub description: String,
    pub scopes: Vec<String>,
}

impl Processor<UpdateCalenderRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateCalenderRequest", err, fields(id = %input.id))]
    async fn process(&self, input: UpdateCalenderRequest) -> Result<bool, Error> {
        if input.name.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        Ok(self
            .database
            .process(UpdateCalender {
                id: input.id,
                name: input.name,
                description: input.description,
                scopes: input.scopes,
            })
            .await?)
    }
}

/// Delete a calendar (cascades to all child entities).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteCalenderRequest {
    pub id: Uuid,
}

impl Processor<DeleteCalenderRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteCalenderRequest", err, fields(id = %input.id))]
    async fn process(&self, input: DeleteCalenderRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteCalender { id: input.id })
            .await?)
    }
}

/// Paginated list of calendars (newest first).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCalendersRequest {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListCalendersRequest> for CalenderService {
    type Output = Vec<CalenderEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListCalendersRequest", err)]
    async fn process(&self, input: ListCalendersRequest) -> Result<Vec<CalenderEntity>, Error> {
        Ok(self
            .database
            .process(ListCalenders {
                limit: input.limit,
                offset: input.offset,
            })
            .await?)
    }
}

// ─── Time-specific event CRUD ─────────────────────────────────────────────────

/// Create a new time-specific event in a calendar.
#[derive(Debug, Clone)]
pub struct CreateCalenderEventRequest {
    pub calendar_id: Uuid,
    pub title: String,
    pub description: String,
    pub time: OffsetDateTime,
    pub repeat: CalenderEventRepeat,
    pub repeat_until: Option<Date>,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateCalenderEventRequest> for CalenderService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "CreateCalenderEventRequest", err,
        fields(calendar_id = %input.calendar_id))]
    async fn process(&self, input: CreateCalenderEventRequest) -> Result<i64, Error> {
        if input.title.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        let id = self
            .database
            .process(CreateCalenderEvent {
                calendar_id: input.calendar_id,
                title: input.title,
                description: input.description,
                time: input.time,
                repeat: input.repeat,
                repeat_until: input.repeat_until,
                privacy: input.privacy,
            })
            .await?;
        self.process(PublishCalenderEventUpsertRequest { id })
            .await?;
        Ok(id)
    }
}

/// Retrieve a time-specific event by its id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindCalenderEventRequest {
    pub id: i64,
}

impl Processor<FindCalenderEventRequest> for CalenderService {
    type Output = Option<CalenderEventEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindCalenderEventRequest", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindCalenderEventRequest,
    ) -> Result<Option<CalenderEventEntity>, Error> {
        Ok(self
            .database
            .process(FindCalenderEventById { id: input.id })
            .await?)
    }
}

/// Update the details of a time-specific event.
#[derive(Debug, Clone)]
pub struct UpdateCalenderEventRequest {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub time: OffsetDateTime,
    pub repeat: CalenderEventRepeat,
    pub repeat_until: Option<Date>,
}

impl Processor<UpdateCalenderEventRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateCalenderEventRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateCalenderEventRequest) -> Result<bool, Error> {
        if input.title.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        let updated = self
            .database
            .process(UpdateCalenderEvent {
                id: input.id,
                title: input.title,
                description: input.description,
                time: input.time,
                repeat: input.repeat,
                repeat_until: input.repeat_until,
            })
            .await?;
        if updated {
            self.process(PublishCalenderEventUpsertRequest { id: input.id })
                .await?;
        }
        Ok(updated)
    }
}

/// Delete a time-specific event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteCalenderEventRequest {
    pub id: i64,
}

impl Processor<DeleteCalenderEventRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteCalenderEventRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteCalenderEventRequest) -> Result<bool, Error> {
        let deleted = self
            .database
            .process(DeleteCalenderEvent { id: input.id })
            .await?;
        Ok(deleted)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a time-specific event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateCalenderEventPrivacyRequest {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateCalenderEventPrivacyRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateCalenderEventPrivacyRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateCalenderEventPrivacyRequest) -> Result<bool, Error> {
        let updated = self
            .database
            .process(UpdateCalenderEventPrivacy {
                id: input.id,
                privacy: input.privacy,
            })
            .await?;
        Ok(updated)
    }
}

/// All time-specific events in a calendar that fall within a time window.
#[derive(Debug, Clone, Copy)]
pub struct ListEventsInRangeRequest {
    pub calendar_id: Uuid,
    pub from: OffsetDateTime,
    pub to: OffsetDateTime,
}

impl Processor<ListEventsInRangeRequest> for CalenderService {
    type Output = Vec<CalenderEventEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListEventsInRangeRequest", err,
        fields(calendar_id = %input.calendar_id))]
    async fn process(
        &self,
        input: ListEventsInRangeRequest,
    ) -> Result<Vec<CalenderEventEntity>, Error> {
        Ok(self
            .database
            .process(ListCalenderEventsInRange {
                calendar_id: input.calendar_id,
                from: input.from,
                to: input.to,
            })
            .await?)
    }
}

/// Paginated list of events for a calendar ordered by time ascending.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCalenderEventsRequest {
    pub calendar_id: Uuid,
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListCalenderEventsRequest> for CalenderService {
    type Output = Vec<CalenderEventEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListCalenderEventsRequest", err,
        fields(calendar_id = %input.calendar_id))]
    async fn process(
        &self,
        input: ListCalenderEventsRequest,
    ) -> Result<Vec<CalenderEventEntity>, Error> {
        Ok(self
            .database
            .process(ListCalenderEventsByCalendar {
                calendar_id: input.calendar_id,
                limit: input.limit,
                offset: input.offset,
            })
            .await?)
    }
}

// ─── All-day event CRUD ───────────────────────────────────────────────────────

/// Create a new all-day event in a calendar.
#[derive(Debug, Clone)]
pub struct CreateDailyEventRequest {
    pub calendar_id: Uuid,
    pub title: String,
    pub description: String,
    pub date: Date,
    pub repeat: DailyEventRepeat,
    pub repeat_until: Option<Date>,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateDailyEventRequest> for CalenderService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "CreateDailyEventRequest", err,
        fields(calendar_id = %input.calendar_id))]
    async fn process(&self, input: CreateDailyEventRequest) -> Result<i64, Error> {
        if input.title.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        Ok(self
            .database
            .process(CreateCalenderDailyEvent {
                calendar_id: input.calendar_id,
                title: input.title,
                description: input.description,
                date: input.date,
                repeat: input.repeat,
                repeat_until: input.repeat_until,
                privacy: input.privacy,
            })
            .await?)
    }
}

/// Retrieve an all-day event by its id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindDailyEventRequest {
    pub id: i64,
}

impl Processor<FindDailyEventRequest> for CalenderService {
    type Output = Option<CalenderDailyEventEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindDailyEventRequest", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindDailyEventRequest,
    ) -> Result<Option<CalenderDailyEventEntity>, Error> {
        Ok(self
            .database
            .process(FindCalenderDailyEventById { id: input.id })
            .await?)
    }
}

/// Update the details of an all-day event.
#[derive(Debug, Clone)]
pub struct UpdateDailyEventRequest {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub date: Date,
    pub repeat: DailyEventRepeat,
    pub repeat_until: Option<Date>,
}

impl Processor<UpdateDailyEventRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateDailyEventRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateDailyEventRequest) -> Result<bool, Error> {
        if input.title.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        Ok(self
            .database
            .process(UpdateCalenderDailyEvent {
                id: input.id,
                title: input.title,
                description: input.description,
                date: input.date,
                repeat: input.repeat,
                repeat_until: input.repeat_until,
            })
            .await?)
    }
}

/// Delete an all-day event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteDailyEventRequest {
    pub id: i64,
}

impl Processor<DeleteDailyEventRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteDailyEventRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteDailyEventRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteCalenderDailyEvent { id: input.id })
            .await?)
    }
}

/// Reassign the [`PrivacyControlFlag`] of an all-day event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateDailyEventPrivacyRequest {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateDailyEventPrivacyRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateDailyEventPrivacyRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateDailyEventPrivacyRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(UpdateCalenderDailyEventPrivacy {
                id: input.id,
                privacy: input.privacy,
            })
            .await?)
    }
}

/// All-day events in a calendar that fall within a date range (inclusive).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListDailyEventsInRangeRequest {
    pub calendar_id: Uuid,
    pub from: Date,
    pub to: Date,
}

impl Processor<ListDailyEventsInRangeRequest> for CalenderService {
    type Output = Vec<CalenderDailyEventEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListDailyEventsInRangeRequest", err,
        fields(calendar_id = %input.calendar_id))]
    async fn process(
        &self,
        input: ListDailyEventsInRangeRequest,
    ) -> Result<Vec<CalenderDailyEventEntity>, Error> {
        Ok(self
            .database
            .process(ListCalenderDailyEventsInRange {
                calendar_id: input.calendar_id,
                from: input.from,
                to: input.to,
            })
            .await?)
    }
}

// ─── Task CRUD ────────────────────────────────────────────────────────────────

/// Create a new task in a calendar.
#[derive(Debug, Clone)]
pub struct CreateTaskRequest {
    pub calendar_id: Uuid,
    pub title: String,
    pub description: String,
    pub start_at: PrimitiveDateTime,
    pub deadline: PrimitiveDateTime,
    pub status: CalenderTaskStatus,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateTaskRequest> for CalenderService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "CreateTaskRequest", err, fields(calendar_id = %input.calendar_id))]
    async fn process(&self, input: CreateTaskRequest) -> Result<i64, Error> {
        if input.title.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        if input.deadline < input.start_at {
            return Err(Error::InvalidInput);
        }
        Ok(self
            .database
            .process(CreateCalenderTask {
                calendar_id: input.calendar_id,
                title: input.title,
                description: input.description,
                start_at: input.start_at,
                deadline: input.deadline,
                status: input.status,
                privacy: input.privacy,
            })
            .await?)
    }
}

/// Retrieve a task by its id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindTaskRequest {
    pub id: i64,
}

impl Processor<FindTaskRequest> for CalenderService {
    type Output = Option<CalenderTaskEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "FindTaskRequest", err, fields(id = input.id))]
    async fn process(&self, input: FindTaskRequest) -> Result<Option<CalenderTaskEntity>, Error> {
        Ok(self
            .database
            .process(FindCalenderTaskById { id: input.id })
            .await?)
    }
}

/// Update the descriptive fields and time window of a task.
#[derive(Debug, Clone)]
pub struct UpdateTaskRequest {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub start_at: PrimitiveDateTime,
    pub deadline: PrimitiveDateTime,
}

impl Processor<UpdateTaskRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateTaskRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateTaskRequest) -> Result<bool, Error> {
        if input.title.trim().is_empty() {
            return Err(Error::InvalidInput);
        }
        if input.deadline < input.start_at {
            return Err(Error::InvalidInput);
        }
        Ok(self
            .database
            .process(UpdateCalenderTask {
                id: input.id,
                title: input.title,
                description: input.description,
                start_at: input.start_at,
                deadline: input.deadline,
            })
            .await?)
    }
}

/// Transition a task to a new status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateTaskStatusRequest {
    pub id: i64,
    pub status: CalenderTaskStatus,
}

impl Processor<UpdateTaskStatusRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateTaskStatusRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateTaskStatusRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(UpdateCalenderTaskStatus {
                id: input.id,
                status: input.status,
            })
            .await?)
    }
}

/// Reassign the [`PrivacyControlFlag`] of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateTaskPrivacyRequest {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateTaskPrivacyRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "UpdateTaskPrivacyRequest", err, fields(id = input.id))]
    async fn process(&self, input: UpdateTaskPrivacyRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(UpdateCalenderTaskPrivacy {
                id: input.id,
                privacy: input.privacy,
            })
            .await?)
    }
}

/// Delete a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteTaskRequest {
    pub id: i64,
}

impl Processor<DeleteTaskRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "DeleteTaskRequest", err, fields(id = input.id))]
    async fn process(&self, input: DeleteTaskRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteCalenderTask { id: input.id })
            .await?)
    }
}

/// Paginated list of tasks for a calendar, optionally filtered by status.
#[derive(Debug, Clone, Copy)]
pub struct ListTasksByCalendarRequest {
    pub calendar_id: Uuid,
    pub status: Option<CalenderTaskStatus>,
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListTasksByCalendarRequest> for CalenderService {
    type Output = Vec<CalenderTaskEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListTasksByCalendarRequest", err,
        fields(calendar_id = %input.calendar_id))]
    async fn process(
        &self,
        input: ListTasksByCalendarRequest,
    ) -> Result<Vec<CalenderTaskEntity>, Error> {
        Ok(self
            .database
            .process(ListCalenderTasksByCalendar {
                calendar_id: input.calendar_id,
                status: input.status,
                limit: input.limit,
                offset: input.offset,
            })
            .await?)
    }
}

// ─── Task dependency CRUD ─────────────────────────────────────────────────────

/// Record that `blocking_task_id` must finish before `blocked_task_id` can start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddTaskDependencyRequest {
    pub blocking_task_id: i64,
    pub blocked_task_id: i64,
}

impl Processor<AddTaskDependencyRequest> for CalenderService {
    type Output = i64;
    type Error = Error;

    #[instrument(skip_all, name = "AddTaskDependencyRequest", err,
        fields(blocking = input.blocking_task_id, blocked = input.blocked_task_id))]
    async fn process(&self, input: AddTaskDependencyRequest) -> Result<i64, Error> {
        if input.blocking_task_id == input.blocked_task_id {
            return Err(Error::InvalidInput);
        }
        Ok(self
            .database
            .process(CreateCalenderTaskDependency {
                blocking_task_id: input.blocking_task_id,
                blocked_task_id: input.blocked_task_id,
            })
            .await?)
    }
}

/// Remove a task dependency by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoveTaskDependencyRequest {
    pub id: i64,
}

impl Processor<RemoveTaskDependencyRequest> for CalenderService {
    type Output = bool;
    type Error = Error;

    #[instrument(skip_all, name = "RemoveTaskDependencyRequest", err, fields(id = input.id))]
    async fn process(&self, input: RemoveTaskDependencyRequest) -> Result<bool, Error> {
        Ok(self
            .database
            .process(DeleteCalenderTaskDependency { id: input.id })
            .await?)
    }
}

/// All dependencies where the given task is downstream (blocked by other tasks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListTaskBlockersRequest {
    pub blocked_task_id: i64,
}

impl Processor<ListTaskBlockersRequest> for CalenderService {
    type Output = Vec<CalenderTaskDependencyEntity>;
    type Error = Error;

    #[instrument(skip_all, name = "ListTaskBlockersRequest", err,
        fields(blocked_task_id = input.blocked_task_id))]
    async fn process(
        &self,
        input: ListTaskBlockersRequest,
    ) -> Result<Vec<CalenderTaskDependencyEntity>, Error> {
        Ok(self
            .database
            .process(ListCalenderTaskDependenciesByBlocked {
                blocked_task_id: input.blocked_task_id,
            })
            .await?)
    }
}
