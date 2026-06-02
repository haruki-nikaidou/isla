//! Task management within calendars.

use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

use super::PrivacyControlFlag;

/// A task with a deadline, tracked within a calendar.
///
/// Tasks differ from events in that they have a duration window (start to deadline)
/// and progress tracking, rather than occurring at a specific moment.
#[derive(Debug, Clone)]
pub struct CalenderTaskEntity {
    /// Unique identifier for this task.
    pub id: i64,

    /// ID of the parent [`CalenderEntity`](super::calender::CalenderEntity).
    pub calendar_id: Uuid,

    /// Task title.
    pub title: String,

    /// Detailed description of what needs to be done.
    pub description: String,

    /// When work on this task can begin.
    pub start_at: PrimitiveDateTime,

    /// When this task must be completed.
    pub deadline: PrimitiveDateTime,

    /// Current progress state of the task.
    pub status: CalenderTaskStatus,

    /// When the task status was last changed.
    pub status_update_at: PrimitiveDateTime,

    /// Audience visibility for this task.
    pub privacy: PrivacyControlFlag,
}

/// Progress state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "memory.calender_task_status")]
pub enum CalenderTaskStatus {
    /// Task has not been started.
    Pending,
    /// Task is currently being worked on.
    Doing,
    /// Task has been completed.
    Finished,
}

/// A dependency relationship between two tasks.
///
/// Represents that one task must be completed before another can start.
#[derive(Debug, Clone)]
pub struct CalenderTaskDependencyEntity {
    /// Unique identifier for this dependency.
    pub id: i64,

    /// ID of the task that must complete first.
    pub blocking_task_id: i64,

    /// ID of the task that is waiting on the blocker.
    pub blocked_task_id: i64,
}

/// Find a [`CalenderTaskEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindCalenderTaskById {
    pub id: i64,
}

impl Processor<FindCalenderTaskById> for DatabaseProcessor {
    type Output = Option<CalenderTaskEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindCalenderTaskById", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindCalenderTaskById,
    ) -> Result<Option<CalenderTaskEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderTaskEntity,
            r#"
            SELECT
                id,
                calendar_id,
                title,
                description,
                start_at,
                deadline,
                status AS "status: CalenderTaskStatus",
                status_update_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.calender_task
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Find a [`CalenderTaskDependencyEntity`] by its bigserial primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindCalenderTaskDependencyById {
    pub id: i64,
}

impl Processor<FindCalenderTaskDependencyById> for DatabaseProcessor {
    type Output = Option<CalenderTaskDependencyEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindCalenderTaskDependencyById", err, fields(id = input.id))]
    async fn process(
        &self,
        input: FindCalenderTaskDependencyById,
    ) -> Result<Option<CalenderTaskDependencyEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderTaskDependencyEntity,
            r#"
            SELECT id, blocking_task_id, blocked_task_id
            FROM memory.calender_task_dependency
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

/// Insert a new task into a calendar.
#[derive(Debug, Clone)]
pub struct CreateCalenderTask {
    pub calendar_id: Uuid,
    pub title: String,
    pub description: String,
    pub start_at: PrimitiveDateTime,
    pub deadline: PrimitiveDateTime,
    pub status: CalenderTaskStatus,
    pub privacy: PrivacyControlFlag,
}

impl Processor<CreateCalenderTask> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateCalenderTask", err, fields(calendar_id = %input.calendar_id))]
    async fn process(&self, input: CreateCalenderTask) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.calender_task
                (calendar_id, title, description, start_at, deadline, status, privacy)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            "#,
            input.calendar_id,
            input.title,
            input.description,
            input.start_at,
            input.deadline,
            input.status as CalenderTaskStatus,
            input.privacy as PrivacyControlFlag,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Update descriptive fields and time window of a task (not its status).
#[derive(Debug, Clone)]
pub struct UpdateCalenderTask {
    pub id: i64,
    pub title: String,
    pub description: String,
    pub start_at: PrimitiveDateTime,
    pub deadline: PrimitiveDateTime,
}

impl Processor<UpdateCalenderTask> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateCalenderTask", err, fields(id = input.id))]
    async fn process(&self, input: UpdateCalenderTask) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.calender_task
            SET title = $2, description = $3, start_at = $4, deadline = $5
            WHERE id = $1
            "#,
            input.id,
            input.title,
            input.description,
            input.start_at,
            input.deadline,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Transition a task's status and stamp `status_update_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateCalenderTaskStatus {
    pub id: i64,
    pub status: CalenderTaskStatus,
}

impl Processor<UpdateCalenderTaskStatus> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateCalenderTaskStatus", err, fields(id = input.id))]
    async fn process(&self, input: UpdateCalenderTaskStatus) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.calender_task
            SET status = $2, status_update_at = (now() AT TIME ZONE 'utc')
            WHERE id = $1
            "#,
            input.id,
            input.status as CalenderTaskStatus,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Delete a task by its primary key (cascades dependency rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteCalenderTask {
    pub id: i64,
}

impl Processor<DeleteCalenderTask> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteCalenderTask", err, fields(id = input.id))]
    async fn process(&self, input: DeleteCalenderTask) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.calender_task WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// Paginated list of tasks for a calendar, optionally filtered by status, ordered by deadline.
#[derive(Debug, Clone, Copy)]
pub struct ListCalenderTasksByCalendar {
    pub calendar_id: Uuid,
    pub status: Option<CalenderTaskStatus>,
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListCalenderTasksByCalendar> for DatabaseProcessor {
    type Output = Vec<CalenderTaskEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListCalenderTasksByCalendar", err, fields(calendar_id = %input.calendar_id))]
    async fn process(
        &self,
        input: ListCalenderTasksByCalendar,
    ) -> Result<Vec<CalenderTaskEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderTaskEntity,
            r#"
            SELECT
                id,
                calendar_id,
                title,
                description,
                start_at,
                deadline,
                status AS "status: CalenderTaskStatus",
                status_update_at,
                privacy AS "privacy: PrivacyControlFlag"
            FROM memory.calender_task
            WHERE calendar_id = $1
              AND ($2::memory.calender_task_status IS NULL OR status = $2)
            ORDER BY deadline ASC
            LIMIT $3 OFFSET $4
            "#,
            input.calendar_id,
            input.status as Option<CalenderTaskStatus>,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}

/// Reassign the [`PrivacyControlFlag`] of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateCalenderTaskPrivacy {
    pub id: i64,
    pub privacy: PrivacyControlFlag,
}

impl Processor<UpdateCalenderTaskPrivacy> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:UpdateCalenderTaskPrivacy", err, fields(id = input.id))]
    async fn process(&self, input: UpdateCalenderTaskPrivacy) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            r#"
            UPDATE memory.calender_task
            SET privacy = $2
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

/// Add a blocking dependency between two tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreateCalenderTaskDependency {
    pub blocking_task_id: i64,
    pub blocked_task_id: i64,
}

impl Processor<CreateCalenderTaskDependency> for DatabaseProcessor {
    type Output = i64;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:CreateCalenderTaskDependency", err,
        fields(blocking = input.blocking_task_id, blocked = input.blocked_task_id))]
    async fn process(&self, input: CreateCalenderTaskDependency) -> Result<i64, sqlx::Error> {
        sqlx::query_scalar!(
            r#"
            INSERT INTO memory.calender_task_dependency (blocking_task_id, blocked_task_id)
            VALUES ($1, $2)
            RETURNING id
            "#,
            input.blocking_task_id,
            input.blocked_task_id,
        )
        .fetch_one(self.db())
        .await
    }
}

/// Remove a task dependency by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeleteCalenderTaskDependency {
    pub id: i64,
}

impl Processor<DeleteCalenderTaskDependency> for DatabaseProcessor {
    type Output = bool;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:DeleteCalenderTaskDependency", err, fields(id = input.id))]
    async fn process(&self, input: DeleteCalenderTaskDependency) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query!(
            "DELETE FROM memory.calender_task_dependency WHERE id = $1",
            input.id,
        )
        .execute(self.db())
        .await?
        .rows_affected();
        Ok(rows > 0)
    }
}

/// All dependencies where the given task is the blocked (downstream) one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCalenderTaskDependenciesByBlocked {
    pub blocked_task_id: i64,
}

impl Processor<ListCalenderTaskDependenciesByBlocked> for DatabaseProcessor {
    type Output = Vec<CalenderTaskDependencyEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:ListCalenderTaskDependenciesByBlocked", err,
        fields(blocked_task_id = input.blocked_task_id))]
    async fn process(
        &self,
        input: ListCalenderTaskDependenciesByBlocked,
    ) -> Result<Vec<CalenderTaskDependencyEntity>, sqlx::Error> {
        sqlx::query_as!(
            CalenderTaskDependencyEntity,
            r#"
            SELECT id, blocking_task_id, blocked_task_id
            FROM memory.calender_task_dependency
            WHERE blocked_task_id = $1
            ORDER BY id ASC
            "#,
            input.blocked_task_id,
        )
        .fetch_all(self.db())
        .await
    }
}
