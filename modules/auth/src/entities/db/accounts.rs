use kanau::processor::Processor;
use time::PrimitiveDateTime;
use tracing::instrument;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
/// Account for owner and trusted members.
///
/// - Schema: `auth`
/// - Table Name: `account`
pub struct AccountEntity {
    /// Primary key
    pub id: Uuid,

    /// Unique username
    pub username: String,

    /// Argon2 hashed password
    pub password: String,

    /// The time user registered. Readonly after creation.
    pub registered_at: PrimitiveDateTime,
}

#[derive(Debug, Clone, sqlx::Type)]
#[sqlx(type_name = "auth.account_status")]
pub enum AccountRole {
    Owner,
    Member,
}

/// Find an [`AccountEntity`] by its primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindAccountById {
    pub id: Uuid,
}

impl Processor<FindAccountById> for DatabaseProcessor {
    type Output = Option<AccountEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindAccountById", err, fields(id = %input.id))]
    async fn process(&self, input: FindAccountById) -> Result<Option<AccountEntity>, sqlx::Error> {
        sqlx::query_as!(
            AccountEntity,
            r#"
            SELECT id, username, password, registered_at
            FROM auth.account
            WHERE id = $1
            LIMIT 1
            "#,
            input.id,
        )
        .fetch_optional(self.db())
        .await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindUserByUsername {
    pub username: String,
}

impl Processor<FindUserByUsername> for DatabaseProcessor {
    type Output = Option<AccountEntity>;
    type Error = sqlx::Error;

    #[instrument(skip_all, name = "SQL:FindUserByUsername", err)]
    async fn process(
        &self,
        input: FindUserByUsername,
    ) -> Result<Option<AccountEntity>, sqlx::Error> {
        sqlx::query_as!(
            AccountEntity,
            r#"
            SELECT id, username, password, registered_at
            FROM auth.account
            WHERE username = $1
            LIMIT 1
            "#,
            input.username,
        )
        .fetch_optional(self.db())
        .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListAllUsers {
    pub limit: i64,
    pub offset: i64,
}

impl Processor<ListAllUsers> for DatabaseProcessor {
    type Output = Vec<AccountEntity>;
    type Error = sqlx::Error;
    #[instrument(skip_all, name = "SQL:ListAllUsers", err)]
    async fn process(&self, input: ListAllUsers) -> Result<Vec<AccountEntity>, sqlx::Error> {
        sqlx::query_as!(
            AccountEntity,
            r#"
            SELECT id, username, password, registered_at
            FROM auth.account
            ORDER BY registered_at DESC
            LIMIT $1
            OFFSET $2
            "#,
            input.limit,
            input.offset,
        )
        .fetch_all(self.db())
        .await
    }
}
