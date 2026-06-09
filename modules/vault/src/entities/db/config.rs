use crate::module_config::ModuleConfig;
use kanau::processor::Processor;
use std::marker::PhantomData;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
pub struct ModuleConfigEntity {
    pub id: i32,
    pub scope: String,
    pub config_name: String,
    pub content: serde_json::Value,
}

#[derive(Debug, Clone, Copy)]
pub struct FindConfig<T: ModuleConfig> {
    _phantom: PhantomData<T>,
}

impl<T: ModuleConfig> FindConfig<T> {
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T: ModuleConfig + Send> Processor<FindConfig<T>> for DatabaseProcessor {
    type Output = T;
    type Error = sqlx::Error;
    async fn process(&self, _request: FindConfig<T>) -> Result<Self::Output, Self::Error> {
        let scope_string = T::SCOPE.to_string();
        let config_name = T::CONFIG_NAME;
        let maybe_record = sqlx::query_as!(
            ModuleConfigEntity,
            r#"
            SELECT id, scope, config_name, content
            FROM vault.config
            WHERE scope = $1 AND config_name = $2
            "#,
            scope_string,
            config_name
        )
        .fetch_optional(self.db())
        .await?;
        let result = maybe_record
            .map(|record| serde_json::from_value::<T>(record.content))
            .transpose()
            .map_err(|e| sqlx::Error::Decode(e.into()))?
            .unwrap_or_default();
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct UpdateConfig<T: ModuleConfig> {
    pub new_value: T,
}

impl<T: ModuleConfig + Send> Processor<UpdateConfig<T>> for DatabaseProcessor {
    type Output = Option<ModuleConfigEntity>;
    type Error = sqlx::Error;
    async fn process(&self, request: UpdateConfig<T>) -> Result<Self::Output, Self::Error> {
        let scope_string = T::SCOPE.to_string();
        let config_name = T::CONFIG_NAME.to_string();
        let value =
            serde_json::to_value(request.new_value).map_err(|e| sqlx::Error::Encode(e.into()))?;
        sqlx::query_as!(
            ModuleConfigEntity,
            r#"
            UPDATE vault.config c
            SET content = $1
            WHERE c.scope = $2 AND c.config_name = $3
            RETURNING c.content as "content!", c.id as "id!", c."scope" as "scope!", c.config_name as "config_name!"
            "#,
            value,
            scope_string,
            config_name
        ).fetch_optional(self.db()).await
    }
}
