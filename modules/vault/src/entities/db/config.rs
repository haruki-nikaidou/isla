use crate::module_config::ModuleConfig;
use kanau::processor::Processor;
use std::marker::PhantomData;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Debug, Clone)]
pub struct ModuleConfigEntity {
    pub id: i32,
    pub scope: String,
    pub name: String,
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
    async fn process(&self, request: FindConfig<T>) -> Result<Self::Output, Self::Error> {
        todo!()
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
        todo!()
    }
}
