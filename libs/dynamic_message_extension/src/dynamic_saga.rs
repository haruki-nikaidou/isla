use amqprs::channel::Channel;
use kanau::message::{MessageDe, MessageSer};
use serde::Serialize;
use uuid::Uuid;
use wakuwaku::amqp::AmqpPool;

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SagaCallRequest<T> {
    pub body: T,
    pub context_id: Uuid,
    pub routing_key: String,
    pub exchange_name: String,
}

impl<T> MessageDe for SagaCallRequest<T>
where
    T: for<'de> serde::Deserialize<'de>,
{
    type DeError = serde_json::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeError> {
        serde_json::from_slice(bytes)
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct SagaCallResponse<T> {
    pub body: T,
    pub context_id: Uuid,
    pub routing_key: String,
    pub exchange_name: String,
}

impl<T> MessageSer for SagaCallRequest<T>
where
    T: Serialize,
{
    type SerError = serde_json::Error;

    fn to_bytes(self) -> Result<Box<[u8]>, Self::SerError> {
        serde_json::to_vec(&self).map(Vec::into_boxed_slice)
    }
}

impl<T> SagaCallResponse<T>
where
    T: Serialize,
    Self: MessageSer,
{
    pub async fn send(
        self,
        pool: &AmqpPool,
    ) -> Result<(), wakuwaku::Error> {
        let routing_key = self.routing_key.clone();
        let exchange_name = self.exchange_name.clone();
        let bytes = self.to_bytes().map_err(Into::into)?;
        let channel: Result<wakuwaku::pool::Pooled<Channel, _>, wakuwaku::Error> =
            pool.get().await.into();
        let channel = channel?;
        let channel = channel
            .get_ref()
            .ok_or(wakuwaku::Error::Io(anyhow::anyhow!(
                "Channel is unexpectedly closed"
            )))?;
        channel
            .confirm_select(amqprs::channel::ConfirmSelectArguments::new(false))
            .await?;
        channel
            .basic_publish(
                amqprs::BasicProperties::default(),
                bytes.into_vec(),
                amqprs::channel::BasicPublishArguments::new(&exchange_name, &routing_key)
                    .mandatory(true)
                    .finish(),
            )
            .await?;
        Ok(())
    }
}

pub struct SagaCallWrap<P> {
    inner: P,
}

impl<P> SagaCallWrap<P> {
    pub fn new(inner: P) -> Self {
        SagaCallWrap { inner }
    }
}

impl<T, P> kanau::processor::Processor<SagaCallRequest<T>> for SagaCallWrap<P>
where
    P: kanau::processor::Processor<T> + Sync,
    T: Send,
{
    type Output = SagaCallResponse<P::Output>;
    type Error = P::Error;

    async fn process(&self, input: SagaCallRequest<T>) -> Result<Self::Output, Self::Error> {
        let SagaCallRequest {
            body,
            context_id,
            routing_key,
            exchange_name,
        } = input;
        let inner_result = self.inner.process(body).await?;
        Ok(SagaCallResponse {
            body: inner_result,
            context_id,
            routing_key,
            exchange_name,
        })
    }
}
