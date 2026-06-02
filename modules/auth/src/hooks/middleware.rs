use amqprs::channel::{BasicAckArguments, BasicNackArguments};
use amqprs::{BasicProperties, FieldName, FieldValue};
use kanau::message::MessageDe;
use ring::signature;
use sha2::{Digest, Sha256};
use std::pin::Pin;
use std::sync::Arc;
use wakuwaku::amqp::{AmqpMessageProcessor, AmqpMessageSend, ack, nack};

#[derive(Clone, Copy)]
pub struct ClusterMessageVerifier {
    pub public_key: signature::UnparsedPublicKey<[u8; 32]>,
}

impl ClusterMessageVerifier {
    pub fn new(public_key: [u8; 32]) -> Self {
        ClusterMessageVerifier {
            public_key: signature::UnparsedPublicKey::new(&signature::ED25519, public_key),
        }
    }
    pub fn verify(&self, body: &[u8], signature: &[u8]) -> bool {
        let mut sha = Sha256::new();
        sha.update(body);
        let body_hash = sha.finalize();
        self.public_key
            .verify(body_hash.as_slice(), signature)
            .is_ok()
    }
}

pub const CLUSTER_SIGNATURE_HEADER: &'static str = "X-Cluster-Signature";

#[derive(Clone)]
pub struct ClusterMessageHook<Message, Inner>
where
    Message: AmqpMessageSend + MessageDe,
    Inner: AmqpMessageProcessor<Message>,
{
    inner: Arc<Inner>,
    verifier: ClusterMessageVerifier,
    _marker: std::marker::PhantomData<fn(Message)>,
}

impl<Message, Inner> ClusterMessageHook<Message, Inner>
where
    Message: AmqpMessageSend + MessageDe,
    Inner: AmqpMessageProcessor<Message>,
{
    pub fn new(verifier: ClusterMessageVerifier, inner: Inner) -> Self {
        ClusterMessageHook {
            inner: Arc::new(inner),
            verifier,
            _marker: std::marker::PhantomData,
        }
    }
    pub async fn on_message(
        &self,
        prop: BasicProperties,
        content: Vec<u8>,
    ) -> Result<(), wakuwaku::Error> {
        let Some(FieldValue::x(signature)) = prop
            .headers()
            .and_then(|h| h.get(&CLUSTER_SIGNATURE_HEADER.try_into().unwrap()))
        else {
            return Err(wakuwaku::Error::PermissionsDenied);
        };
        let signature: Vec<u8> = signature.to_owned().into();
        let is_valid = self.verifier.verify(&content, &signature);
        if !is_valid {
            Err(wakuwaku::Error::PermissionsDenied)
        } else {
            let decoded_message = Message::from_bytes(&content).map_err(|e| e.into())?;
            self.inner.process(decoded_message).await
        }
    }
}

impl<M, I> amqprs::consumer::AsyncConsumer for ClusterMessageHook<M, I>
where
    M: AmqpMessageSend + MessageDe + Send + Sync,
    I: AmqpMessageProcessor<M> + Send + Sync,
    M::DeError: Send,
{
    fn consume<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        channel: &'life1 amqprs::channel::Channel,
        deliver: amqprs::Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        Box::pin(async move {
            match self.on_message(basic_properties, content).await {
                Ok(_) => {
                    ack(
                        channel,
                        BasicAckArguments::new(deliver.delivery_tag(), false),
                        5,
                    )
                    .await;
                }
                Err(wakuwaku::Error::DatabaseError(e)) => {
                    nack(
                        channel,
                        BasicNackArguments::new(deliver.delivery_tag(), false, true),
                        5,
                    )
                    .await;
                    tracing::error!("Database: {}", e);
                }
                Err(wakuwaku::Error::SerializeError(_))
                | Err(wakuwaku::Error::DeserializeError(_)) => {
                    ack(
                        channel,
                        BasicAckArguments::new(deliver.delivery_tag(), false),
                        5,
                    )
                    .await;
                }
                Err(wakuwaku::Error::RedisError(e)) => {
                    nack(
                        channel,
                        BasicNackArguments::new(deliver.delivery_tag(), false, true),
                        5,
                    )
                    .await;
                    tracing::error!("Redis: {}", e);
                }
                Err(wakuwaku::Error::InvalidInput)
                | Err(wakuwaku::Error::NotFound)
                | Err(wakuwaku::Error::PermissionsDenied) => {
                    ack(
                        channel,
                        BasicAckArguments::new(deliver.delivery_tag(), false),
                        5,
                    )
                    .await;
                    tracing::error!("Invalid input in event");
                }
                Err(wakuwaku::Error::AmqpError(e)) => {
                    tracing::error!("RabbitMQ: {}", e);
                }
                Err(wakuwaku::Error::BusinessPanic(e)) => {
                    ack(
                        channel,
                        BasicAckArguments::new(deliver.delivery_tag(), false),
                        5,
                    )
                    .await;
                    tracing::error!("Business panic: {}", e);
                }
                Err(wakuwaku::Error::Io(e)) => {
                    nack(
                        channel,
                        BasicNackArguments::new(deliver.delivery_tag(), false, true),
                        5,
                    )
                    .await;
                    tracing::error!("IO error: {}", e);
                }
            }
        })
    }
}
