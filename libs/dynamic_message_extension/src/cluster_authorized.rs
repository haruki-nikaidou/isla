//! Ed25519-authenticated AMQP messaging between cluster nodes.
//!
//! Messages crossing the internal RabbitMQ bus are signed by the sender and
//! verified by the consumer so that only nodes holding the cluster private key
//! can inject work. The scheme is:
//!
//! 1. The sender hashes the message body with SHA-256 and signs the digest with
//!    its [`Ed25519KeyPair`].
//! 2. The signature is attached as the [`CLUSTER_SIGNATURE_HEADER`] AMQP header.
//! 3. The consumer recomputes the digest, verifies the signature against the
//!    cluster public key, and only then hands the payload to the inner
//!    processor.
//!
//! [`ClusterMessageSender`] performs step 1–2, [`ClusterMessageVerifier`]
//! performs the verification, and [`ClusterMessageHook`] wires verification in
//! front of an existing [`AmqpMessageProcessor`] as an [`AsyncConsumer`].

use crate::dynamic_saga::SagaCallResponse;
use amqprs::channel::{
    BasicAckArguments, BasicNackArguments, BasicPublishArguments, Channel, ConfirmSelectArguments,
};
use amqprs::consumer::AsyncConsumer;
use amqprs::{BasicProperties, ByteArray, Deliver, FieldName, FieldTable, FieldValue};
use kanau::message::{MessageDe, MessageSer};
use ring::signature::{self, Ed25519KeyPair, UnparsedPublicKey};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use wakuwaku::Error;
use wakuwaku::amqp::{
    AmqpExchangeType, AmqpMessageProcessor, AmqpMessageSend, AmqpPool, AmqpRouting, ack, nack,
};
use wakuwaku::pool::Pooled;

/// AMQP header carrying the base64-free raw Ed25519 signature bytes.
pub const CLUSTER_SIGNATURE_HEADER: &str = "X-Cluster-Signature";

/// Number of ack/nack retry attempts on transient AMQP failures.
const SETTLE_RETRIES: u32 = 5;

/// Sign the SHA-256 digest of `body` with `key`, returning the raw Ed25519
/// signature bytes.
///
/// Hashing first keeps the signed input fixed-size regardless of message size,
/// and is the exact inverse of [`ClusterMessageVerifier::verify`].
fn sign_digest(key: &Ed25519KeyPair, body: &[u8]) -> Vec<u8> {
    let digest = Sha256::digest(body);
    key.sign(digest.as_slice()).as_ref().to_vec()
}

/// Transparent routing wrapper that reuses the inner type's serialization and
/// routing, marking a message as cluster-authenticated at the type level.
pub struct ClusterMessage<T>(pub T);

impl<T: MessageSer> MessageSer for ClusterMessage<T> {
    type SerError = T::SerError;

    fn to_bytes(self) -> Result<Box<[u8]>, Self::SerError> {
        self.0.to_bytes()
    }
}

impl<T: MessageSer + AmqpRouting> AmqpRouting for ClusterMessage<T> {
    const EXCHANGE: &'static str = T::EXCHANGE;
    const EXCHANGE_TYPE: AmqpExchangeType = T::EXCHANGE_TYPE;
    const ROUTING_KEY: &'static str = T::ROUTING_KEY;
}

/// Publishes signed messages onto the cluster bus.
///
/// Holds the node's private key and an AMQP channel pool. Every message it
/// sends carries a [`CLUSTER_SIGNATURE_HEADER`] that a [`ClusterMessageVerifier`]
/// on the receiving side can check.
pub struct ClusterMessageSender {
    pool: AmqpPool,
    key: Ed25519KeyPair,
}

impl ClusterMessageSender {
    /// Create a sender from an AMQP channel `pool` and the node's signing `key`.
    pub fn new(pool: AmqpPool, key: Ed25519KeyPair) -> Self {
        Self { pool, key }
    }

    /// Sign the SHA-256 digest of `bytes`, returning the raw signature.
    pub fn sign(&self, bytes: &[u8]) -> Vec<u8> {
        sign_digest(&self.key, bytes)
    }

    /// Build the `(name, value)` AMQP header pair carrying the signature of
    /// `bytes`.
    pub fn get_sign_header(&self, bytes: &[u8]) -> Result<(FieldName, FieldValue), anyhow::Error> {
        let name = FieldName::try_from(CLUSTER_SIGNATURE_HEADER)?;
        let value = FieldValue::x(ByteArray::try_from(self.sign(bytes))?);
        Ok((name, value))
    }

    /// AMQP properties carrying the signature header for a message `body`.
    fn signed_properties(&self, body: &[u8]) -> Result<BasicProperties, Error> {
        let (name, value) = self.get_sign_header(body).map_err(Error::BusinessPanic)?;
        let mut headers = FieldTable::new();
        headers.insert(name, value);
        Ok(BasicProperties::default().with_headers(headers).to_owned())
    }

    /// Acquire a channel, enable publisher confirms, and publish `bytes` to
    /// `exchange`/`routing_key` with the mandatory flag set.
    async fn publish(
        &self,
        properties: BasicProperties,
        bytes: Vec<u8>,
        exchange: &str,
        routing_key: &str,
    ) -> Result<(), Error> {
        let channel: Result<Pooled<Channel, _>, Error> = self.pool.get().await.into();
        let channel = channel?;
        let channel = channel
            .get_ref()
            .ok_or_else(|| Error::Io(anyhow::anyhow!("Channel is unexpectedly closed")))?;

        channel
            .confirm_select(ConfirmSelectArguments::new(false))
            .await?;
        channel
            .basic_publish(
                properties,
                bytes,
                BasicPublishArguments::new(exchange, routing_key)
                    .mandatory(true)
                    .finish(),
            )
            .await?;
        Ok(())
    }

    /// Sign and publish a routed cluster message.
    pub async fn send<T: AmqpMessageSend>(&self, message: T) -> Result<(), Error> {
        let bytes = message.to_bytes().map_err(Into::into)?;
        let properties = self.signed_properties(&bytes)?;
        self.publish(properties, bytes.into_vec(), T::EXCHANGE, T::ROUTING_KEY)
            .await
    }

    /// Sign and publish a saga reply to the address embedded in `saga_message`.
    pub async fn send_saga<T>(&self, saga_message: SagaCallResponse<T>) -> Result<(), Error>
    where
        T: Serialize + Clone,
    {
        let bytes = saga_message
            .clone()
            .to_bytes()
            .map_err(kanau::message::SerializeError::from)?;
        let properties = self.signed_properties(&bytes)?;
        saga_message.send_with_prop(&self.pool, properties).await
    }
}

/// Verifies signatures produced by a [`ClusterMessageSender`].
#[derive(Clone, Copy)]
pub struct ClusterMessageVerifier {
    /// Cluster public key used to verify message signatures.
    pub public_key: UnparsedPublicKey<[u8; 32]>,
}

impl ClusterMessageVerifier {
    /// Create a verifier from the raw 32-byte Ed25519 cluster public key.
    pub fn new(public_key: [u8; 32]) -> Self {
        Self {
            public_key: UnparsedPublicKey::new(&signature::ED25519, public_key),
        }
    }

    /// Return `true` iff `signature` is a valid signature, under the cluster
    /// public key, of the SHA-256 digest of `body`.
    pub fn verify(&self, body: &[u8], signature: &[u8]) -> bool {
        let digest = Sha256::digest(body);
        self.public_key.verify(digest.as_slice(), signature).is_ok()
    }
}

/// Authenticating wrapper around an [`AmqpMessageProcessor`].
///
/// As an [`AsyncConsumer`] it verifies the
/// [`CLUSTER_SIGNATURE_HEADER`] of each delivery before decoding the payload and
/// forwarding it to `Inner`. Unsigned or badly signed deliveries are rejected
/// with [`Error::PermissionsDenied`].
#[derive(Clone)]
pub struct ClusterMessageHook<Message, Inner>
where
    Message: AmqpMessageSend + MessageDe,
    Inner: AmqpMessageProcessor<Message>,
{
    inner: Arc<Inner>,
    verifier: ClusterMessageVerifier,
    _marker: PhantomData<fn(Message)>,
}

impl<Message, Inner> ClusterMessageHook<Message, Inner>
where
    Message: AmqpMessageSend + MessageDe,
    Inner: AmqpMessageProcessor<Message>,
{
    /// Wrap `inner` so it only receives deliveries verified by `verifier`.
    pub fn new(verifier: ClusterMessageVerifier, inner: Inner) -> Self {
        Self {
            inner: Arc::new(inner),
            verifier,
            _marker: PhantomData,
        }
    }

    /// Verify, decode, and dispatch a single delivery.
    ///
    /// Returns [`Error::PermissionsDenied`] when the signature header is absent
    /// or invalid; otherwise propagates the inner processor's result.
    pub async fn on_message(&self, prop: BasicProperties, content: Vec<u8>) -> Result<(), Error> {
        let header_name = FieldName::try_from(CLUSTER_SIGNATURE_HEADER)
            .map_err(|e| Error::BusinessPanic(e.into()))?;
        let Some(FieldValue::x(signature)) =
            prop.headers().and_then(|headers| headers.get(&header_name))
        else {
            return Err(Error::PermissionsDenied);
        };

        let signature: Vec<u8> = signature.to_owned().into();
        if !self.verifier.verify(&content, &signature) {
            return Err(Error::PermissionsDenied);
        }

        let message = Message::from_bytes(&content).map_err(Into::into)?;
        self.inner.process(message).await
    }
}

/// How a delivery should be settled after processing.
enum Disposition {
    /// Remove the message from the queue (success, or a permanent failure that
    /// a retry cannot fix).
    Ack,
    /// Return the message to the queue for another attempt (transient failure).
    Requeue,
    /// Leave the message unsettled — used when the channel itself is the
    /// problem and neither ack nor nack can be trusted.
    Drop,
}

/// Classify a processing error into a [`Disposition`] and log it.
///
/// Permanent failures (bad input, missing data, serialization, business
/// panics) are acked so they do not loop forever; transient infrastructure
/// failures (database, cache, IO) are requeued; an AMQP error means the channel
/// is unusable, so the delivery is left untouched.
fn classify(error: Error) -> Disposition {
    match error {
        Error::DatabaseError(e) => {
            tracing::error!("Database: {e}");
            Disposition::Requeue
        }
        Error::RedisError(e) => {
            tracing::error!("Redis: {e}");
            Disposition::Requeue
        }
        Error::Io(e) => {
            tracing::error!("IO error: {e}");
            Disposition::Requeue
        }
        Error::SerializeError(_) | Error::DeserializeError(_) => Disposition::Ack,
        Error::InvalidInput | Error::NotFound | Error::PermissionsDenied => {
            tracing::error!("Invalid input in event");
            Disposition::Ack
        }
        Error::BusinessPanic(e) => {
            tracing::error!("Business panic: {e}");
            Disposition::Ack
        }
        Error::AmqpError(e) => {
            tracing::error!("RabbitMQ: {e}");
            Disposition::Drop
        }
    }
}

impl<M, I> AsyncConsumer for ClusterMessageHook<M, I>
where
    M: AmqpMessageSend + MessageDe + Send + Sync,
    I: AmqpMessageProcessor<M> + Send + Sync,
    M::DeError: Send,
{
    fn consume<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        channel: &'life1 Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        Box::pin(async move {
            let tag = deliver.delivery_tag();
            let disposition = match self.on_message(basic_properties, content).await {
                Ok(()) => Disposition::Ack,
                Err(error) => classify(error),
            };
            match disposition {
                Disposition::Ack => {
                    ack(channel, BasicAckArguments::new(tag, false), SETTLE_RETRIES).await;
                }
                Disposition::Requeue => {
                    nack(
                        channel,
                        BasicNackArguments::new(tag, false, true),
                        SETTLE_RETRIES,
                    )
                    .await;
                }
                Disposition::Drop => {}
            }
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::KeyPair;

    fn keypair() -> Ed25519KeyPair {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate key");
        Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse key")
    }

    fn verifier_for(key: &Ed25519KeyPair) -> ClusterMessageVerifier {
        let public_key: [u8; 32] = key.public_key().as_ref().try_into().expect("32-byte key");
        ClusterMessageVerifier::new(public_key)
    }

    #[test]
    fn sign_then_verify_roundtrips() {
        let key = keypair();
        let verifier = verifier_for(&key);
        let body = b"cluster payload";

        let signature = sign_digest(&key, body);
        assert!(verifier.verify(body, &signature));
    }

    #[test]
    fn verify_rejects_tampered_body() {
        let key = keypair();
        let verifier = verifier_for(&key);

        let signature = sign_digest(&key, b"original body");
        assert!(!verifier.verify(b"tampered body", &signature));
    }

    #[test]
    fn verify_rejects_signature_from_other_key() {
        let body = b"cluster payload";
        let attacker = keypair();
        let verifier = verifier_for(&keypair());

        let forged = sign_digest(&attacker, body);
        assert!(!verifier.verify(body, &forged));
    }

    #[test]
    fn verify_rejects_garbage_signature() {
        let key = keypair();
        let verifier = verifier_for(&key);
        assert!(!verifier.verify(b"cluster payload", &[0u8; 64]));
    }

    #[test]
    fn cluster_message_forwards_serialization() {
        // ClusterMessage<T> must serialize identically to its inner value.
        let inner = SagaCallResponse {
            body: 7u32,
            context_id: uuid::Uuid::nil(),
            routing_key: "rk".to_owned(),
            exchange_name: "ex".to_owned(),
        };
        let wrapped = ClusterMessage(inner.clone());
        assert_eq!(wrapped.to_bytes().unwrap(), inner.to_bytes().unwrap());
    }
}
