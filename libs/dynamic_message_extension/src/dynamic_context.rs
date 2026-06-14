//! Out-of-band message bodies: store the payload in Redis, send only a pointer.
//!
//! The internal AMQP bus is for *small* control messages; a bulky body (an
//! assembled summary, a message excerpt) must not ride on it. [`ContextRef<T>`]
//! solves this the same way [`SagaCallRequest`](crate::dynamic_saga::SagaCallRequest)
//! uses a `context_id`: the real payload `T` is written to Redis under a fresh
//! [`Uuid`], and only that id travels on the bus.
//!
//! ```text
//! producer:  body: T  --write_with_ttl-->  Redis[isla:msgctx:{uuid}] = JSON(body)
//!                      --send-->            AMQP: ContextRef<T>{ context_id }
//! consumer:  ContextRef<T>  --read--> Redis -> body: T -> inner Processor<T>
//! ```
//!
//! [`ContextRef<T>`] mirrors the routing of `T`, so the exchange and routing key
//! a consumer binds to are unchanged — only the wire body shrinks.
//!
//! The pointer rides the *internal* cluster bus, so it is signed on the way out
//! and verified on the way in like any other cluster message:
//! [`ContextRef::publish`] sends through a [`ClusterMessageSender`], and
//! [`verified_context_consumer`] fronts the pointer-resolving [`ContextRefWrap`]
//! with a [`ClusterMessageHook`] so a delivery is authenticated before its body
//! is ever fetched.

use std::marker::PhantomData;
use std::time::Duration;

use kanau::message::{MessageDe, MessageSer};
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use wakuwaku::Error;
use wakuwaku::amqp::{AmqpExchangeType, AmqpMessageProcessor, AmqpMessageSend, AmqpRouting};
use wakuwaku::pool::{Pool, Pooled, PoolingResult};
use wakuwaku::redis::{KeyValue, KeyValueRead, KeyValueWrite, RedisConnection, RedisKey};

use crate::cluster_authorized::{ClusterMessageHook, ClusterMessageSender, ClusterMessageVerifier};

/// Pool of Redis connections backing the context store.
pub type RedisPool = Pool<RedisConnection>;

/// How long a stored body lives before Redis evicts it.
///
/// Chosen comfortably longer than any consumer's requeue-retry window so an
/// in-flight redelivery never finds the body already expired.
const DEFAULT_CONTEXT_TTL: Duration = Duration::from_secs(3600);

/// Build the Redis key for a stored context body.
///
/// A human-readable string key (rather than the raw uuid bytes) keeps stored
/// bodies greppable with `redis-cli --scan --pattern 'isla:msgctx:*'`.
fn context_key(id: Uuid) -> RedisKey {
    format!("isla:msgctx:{id}").into()
}

/// Acquire a pooled Redis connection, mapping pool failures onto [`Error`].
async fn acquire(redis: &RedisPool) -> Result<Pooled<RedisConnection>, Error> {
    match redis.get().await {
        PoolingResult::Ok(conn) => Ok(conn),
        PoolingResult::FactoryErr(e) => Err(Error::Io(e)),
        PoolingResult::SemanticsError => Err(Error::BusinessPanic(anyhow::anyhow!(
            "Redis pool semaphore error"
        ))),
    }
}

/// A key/value pair in the Redis context store: a [`Uuid`]-derived key bound to
/// a message body `T`.
///
/// Only ever used to name the [`KeyValueRead`]/[`KeyValueWrite`] operations; the
/// `value()` accessor exists to satisfy [`KeyValue`] and is unused on the hot
/// paths, which move the body by value.
struct StoredContext<T> {
    key: RedisKey,
    value: T,
}

impl<T: Clone + Send + Sync> KeyValue for StoredContext<T> {
    type Key = RedisKey;
    type Value = T;

    fn key(&self) -> Self::Key {
        self.key.clone()
    }
    fn value(&self) -> Self::Value {
        self.value.clone()
    }
    fn into_value(self) -> Self::Value {
        self.value
    }
    fn new(key: Self::Key, value: Self::Value) -> Self {
        Self { key, value }
    }
}

impl<T: Clone + MessageDe + Send + Sync> KeyValueRead for StoredContext<T> {}
impl<T: Clone + MessageSer + Send + Sync> KeyValueWrite for StoredContext<T> {}

/// JSON wire shape of a [`ContextRef`]: just the pointer, independent of `T`.
#[derive(Serialize, Deserialize)]
struct ContextRefWire {
    context_id: Uuid,
}

/// A bus-sized pointer to a body `T` parked in the Redis context store.
///
/// Carries only a [`Uuid`]; routing is inherited from `T` so the message lands
/// on the same exchange/routing key the body would have used directly.
pub struct ContextRef<T> {
    /// Redis pointer to the stored body.
    pub context_id: Uuid,
    _marker: PhantomData<fn() -> T>,
}

impl<T> ContextRef<T> {
    /// Wrap an existing pointer (e.g. one decoded off the bus).
    #[must_use]
    pub fn new(context_id: Uuid) -> Self {
        Self {
            context_id,
            _marker: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for ContextRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextRef")
            .field("context_id", &self.context_id)
            .finish()
    }
}

impl<T> MessageSer for ContextRef<T> {
    type SerError = serde_json::Error;

    fn to_bytes(self) -> Result<Box<[u8]>, Self::SerError> {
        serde_json::to_vec(&ContextRefWire {
            context_id: self.context_id,
        })
        .map(Vec::into_boxed_slice)
    }
}

impl<T> MessageDe for ContextRef<T> {
    type DeError = serde_json::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeError> {
        let wire: ContextRefWire = serde_json::from_slice(bytes)?;
        Ok(Self::new(wire.context_id))
    }
}

impl<T: AmqpRouting> AmqpRouting for ContextRef<T> {
    const EXCHANGE: &'static str = T::EXCHANGE;
    const EXCHANGE_TYPE: AmqpExchangeType = T::EXCHANGE_TYPE;
    const ROUTING_KEY: &'static str = T::ROUTING_KEY;
}

impl<T: AmqpRouting + Send> AmqpMessageSend for ContextRef<T> {}

impl<T> ContextRef<T>
where
    T: AmqpRouting + Clone + MessageSer + MessageDe + Send + Sync,
{
    /// Park `body` in the Redis context store and publish a signed pointer to
    /// it on the cluster bus.
    ///
    /// Writes the body (with [`DEFAULT_CONTEXT_TTL`]) *before* sending, so the
    /// pointer is never observable before the body it resolves. The pointer is
    /// sent through `sender`, so it carries a cluster signature that
    /// [`verified_context_consumer`] checks on the way in. Returns the generated
    /// `context_id` for tracing/correlation.
    pub async fn publish(
        redis: &RedisPool,
        sender: &ClusterMessageSender,
        body: T,
    ) -> Result<Uuid, Error> {
        let context_id = Uuid::new_v4();
        {
            let mut pooled = acquire(redis).await?;
            let conn = pooled.get_mut().ok_or_else(|| {
                Error::Io(anyhow::anyhow!("Redis connection unexpectedly closed"))
            })?;
            StoredContext::<T>::write_kv_with_ttl(
                conn,
                context_key(context_id),
                body,
                DEFAULT_CONTEXT_TTL,
            )
            .await?;
        }
        sender.send(ContextRef::<T>::new(context_id)).await?;
        Ok(context_id)
    }
}

/// Adapts an [`AmqpMessageProcessor<T>`] into one that consumes
/// [`ContextRef<T>`]: it resolves the pointer against Redis and forwards the
/// recovered body to `inner`.
///
/// Binds the same queue as the wrapped processor. A pointer that no longer
/// resolves (the body expired, or was never written) yields [`Error::NotFound`],
/// which the cluster consumer settles as an ack — a missing one-shot body is a
/// permanent condition a redelivery cannot fix.
///
/// The body is deliberately *not* deleted after a successful read: a transient
/// downstream failure requeues the delivery, and the retry must still find it.
pub struct ContextRefWrap<P> {
    inner: P,
    redis: RedisPool,
}

impl<P> ContextRefWrap<P> {
    /// Wrap `inner`, resolving pointers against `redis`.
    pub fn new(inner: P, redis: RedisPool) -> Self {
        Self { inner, redis }
    }
}

impl<T, P> AmqpMessageProcessor<ContextRef<T>> for ContextRefWrap<P>
where
    T: AmqpMessageSend + Clone + MessageDe + Sync,
    P: AmqpMessageProcessor<T> + Send + Sync,
{
    const QUEUE: &'static str = P::QUEUE;
}

impl<T, P> Processor<ContextRef<T>> for ContextRefWrap<P>
where
    T: Clone + MessageDe + Send + Sync,
    P: Processor<T, Output = (), Error = Error> + Send + Sync,
{
    type Output = ();
    type Error = Error;

    async fn process(&self, input: ContextRef<T>) -> Result<(), Error> {
        let body = {
            let mut pooled = acquire(&self.redis).await?;
            let conn = pooled.get_mut().ok_or_else(|| {
                Error::Io(anyhow::anyhow!("Redis connection unexpectedly closed"))
            })?;
            StoredContext::<T>::read(conn, context_key(input.context_id)).await?
        };
        match body {
            Some(body) => self.inner.process(body).await,
            None => Err(Error::NotFound),
        }
    }
}

/// Build the full inbound stack for a cluster-internal `ContextRef<T>` consumer.
///
/// Fronts the pointer-resolving [`ContextRefWrap`] with a [`ClusterMessageHook`]:
/// a delivery's cluster signature is verified *first*, and only an authenticated
/// pointer is then resolved against `redis` and handed to `inner`. This is the
/// counterpart to [`ContextRef::publish`], which signs on the way out.
pub fn verified_context_consumer<T, P>(
    inner: P,
    redis: RedisPool,
    verifier: ClusterMessageVerifier,
) -> ClusterMessageHook<ContextRef<T>, ContextRefWrap<P>>
where
    T: AmqpMessageSend + Clone + MessageDe + Sync,
    P: AmqpMessageProcessor<T> + Send + Sync,
{
    ClusterMessageHook::new(verifier, ContextRefWrap::new(inner, redis))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// A stand-in body type with its own routing, used to check that
    /// `ContextRef` mirrors routing and round-trips its pointer.
    struct Body;

    impl AmqpRouting for Body {
        const EXCHANGE: &'static str = "memory.events";
        const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
        const ROUTING_KEY: &'static str = "entry.upserted";
    }

    #[test]
    fn context_ref_serializes_to_just_the_pointer() {
        let id = Uuid::from_u128(0xABCD);
        let bytes = ContextRef::<Body>::new(id).to_bytes().expect("serialize");

        // Only the pointer is on the wire — body-sized fields never appear.
        let json = String::from_utf8(bytes.to_vec()).expect("utf8");
        assert_eq!(json, format!("{{\"context_id\":\"{id}\"}}"));
    }

    #[test]
    fn context_ref_roundtrips_pointer() {
        let id = Uuid::from_u128(7);
        let bytes = ContextRef::<Body>::new(id).to_bytes().expect("serialize");
        let decoded = ContextRef::<Body>::from_bytes(&bytes).expect("deserialize");
        assert_eq!(decoded.context_id, id);
    }

    #[test]
    fn context_ref_mirrors_inner_routing() {
        assert_eq!(ContextRef::<Body>::EXCHANGE, Body::EXCHANGE);
        assert_eq!(ContextRef::<Body>::ROUTING_KEY, Body::ROUTING_KEY);
    }

    #[test]
    fn context_key_is_namespaced_and_stable() {
        let id = Uuid::from_u128(0x1234);
        let RedisKey(bytes) = context_key(id);
        assert_eq!(
            String::from_utf8(bytes.to_vec()).expect("utf8"),
            format!("isla:msgctx:{id}")
        );
    }
}
