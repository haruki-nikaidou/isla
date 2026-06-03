//! Saga-style request/response envelopes carried over AMQP.
//!
//! A saga step is a unit of work that one service asks another to perform and
//! then expects a reply for. Unlike a fire-and-forget event, the caller needs
//! the answer routed back to a destination it chooses at call time — RabbitMQ
//! exchanges and routing keys are static per message type, so the reply address
//! must travel *with* the message instead.
//!
//! Both [`SagaCallRequest`] and [`SagaCallResponse`] therefore carry:
//!
//! - `body`          — the application payload,
//! - `context_id`    — correlation id tying a reply back to its originating call,
//! - `routing_key` / `exchange_name` — where the *reply* should be published.
//!
//! The two types are structurally identical and convert into each other for
//! free. The distinction is directional: a handler decodes a [`SagaCallRequest`]
//! ([`MessageDe`]), processes it through [`SagaCallWrap`], and publishes the
//! resulting [`SagaCallResponse`] ([`MessageSer`]) to the embedded reply address.

use amqprs::BasicProperties;
use amqprs::channel::{BasicPublishArguments, Channel, ConfirmSelectArguments};
use kanau::message::{MessageDe, MessageSer};
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use wakuwaku::Error;
use wakuwaku::amqp::AmqpPool;
use wakuwaku::pool::Pooled;

/// An inbound saga step: work requested by a peer service.
///
/// Carries the reply address (`exchange_name` / `routing_key`) so the produced
/// [`SagaCallResponse`] can be routed back to the originator.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SagaCallRequest<T> {
    /// Application payload to process.
    pub body: T,
    /// Correlation id linking the eventual reply to this call.
    pub context_id: Uuid,
    /// Routing key the reply must be published with.
    pub routing_key: String,
    /// Exchange the reply must be published to.
    pub exchange_name: String,
}

/// An outbound saga step: the reply produced for a [`SagaCallRequest`].
///
/// The `context_id` and reply address are copied verbatim from the request so
/// the originator can correlate and receive the answer.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SagaCallResponse<T> {
    /// Result payload.
    pub body: T,
    /// Correlation id copied from the originating [`SagaCallRequest`].
    pub context_id: Uuid,
    /// Routing key the reply is published with.
    pub routing_key: String,
    /// Exchange the reply is published to.
    pub exchange_name: String,
}

impl<T> MessageDe for SagaCallRequest<T>
where
    T: for<'de> Deserialize<'de>,
{
    type DeError = serde_json::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeError> {
        serde_json::from_slice(bytes)
    }
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

impl<T> MessageDe for SagaCallResponse<T>
where
    T: for<'de> Deserialize<'de>,
{
    type DeError = serde_json::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeError> {
        serde_json::from_slice(bytes)
    }
}

impl<T> MessageSer for SagaCallResponse<T>
where
    T: Serialize,
{
    type SerError = serde_json::Error;

    fn to_bytes(self) -> Result<Box<[u8]>, Self::SerError> {
        serde_json::to_vec(&self).map(Vec::into_boxed_slice)
    }
}

impl<T> From<SagaCallRequest<T>> for SagaCallResponse<T> {
    fn from(request: SagaCallRequest<T>) -> Self {
        Self {
            body: request.body,
            context_id: request.context_id,
            routing_key: request.routing_key,
            exchange_name: request.exchange_name,
        }
    }
}

impl<T> From<SagaCallResponse<T>> for SagaCallRequest<T> {
    fn from(response: SagaCallResponse<T>) -> Self {
        Self {
            body: response.body,
            context_id: response.context_id,
            routing_key: response.routing_key,
            exchange_name: response.exchange_name,
        }
    }
}

impl<T> SagaCallResponse<T>
where
    T: Serialize,
{
    /// Publish this reply to its embedded address using caller-supplied AMQP
    /// `properties` (for example, a signature header).
    ///
    /// Publishes with the mandatory flag set on a publisher-confirm channel, so
    /// an unroutable reply surfaces as an error rather than being silently
    /// dropped.
    pub async fn send_with_prop(
        self,
        pool: &AmqpPool,
        properties: BasicProperties,
    ) -> Result<(), Error> {
        let routing_key = self.routing_key.clone();
        let exchange_name = self.exchange_name.clone();
        let bytes = self
            .to_bytes()
            .map_err(kanau::message::SerializeError::from)?;

        let channel: Result<Pooled<Channel, _>, Error> = pool.get().await.into();
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
                bytes.into_vec(),
                BasicPublishArguments::new(&exchange_name, &routing_key)
                    .mandatory(true)
                    .finish(),
            )
            .await?;
        Ok(())
    }

    /// Publish this reply with default AMQP properties.
    ///
    /// Convenience wrapper over [`send_with_prop`](Self::send_with_prop) for
    /// callers that do not need to attach custom headers.
    pub async fn send(self, pool: &AmqpPool) -> Result<(), Error> {
        self.send_with_prop(pool, BasicProperties::default()).await
    }
}

/// Adapts a plain [`Processor<T>`] into one that speaks the saga protocol.
///
/// Wrapping a processor `P: Processor<T>` yields a
/// `Processor<SagaCallRequest<T>, Output = SagaCallResponse<P::Output>>`: it
/// runs the inner processor on the request `body` and threads the correlation
/// id and reply address straight through to the response, leaving transport
/// concerns (signing, publishing) to the caller.
pub struct SagaCallWrap<P> {
    inner: P,
}

impl<P> SagaCallWrap<P> {
    /// Wrap `inner` so it can process [`SagaCallRequest`] envelopes.
    pub fn new(inner: P) -> Self {
        SagaCallWrap { inner }
    }
}

impl<T, P> Processor<SagaCallRequest<T>> for SagaCallWrap<P>
where
    P: Processor<T> + Sync,
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
        let body = self.inner.process(body).await?;
        Ok(SagaCallResponse {
            body,
            context_id,
            routing_key,
            exchange_name,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    /// Drive an immediately-resolving future to completion without a runtime.
    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
        }
    }

    /// Trivial processor that doubles its input, used to test the wrapper.
    struct Doubler;

    impl Processor<u32> for Doubler {
        type Output = u32;
        type Error = std::convert::Infallible;

        async fn process(&self, input: u32) -> Result<u32, Self::Error> {
            Ok(input * 2)
        }
    }

    fn sample_request(body: u32) -> SagaCallRequest<u32> {
        SagaCallRequest {
            body,
            context_id: Uuid::from_u128(42),
            routing_key: "reply.key".to_owned(),
            exchange_name: "reply.exchange".to_owned(),
        }
    }

    #[test]
    fn wrap_processes_body_and_threads_reply_address() {
        let wrapped = SagaCallWrap::new(Doubler);
        let response = block_on(wrapped.process(sample_request(21))).expect("infallible");

        // Inner processor ran on the body.
        assert_eq!(response.body, 42);
        // Correlation id and reply address pass through untouched.
        assert_eq!(response.context_id, Uuid::from_u128(42));
        assert_eq!(response.routing_key, "reply.key");
        assert_eq!(response.exchange_name, "reply.exchange");
    }

    #[test]
    fn request_response_conversions_are_lossless() {
        let request = sample_request(5);
        let response: SagaCallResponse<u32> = request.clone().into();
        let roundtrip: SagaCallRequest<u32> = response.into();

        assert_eq!(roundtrip.body, request.body);
        assert_eq!(roundtrip.context_id, request.context_id);
        assert_eq!(roundtrip.routing_key, request.routing_key);
        assert_eq!(roundtrip.exchange_name, request.exchange_name);
    }

    #[test]
    fn response_serde_roundtrips() {
        let response: SagaCallResponse<u32> = sample_request(9).into();
        let bytes = response.clone().to_bytes().expect("serialize");
        let decoded = SagaCallResponse::<u32>::from_bytes(&bytes).expect("deserialize");

        assert_eq!(decoded.body, response.body);
        assert_eq!(decoded.context_id, response.context_id);
        assert_eq!(decoded.routing_key, response.routing_key);
        assert_eq!(decoded.exchange_name, response.exchange_name);
    }
}
