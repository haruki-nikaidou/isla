//! Channel service: publishes user messages onto the cluster bus and hands out
//! subscriptions to the outbound stream.

use kanau::processor::Processor;
use tokio::sync::broadcast;
use tracing::instrument;
use wakuwaku::Error;
use wakuwaku::amqp::{AmqpMessageSend, AmqpPool};

use crate::events::{InboundUserMessage, OutboundUserMessage};

/// Business logic behind the [`Channel`](crate::proto::channel) gRPC surface.
///
/// Inbound messages are published onto the cluster bus; outbound messages are
/// fanned out to the platform adapters currently subscribed through
/// [`subscribe`](Self::subscribe). The outbound stream is fed by
/// [`OutboundDeliveryHook`](crate::hooks::OutboundDeliveryHook), which consumes
/// [`OutboundUserMessage`] events off the bus.
#[derive(Clone)]
pub struct ChannelService {
    mq: AmqpPool,
    outbound: broadcast::Sender<OutboundUserMessage>,
}

impl ChannelService {
    /// Build a channel service over an AMQP pool and an outbound broadcaster.
    pub fn new(mq: AmqpPool, outbound: broadcast::Sender<OutboundUserMessage>) -> Self {
        Self { mq, outbound }
    }

    /// Subscribe to the outbound message stream (one receiver per adapter).
    pub fn subscribe(&self) -> broadcast::Receiver<OutboundUserMessage> {
        self.outbound.subscribe()
    }
}

/// Deliver one inbound user message into the cluster.
#[derive(Debug, Clone)]
pub struct DeliverInboundRequest {
    pub message: InboundUserMessage,
}

impl Processor<DeliverInboundRequest> for ChannelService {
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "DeliverInbound", err, fields(platform = %input.message.platform))]
    async fn process(&self, input: DeliverInboundRequest) -> Result<(), Error> {
        input.message.send(&self.mq).await
    }
}

/// Publish one outbound user message onto the cluster bus.
#[derive(Debug, Clone)]
pub struct SendOutboundRequest {
    pub message: OutboundUserMessage,
}

impl Processor<SendOutboundRequest> for ChannelService {
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "SendOutbound", err, fields(platform = %input.message.platform))]
    async fn process(&self, input: SendOutboundRequest) -> Result<(), Error> {
        input.message.send(&self.mq).await
    }
}
