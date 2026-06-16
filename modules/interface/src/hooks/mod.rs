//! Inbound AMQP hooks for the interface module.
//!
//! [`OutboundDeliveryHook`] consumes [`OutboundUserMessage`] events off the bus
//! and fans them into the process-local broadcast channel that the gRPC
//! `Subscribe` stream reads from, so a message the cluster published reaches the
//! adapter currently serving that platform.

use kanau::processor::Processor;
use tokio::sync::broadcast;
use tracing::instrument;
use wakuwaku::Error;
use wakuwaku::amqp::AmqpMessageProcessor;

use crate::events::OutboundUserMessage;

/// Forwards outbound messages from the bus into the local outbound broadcaster.
#[derive(Clone)]
pub struct OutboundDeliveryHook {
    outbound: broadcast::Sender<OutboundUserMessage>,
}

impl OutboundDeliveryHook {
    /// Build the hook over the broadcaster shared with [`ChannelService`].
    ///
    /// [`ChannelService`]: crate::services::channel::ChannelService
    pub fn new(outbound: broadcast::Sender<OutboundUserMessage>) -> Self {
        Self { outbound }
    }
}

impl AmqpMessageProcessor<OutboundUserMessage> for OutboundDeliveryHook {
    const QUEUE: &'static str = "interface_outbound_delivery";
}

impl Processor<OutboundUserMessage> for OutboundDeliveryHook {
    type Output = ();
    type Error = Error;

    #[instrument(skip_all, name = "OutboundUserMessage", err, fields(platform = %event.platform))]
    async fn process(&self, event: OutboundUserMessage) -> Result<(), Error> {
        // An empty subscriber set is not an error: the message is simply not
        // delivered (no adapter is currently serving that platform).
        let _ = self.outbound.send(event);
        Ok(())
    }
}
