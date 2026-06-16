//! AMQP events carrying user messages across the cluster.
//!
//! The `interface` module is the boundary between platform adapters and the
//! rest of the cluster. Two events cross the bus:
//!
//! - [`InboundUserMessage`] — published when an adapter delivers a message a
//!   user sent. Whatever drives the agent (the personality/agent loop) consumes
//!   it.
//! - [`OutboundUserMessage`] — published when the cluster wants to say something
//!   back. The interface module consumes it (see
//!   [`OutboundDeliveryHook`](crate::hooks::OutboundDeliveryHook)) and routes it
//!   to the adapter subscribed for that platform.

use kanau::{JsonMessageDe, JsonMessageSer};
use serde::{Deserialize, Serialize};
use wakuwaku::amqp::{AmqpExchangeType, AmqpMessageSend, AmqpRouting};

/// Topic exchange every interface event is published to.
pub const INTERFACE_EXCHANGE: &str = "interface.events";

/// A message a user sent on some platform, normalized for the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct InboundUserMessage {
    /// Channel adapter the message arrived on, e.g. `telegram`.
    pub platform: String,
    /// Platform-specific chat/conversation address to reply to.
    pub chat_id: String,
    /// Platform-specific id of the user who sent the message.
    pub user_id: String,
    /// The message text.
    pub text: String,
}

impl AmqpRouting for InboundUserMessage {
    const EXCHANGE: &'static str = INTERFACE_EXCHANGE;
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
    const ROUTING_KEY: &'static str = "user.message.inbound";
}
impl AmqpMessageSend for InboundUserMessage {}

/// A message the cluster wants delivered back to a user on some platform.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonMessageDe, JsonMessageSer)]
pub struct OutboundUserMessage {
    /// Channel adapter that should deliver the message, e.g. `telegram`.
    pub platform: String,
    /// Platform-specific chat/conversation address to deliver to.
    pub chat_id: String,
    /// The message text.
    pub text: String,
}

impl AmqpRouting for OutboundUserMessage {
    const EXCHANGE: &'static str = INTERFACE_EXCHANGE;
    const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
    const ROUTING_KEY: &'static str = "user.message.outbound";
}
impl AmqpMessageSend for OutboundUserMessage {}
