//! # `dynamic_message_extension`
//!
//! AMQP messaging extensions shared across Isla services.
//!
//! These building blocks layer on top of `wakuwaku`'s AMQP primitives:
//!
//! - [`cluster_authorized`] — Ed25519-signed messages so only nodes holding the
//!   cluster key can publish onto the internal bus, plus a consumer hook that
//!   verifies signatures before dispatching to an inner processor.
//! - [`dynamic_saga`] — request/response envelopes that carry their own reply
//!   address, enabling saga-style call chains over static AMQP topologies.
//! - [`dynamic_context`] — out-of-band bodies: park a bulky payload in Redis and
//!   put only a pointer on the bus, keeping the message small.

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

pub mod cluster_authorized;
pub mod dynamic_context;
pub mod dynamic_saga;
pub mod plugin_message;
mod settle;
