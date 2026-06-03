//! AMQP event definitions for memory-related notifications.
//!
//! Events published here allow other modules and plugins to react to
//! memory changes (e.g., new conversation created, contact relationship updated).

pub mod call;
pub mod publish;
