//! Shared AMQP delivery settlement for authenticating consumer hooks.
//!
//! Every authenticating consumer in this crate ([`cluster_authorized`] and
//! [`plugin_message`]) runs an inner processor and then has to tell RabbitMQ
//! what to do with the delivery. The decision is identical regardless of how the
//! message was authenticated, so it lives here:
//!
//! - success, or a permanent failure a retry cannot fix → **ack** (drop it),
//! - a transient infrastructure failure → **nack + requeue** (try again),
//! - a broken channel → leave the delivery untouched (neither can be trusted).
//!
//! [`cluster_authorized`]: crate::cluster_authorized
//! [`plugin_message`]: crate::plugin_message

use amqprs::channel::{BasicAckArguments, BasicNackArguments, Channel};
use wakuwaku::Error;
use wakuwaku::amqp::{ack, nack};

/// Number of ack/nack retry attempts on transient AMQP failures.
const SETTLE_RETRIES: u32 = 5;

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

/// Settle a delivery on `channel` according to the outcome of processing it.
///
/// `result` is whatever the consumer's `on_message` returned. Success acks;
/// failures are classified into ack / requeue / drop by [`classify`].
pub(crate) async fn settle(channel: &Channel, delivery_tag: u64, result: Result<(), Error>) {
    let disposition = match result {
        Ok(()) => Disposition::Ack,
        Err(error) => classify(error),
    };
    match disposition {
        Disposition::Ack => {
            ack(
                channel,
                BasicAckArguments::new(delivery_tag, false),
                SETTLE_RETRIES,
            )
            .await;
        }
        Disposition::Requeue => {
            nack(
                channel,
                BasicNackArguments::new(delivery_tag, false, true),
                SETTLE_RETRIES,
            )
            .await;
        }
        Disposition::Drop => {}
    }
}
