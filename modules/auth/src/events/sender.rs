use kanau::message::MessageSer;
use wakuwaku::amqp::{AmqpPool, AmqpRouting};

pub struct ClusterMessage<T>(pub T);

impl<T: MessageSer> ClusterMessage<T> {}

impl<T: MessageSer> MessageSer for ClusterMessage<T> {
    type SerError = T::SerError;

    fn to_bytes(self) -> Result<Box<[u8]>, Self::SerError> {
        self.0.to_bytes()
    }
}

impl<T: MessageSer + AmqpRouting> AmqpRouting for ClusterMessage<T> {
    const EXCHANGE: &'static str = T::EXCHANGE;
    const EXCHANGE_TYPE: wakuwaku::amqp::AmqpExchangeType = T::EXCHANGE_TYPE;
    const ROUTING_KEY: &'static str = T::ROUTING_KEY;
}

pub struct ClusterMessageSender {
    pool: AmqpPool,
}

impl ClusterMessageSender {
    pub fn new(pool: AmqpPool) -> Self {
        Self { pool }
    }
}