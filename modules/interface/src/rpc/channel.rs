//! gRPC adapter for the channel surface.
//!
//! Thin: each handler maps proto ⇄ domain and calls a [`ChannelService`]
//! processor. `Subscribe` turns the per-adapter broadcast receiver into a
//! server stream, filtered to the platform the adapter asked for.

use std::pin::Pin;

use kanau::processor::Processor;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use crate::events::InboundUserMessage;
use crate::proto::channel as pb;
use crate::proto::channel::channel_server::Channel;
use crate::services::channel::{ChannelService, DeliverInboundRequest};

/// gRPC front door for platform adapters.
#[derive(Clone)]
pub struct ChannelGrpcService {
    service: ChannelService,
}

impl ChannelGrpcService {
    /// Wire the gRPC service over a [`ChannelService`].
    pub fn new(service: ChannelService) -> Self {
        Self { service }
    }
}

#[tonic::async_trait]
impl Channel for ChannelGrpcService {
    async fn deliver_inbound(
        &self,
        request: Request<pb::InboundMessage>,
    ) -> Result<Response<pb::DeliverInboundResponse>, Status> {
        let pb::InboundMessage {
            platform,
            chat_id,
            user_id,
            text,
        } = request.into_inner();
        self.service
            .process(DeliverInboundRequest {
                message: InboundUserMessage {
                    platform,
                    chat_id,
                    user_id,
                    text,
                },
            })
            .await?;
        Ok(Response::new(pb::DeliverInboundResponse {}))
    }

    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<pb::OutboundMessage, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let platform = request.into_inner().platform;
        let receiver = self.service.subscribe();
        let stream = BroadcastStream::new(receiver).filter_map(move |item| match item {
            Ok(message) if message.platform == platform => Some(Ok(pb::OutboundMessage {
                platform: message.platform,
                chat_id: message.chat_id,
                text: message.text,
            })),
            // Other platforms, and lagged-receiver errors, are skipped.
            _ => None,
        });
        Ok(Response::new(Box::pin(stream)))
    }
}
