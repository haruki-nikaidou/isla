//! # `telegram_bot` — Telegram user interface
//!
//! First-party Telegram adapter for Isla.
//!
//! This binary is a thin shim between the Telegram Bot API and the
//! `interface` module: it forwards inbound user messages into the cluster
//! over gRPC and renders outbound messages from the cluster back to
//! Telegram chats.
//!
//! It is *not* a plugin — it provides no tools and no skills. "Send a
//! message" tooling lives inside the `interface` module itself, so this
//! adapter only has to translate between transport formats. Other chat
//! platforms (Discord, Slack, …) are added as siblings under
//! `user_interface/` following the same pattern.
//!
//! ## Running
//!
//! Configuration is read from the environment:
//!
//! - `TELEGRAM_BOT_TOKEN` — bot token issued by `@BotFather` (required).
//! - `INTERFACE_GRPC_URL` — gRPC endpoint of the `interface` module,
//!   e.g. `http://127.0.0.1:50051` (required).
//! - `TELEGRAM_PLATFORM` — platform identifier reported to the cluster
//!   (optional, defaults to `telegram`).

#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod proto {
    pub mod channel {
        #![allow(clippy::all, clippy::pedantic)]
        tonic::include_proto!("isla.interface.channel");
    }
}

use std::time::Duration;

use anyhow::Context as _;
use proto::channel::channel_client::ChannelClient;
use proto::channel::{InboundMessage, SubscribeRequest};
use serde::Deserialize;
use tracing::{error, info, warn};

/// Telegram `getUpdates` long-poll timeout, in seconds.
const POLL_TIMEOUT_SECS: u64 = 30;

/// Relevant fields of a single Telegram update.
#[derive(Debug, Clone, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<TgMessage>,
}

/// Relevant fields of a Telegram message.
#[derive(Debug, Clone, Deserialize)]
struct TgMessage {
    #[allow(dead_code)]
    message_id: i64,
    chat: TgChat,
    from: Option<TgUser>,
    text: Option<String>,
}

/// Telegram chat descriptor.
#[derive(Debug, Clone, Deserialize)]
struct TgChat {
    id: i64,
}

/// Telegram user descriptor.
#[derive(Debug, Clone, Deserialize)]
struct TgUser {
    id: i64,
}

/// Envelope returned by the Telegram `getUpdates` endpoint.
#[derive(Debug, Clone, Deserialize)]
struct GetUpdates {
    ok: bool,
    result: Vec<Update>,
}

/// Build the JSON request body for the Telegram `sendMessage` endpoint.
///
/// `chat_id` is emitted as a JSON number when it parses as an `i64`
/// (Telegram's canonical numeric chat id) and as a string otherwise
/// (channel usernames such as `@my_channel`).
fn send_message_body(chat_id: &str, text: &str) -> serde_json::Value {
    let chat_id = match chat_id.parse::<i64>() {
        Ok(id) => serde_json::Value::from(id),
        Err(_) => serde_json::Value::from(chat_id),
    };
    serde_json::json!({ "chat_id": chat_id, "text": text })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let token = std::env::var("TELEGRAM_BOT_TOKEN").context("TELEGRAM_BOT_TOKEN must be set")?;
    let interface_url =
        std::env::var("INTERFACE_GRPC_URL").context("INTERFACE_GRPC_URL must be set")?;
    let platform = std::env::var("TELEGRAM_PLATFORM").unwrap_or_else(|_| "telegram".to_owned());

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(POLL_TIMEOUT_SECS + 15))
        .build()
        .context("failed to build reqwest client")?;

    let client = ChannelClient::connect(interface_url.clone())
        .await
        .with_context(|| format!("failed to connect to interface at {interface_url}"))?;

    info!(%platform, "telegram_bot connected to interface");

    // Outbound: stream cluster messages and relay them to Telegram.
    tokio::spawn(run_outbound(
        client.clone(),
        http.clone(),
        token.clone(),
        platform.clone(),
    ));

    // Inbound: long-poll Telegram and forward into the cluster.
    run_inbound(client, http, token, platform).await
}

/// Subscribe to the cluster's outbound stream and relay each message to the
/// Telegram `sendMessage` API. Errors are logged; the loop keeps running.
async fn run_outbound(
    mut client: ChannelClient<tonic::transport::Channel>,
    http: reqwest::Client,
    token: String,
    platform: String,
) {
    let request = SubscribeRequest {
        platform: platform.clone(),
    };
    let mut stream = match client.subscribe(request).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            error!(%status, "failed to subscribe to interface outbound stream");
            return;
        }
    };

    let send_url = format!("https://api.telegram.org/bot{token}/sendMessage");
    loop {
        match stream.message().await {
            Ok(Some(msg)) => {
                let body = send_message_body(&msg.chat_id, &msg.text);
                if let Err(err) = http.post(&send_url).json(&body).send().await {
                    error!(%err, chat_id = %msg.chat_id, "failed to send Telegram message");
                }
            }
            Ok(None) => {
                info!("interface outbound stream closed");
                return;
            }
            Err(status) => {
                error!(%status, "interface outbound stream error");
                return;
            }
        }
    }
}

/// Long-poll Telegram `getUpdates` and forward text messages into the
/// interface module via `DeliverInbound`. Transient errors are logged and the
/// loop sleeps briefly before retrying so a single failure never crashes it.
async fn run_inbound(
    mut client: ChannelClient<tonic::transport::Channel>,
    http: reqwest::Client,
    token: String,
    platform: String,
) -> anyhow::Result<()> {
    let updates_url = format!("https://api.telegram.org/bot{token}/getUpdates");
    let mut offset: i64 = 0;

    loop {
        let response = http
            .get(&updates_url)
            .query(&[
                ("timeout", POLL_TIMEOUT_SECS.to_string()),
                ("offset", offset.to_string()),
            ])
            .send()
            .await
            .and_then(reqwest::Response::error_for_status);

        let body = match response {
            Ok(resp) => resp.json::<GetUpdates>().await,
            Err(err) => {
                warn!(%err, "getUpdates request failed; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let updates = match body {
            Ok(payload) if payload.ok => payload.result,
            Ok(_) => {
                warn!("getUpdates returned ok=false; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(err) => {
                warn!(%err, "failed to decode getUpdates response; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        for update in updates {
            offset = update.update_id + 1;
            let Some(message) = update.message else {
                continue;
            };
            let Some(text) = message.text else {
                continue;
            };

            let inbound = InboundMessage {
                platform: platform.clone(),
                chat_id: message.chat.id.to_string(),
                user_id: message
                    .from
                    .map(|user| user.id.to_string())
                    .unwrap_or_default(),
                text,
            };

            if let Err(status) = client.deliver_inbound(inbound).await {
                error!(%status, "failed to deliver inbound message to interface");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::send_message_body;

    #[test]
    fn numeric_chat_id_is_serialized_as_number() {
        let body = send_message_body("12345", "hello");
        assert_eq!(body["chat_id"], serde_json::json!(12345));
        assert!(body["chat_id"].is_i64());
        assert_eq!(body["text"], serde_json::json!("hello"));
    }

    #[test]
    fn negative_chat_id_is_serialized_as_number() {
        let body = send_message_body("-1009876543210", "group");
        assert_eq!(body["chat_id"], serde_json::json!(-1009876543210_i64));
        assert!(body["chat_id"].is_i64());
    }

    #[test]
    fn username_chat_id_is_serialized_as_string() {
        let body = send_message_body("@my_channel", "hi");
        assert_eq!(body["chat_id"], serde_json::json!("@my_channel"));
        assert!(body["chat_id"].is_string());
    }
}
