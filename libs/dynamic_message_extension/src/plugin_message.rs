//! JWT-authenticated AMQP messaging from plugins into the cluster.
//!
//! Plugins live outside the trust boundary and may be written in any language,
//! so they cannot hold the cluster's Ed25519 key the way internal nodes do (see
//! [`cluster_authorized`](crate::cluster_authorized)). Instead each registered
//! plugin is issued its own symmetric secret and proves its identity per message
//! with a short-lived JWT:
//!
//! 1. The plugin signs a [`PluginClaims`] token (HS256) with its secret, putting
//!    its registered id in both the `kid` header and the `sub` claim.
//! 2. The token travels as the [`PLUGIN_TOKEN_HEADER`] AMQP header on every
//!    message the plugin publishes.
//! 3. The cluster reads the `kid` to learn *which* plugin claims to be sending,
//!    fetches that plugin's secret through an injected
//!    [`Processor<FindJwtSecretRequest>`], verifies the signature and expiry,
//!    and only then decodes the payload and hands it to the inner processor.
//!
//! [`PluginMessageSender`] performs steps 1–2 (used by first-party Rust plugins
//! and tests), and [`PluginMessageHook`] performs step 3 as an
//! [`AsyncConsumer`] in front of an existing [`AmqpMessageProcessor`]. The
//! secret lookup is *injected* rather than owned so the auth layer stays
//! decoupled from where plugin secrets are stored (`plugin_registrar`).

use crate::settle::settle;
use amqprs::channel::{BasicPublishArguments, Channel, ConfirmSelectArguments};
use amqprs::consumer::AsyncConsumer;
use amqprs::{BasicProperties, Deliver, FieldName, FieldTable, FieldValue, LongStr};
use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use kanau::message::MessageDe;
use kanau::processor::Processor;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use wakuwaku::Error;
use wakuwaku::amqp::{AmqpMessageProcessor, AmqpMessageSend, AmqpPool};
use wakuwaku::pool::Pooled;

/// AMQP header carrying the per-message plugin JWT.
pub const PLUGIN_TOKEN_HEADER: &str = "x-plugin-token";

/// Signing algorithm for plugin tokens. Symmetric (HS256) so a plugin and the
/// cluster share one per-plugin secret.
const TOKEN_ALGORITHM: Algorithm = Algorithm::HS256;

/// Request to fetch the JWT signing secret of a registered plugin.
///
/// This is the seam the plugin-auth layer is generic over: an implementor (in
/// practice `plugin_registrar`) provides
/// `Processor<FindJwtSecretRequest, Output = Option<String>, Error = wakuwaku::Error>`,
/// returning the plugin's secret or `None` when no such plugin is registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FindJwtSecretRequest {
    /// Registered id of the plugin whose secret is wanted.
    pub plugin_id: Uuid,
}

/// Claims carried by a plugin token.
///
/// `sub` names the plugin and is cross-checked against the `kid` header that
/// selected the verifying secret, so a token signed for one plugin cannot be
/// replayed as another even if the secrets were to collide.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PluginClaims {
    /// Subject: the registered id of the plugin that minted the token.
    pub sub: Uuid,
    /// Issued-at time, seconds since the Unix epoch.
    pub iat: u64,
    /// Expiry time, seconds since the Unix epoch.
    pub exp: u64,
}

/// Seconds since the Unix epoch, or a [`Error::BusinessPanic`] if the clock is
/// before the epoch.
fn now_unix() -> Result<u64, Error> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .map_err(|e| Error::BusinessPanic(anyhow::anyhow!(e)))
}

/// Mint a plugin token for `plugin_id`, signed with `secret` (HS256), issued at
/// `issued_at` and valid for `ttl_secs` (both Unix seconds).
///
/// The `plugin_id` is written to both the `kid` header (so the verifier can pick
/// the right secret before trusting anything) and the `sub` claim.
pub fn sign_plugin_token(
    plugin_id: Uuid,
    secret: &str,
    issued_at: u64,
    ttl_secs: u64,
) -> Result<String, Error> {
    let mut header = Header::new(TOKEN_ALGORITHM);
    header.kid = Some(plugin_id.to_string());
    let claims = PluginClaims {
        sub: plugin_id,
        iat: issued_at,
        exp: issued_at.saturating_add(ttl_secs),
    };
    encode(
        &header,
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
    .map_err(|e| Error::BusinessPanic(anyhow::anyhow!(e)))
}

/// Read the plugin id from a token's `kid` header **without** verifying it.
///
/// The id only decides which secret to verify against; the token is untrusted
/// until [`verify_plugin_token`] succeeds. A missing/unparseable `kid` is
/// [`Error::PermissionsDenied`].
pub fn plugin_id_from_token(token: &str) -> Result<Uuid, Error> {
    let header = decode_header(token).map_err(|_| Error::PermissionsDenied)?;
    let kid = header.kid.ok_or(Error::PermissionsDenied)?;
    Uuid::parse_str(&kid).map_err(|_| Error::PermissionsDenied)
}

/// Verify `token` against `secret` and return its claims.
///
/// Fails with [`Error::PermissionsDenied`] when the signature is invalid, the
/// token has expired, or the `sub` claim does not match `plugin_id` (the id the
/// `kid` header used to select the secret).
pub fn verify_plugin_token(
    token: &str,
    plugin_id: Uuid,
    secret: &str,
) -> Result<PluginClaims, Error> {
    let mut validation = Validation::new(TOKEN_ALGORITHM);
    // We do not use the `aud` claim; leaving aud validation on would reject
    // every token that omits it.
    validation.validate_aud = false;
    let data = decode::<PluginClaims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )
    .map_err(|_| Error::PermissionsDenied)?;
    if data.claims.sub != plugin_id {
        return Err(Error::PermissionsDenied);
    }
    Ok(data.claims)
}

/// Build the AMQP properties carrying `token` in the [`PLUGIN_TOKEN_HEADER`].
fn token_properties(token: String) -> Result<BasicProperties, Error> {
    let name =
        FieldName::try_from(PLUGIN_TOKEN_HEADER).map_err(|e| Error::BusinessPanic(e.into()))?;
    let value =
        FieldValue::S(LongStr::try_from(token).map_err(|e| Error::BusinessPanic(e.into()))?);
    let mut headers = FieldTable::new();
    headers.insert(name, value);
    Ok(BasicProperties::default().with_headers(headers).to_owned())
}

/// Extract the raw plugin token from a delivery's headers.
///
/// Returns [`Error::PermissionsDenied`] when the header is absent or not a
/// string — i.e. an unauthenticated message.
fn read_token(prop: &BasicProperties) -> Result<String, Error> {
    let name =
        FieldName::try_from(PLUGIN_TOKEN_HEADER).map_err(|e| Error::BusinessPanic(e.into()))?;
    let Some(FieldValue::S(token)) = prop.headers().and_then(|headers| headers.get(&name)) else {
        return Err(Error::PermissionsDenied);
    };
    Ok(token.as_ref().clone())
}

/// Publishes JWT-authenticated messages onto the plugin bus.
///
/// Holds the plugin's id, its per-plugin signing `secret`, and an AMQP channel
/// pool. Every message it sends carries a freshly minted [`PLUGIN_TOKEN_HEADER`]
/// that a [`PluginMessageHook`] on the cluster side verifies.
pub struct PluginMessageSender {
    pool: AmqpPool,
    plugin_id: Uuid,
    secret: String,
    token_ttl_secs: u64,
}

impl PluginMessageSender {
    /// Create a sender for `plugin_id` signing with `secret`, minting tokens
    /// valid for `token_ttl_secs`.
    pub fn new(pool: AmqpPool, plugin_id: Uuid, secret: String, token_ttl_secs: u64) -> Self {
        Self {
            pool,
            plugin_id,
            secret,
            token_ttl_secs,
        }
    }

    /// Mint a fresh token for this plugin, valid from now for `token_ttl_secs`.
    pub fn mint_token(&self) -> Result<String, Error> {
        let now = now_unix()?;
        sign_plugin_token(self.plugin_id, &self.secret, now, self.token_ttl_secs)
    }

    /// Acquire a channel, enable publisher confirms, and publish `bytes` to
    /// `exchange`/`routing_key` with the mandatory flag set.
    async fn publish(
        &self,
        properties: BasicProperties,
        bytes: Vec<u8>,
        exchange: &str,
        routing_key: &str,
    ) -> Result<(), Error> {
        let channel: Result<Pooled<Channel, _>, Error> = self.pool.get().await.into();
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
                bytes,
                BasicPublishArguments::new(exchange, routing_key)
                    .mandatory(true)
                    .finish(),
            )
            .await?;
        Ok(())
    }

    /// Mint a token and publish a routed plugin message.
    pub async fn send<T: AmqpMessageSend>(&self, message: T) -> Result<(), Error> {
        let bytes = message.to_bytes().map_err(Into::into)?;
        let properties = token_properties(self.mint_token()?)?;
        self.publish(properties, bytes.into_vec(), T::EXCHANGE, T::ROUTING_KEY)
            .await
    }
}

/// Authenticating wrapper around an [`AmqpMessageProcessor`].
///
/// As an [`AsyncConsumer`] it reads the [`PLUGIN_TOKEN_HEADER`] of each delivery,
/// resolves the claimed plugin's secret through the injected `Secrets`
/// processor, verifies the token, and only then decodes the payload and forwards
/// it to `Inner`. Unauthenticated, unknown-plugin, or badly signed deliveries
/// are rejected with [`Error::PermissionsDenied`].
#[derive(Clone)]
pub struct PluginMessageHook<Message, Inner, Secrets>
where
    Message: AmqpMessageSend + MessageDe,
    Inner: AmqpMessageProcessor<Message>,
    Secrets: Processor<FindJwtSecretRequest, Output = Option<String>, Error = Error>,
{
    inner: Arc<Inner>,
    secrets: Secrets,
    _marker: PhantomData<fn(Message)>,
}

impl<Message, Inner, Secrets> PluginMessageHook<Message, Inner, Secrets>
where
    Message: AmqpMessageSend + MessageDe,
    Inner: AmqpMessageProcessor<Message>,
    Secrets: Processor<FindJwtSecretRequest, Output = Option<String>, Error = Error>,
{
    /// Wrap `inner` so it only receives deliveries whose plugin token verifies
    /// against the secret returned by `secrets`.
    pub fn new(secrets: Secrets, inner: Inner) -> Self {
        Self {
            inner: Arc::new(inner),
            secrets,
            _marker: PhantomData,
        }
    }

    /// Authenticate, decode, and dispatch a single delivery.
    ///
    /// Returns [`Error::PermissionsDenied`] when the token is missing, names an
    /// unregistered plugin, or fails verification; otherwise propagates the
    /// inner processor's result.
    pub async fn on_message(&self, prop: BasicProperties, content: Vec<u8>) -> Result<(), Error> {
        let token = read_token(&prop)?;
        let plugin_id = plugin_id_from_token(&token)?;
        let secret = self
            .secrets
            .process(FindJwtSecretRequest { plugin_id })
            .await?
            .ok_or(Error::PermissionsDenied)?;
        verify_plugin_token(&token, plugin_id, &secret)?;

        let message = Message::from_bytes(&content).map_err(Into::into)?;
        self.inner.process(message).await
    }
}

impl<M, I, S> AsyncConsumer for PluginMessageHook<M, I, S>
where
    M: AmqpMessageSend + MessageDe + Send + Sync,
    I: AmqpMessageProcessor<M> + Send + Sync,
    S: Processor<FindJwtSecretRequest, Output = Option<String>, Error = Error> + Send + Sync,
    M::DeError: Send,
{
    fn consume<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        channel: &'life1 Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        Box::pin(async move {
            let result = self.on_message(basic_properties, content).await;
            settle(channel, deliver.delivery_tag(), result).await;
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    const SECRET: &str = "super-secret-plugin-key";

    fn now() -> u64 {
        now_unix().expect("clock after epoch")
    }

    #[test]
    fn sign_then_verify_roundtrips() {
        let plugin = Uuid::new_v4();
        let token = sign_plugin_token(plugin, SECRET, now(), 3600).expect("sign");

        let claims = verify_plugin_token(&token, plugin, SECRET).expect("verify");
        assert_eq!(claims.sub, plugin);
    }

    #[test]
    fn kid_header_carries_plugin_id() {
        let plugin = Uuid::new_v4();
        let token = sign_plugin_token(plugin, SECRET, now(), 3600).expect("sign");
        assert_eq!(plugin_id_from_token(&token).expect("kid"), plugin);
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let plugin = Uuid::new_v4();
        let token = sign_plugin_token(plugin, SECRET, now(), 3600).expect("sign");
        assert!(verify_plugin_token(&token, plugin, "different-secret").is_err());
    }

    #[test]
    fn verify_rejects_expired_token() {
        let plugin = Uuid::new_v4();
        // Issued well in the past with a 1s lifetime: expired beyond the 60s leeway.
        let token = sign_plugin_token(plugin, SECRET, now() - 1000, 1).expect("sign");
        assert!(verify_plugin_token(&token, plugin, SECRET).is_err());
    }

    #[test]
    fn verify_rejects_subject_mismatch() {
        let signer = Uuid::new_v4();
        let other = Uuid::new_v4();
        let token = sign_plugin_token(signer, SECRET, now(), 3600).expect("sign");
        // Same secret, but the token's `sub` is `signer`, not `other`.
        assert!(verify_plugin_token(&token, other, SECRET).is_err());
    }

    #[test]
    fn plugin_id_from_garbage_is_denied() {
        assert!(plugin_id_from_token("not-a-jwt").is_err());
    }

    #[test]
    fn token_header_roundtrips_through_properties() {
        let plugin = Uuid::new_v4();
        let token = sign_plugin_token(plugin, SECRET, now(), 3600).expect("sign");
        let props = token_properties(token.clone()).expect("properties");
        assert_eq!(read_token(&props).expect("read"), token);
    }

    #[test]
    fn missing_header_is_denied() {
        let props = BasicProperties::default();
        assert!(read_token(&props).is_err());
    }
}
