//! Redis-backed entities for the plugin registrar.
//!
//! These records track *live* state — currently-online plugins, their last
//! heartbeat, and any in-flight RPC correlation needed to route a reply back
//! to its caller. Everything here is intentionally ephemeral: on Redis loss
//! the registrar rebuilds the live set from the next heartbeat round.
//!
//! Currently empty — populated as the heartbeat / routing implementation
//! lands.
