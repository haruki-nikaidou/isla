# Isla

> **Status: pre-alpha.** Almost nothing is implemented yet. APIs, module
> boundaries, and the on-the-wire formats described below will change without
> warning. Do not run this against anything you care about.

Isla is a self-hosted, self-owned, plugin-driven personal AI assistant.

The name *Isla* comes from the anime *Plastic Memories*. It is a reminder to
never forget the limitations of AI, to never think AI is the same as a human,
and that AI can behave crazily. And, because she is a cute girl.

## Philosophy

Isla is built on two opinions.

### 1. You own it

The binary runs on your machine, your VPS, or your SBC. Your conversation
history lives in your database. No telemetry, no cloud tenancy, no license
server.

The AI agents follow your instructions and *you* are the one responsible for
them. You can let her do anything you want; the choice is yours. WebUI,
dashboard, base instructions, and most of the rest of the functionality are
pluggable. Don't like the default? Bring your own. By the way: if you give her
full control of your computer and she nukes your computer, that is on you.

### 2. Demythologization and serious engineering

AI is not magic. An AI agent is nothing more than an application, and Isla is
built as an application — not as a magical artifact.

Concretely, this means:

- An "old-school" microservices architecture, not a single megabinary that
  pretends to be a brain.
- Very defensive design, optimized for stability over cleverness.
- Rust everywhere in the core, with `#![forbid(unsafe_code)]` and
  `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]` in every
  crate.

## Architecture

Isla is a cluster of focused microservices that cooperate through a shared set
of contracts, messaging patterns, and observability tooling.

### Goals

- **Clear boundaries** — Services expose well-defined APIs (gRPC, REST, AMQP
  events) and depend on shared libraries for cross-cutting concerns, so that
  business logic stays isolated inside its module.
- **Operational resiliency** — Stateless services, the Saga pattern, database
  connection pooling, and message queues allow resilient deployments with
  graceful failure handling.
- **Security by design** — Rust, strict processor patterns, and zero shared
  mutable state within processes prevent memory-safety issues and accidental
  privilege escalations.
- **Flexible for the user** — Service discovery for plugins, communicating over
  JSON, so that you can install a plugin written in any language and run it
  anywhere reachable by the main cluster.

### Transport summary

| Traffic                                          | Transport            | Payload               |
| ------------------------------------------------ | -------------------- | --------------------- |
| Intra-cluster module-to-module                   | RabbitMQ             | JSON                  |
| Channel adapter (e.g. `telegram_bot`) ↔ cluster  | RabbitMQ (AMQP)      | JSON (cluster-signed) |
| `webui` ↔ `interface` module                     | gRPC                 | Protobuf              |
| `dashboard` ↔ most modules                       | gRPC                 | Protobuf              |
| Plugin ↔ cluster                                 | RabbitMQ (AMQP)      | JSON                  |
| Persistent state                                 | PostgreSQL           | —                     |
| Caches / ephemeral state                         | Redis                | —                     |

The gRPC stack is reserved for user-facing clients that cannot join the message
bus directly: the first-party `webui` reaches the cluster through the
`interface` module's gRPC API, and `dashboard` talks to most modules over gRPC.

Every other user-facing channel adapter (such as `user_interface/telegram_bot`)
is a trusted first-party cluster node, so it talks to the cluster directly over
RabbitMQ and signs each message as a cluster message (Ed25519 over the SHA-256
digest, carried in the `X-Cluster-Signature` header; see
`libs/dynamic_message_extension`). The `webui` is the exception — as a browser
client it cannot hold the cluster signing key, so it talks to the `interface`
module over gRPC instead.

Plugins also reach the cluster over RabbitMQ, but as untrusted senders
authenticated with per-plugin JWTs rather than the cluster key, so that a plugin
can be implemented in any programming language and hosted anywhere the broker is
reachable.

### Service topology

Core modules (all live under `modules/`):

| Module              | Responsibility                                                                                                                |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `auth`              | Auth provider for admin and WebUI users.                                                                                      |
| `administration`    | Administration layer used by other modules, allowing the user to manage the deployment.                                       |
| `shield`            | WAF-like module. Provides anti-XSS, CAPTCHA, and other tools to prevent unauthorized access.                                  |
| `vault`             | End-to-end encrypted vault. Stores tokens and passwords used by the AI, and the API tokens used to call upstream AI APIs.     |
| `ai_caller`         | Handles upstream LLM API calls and dispatches tool-use requests.                                                              |
| `interface`         | Unified gRPC abstraction over user-facing channels (WebUI, Discord, Telegram, Slack, …). The "send a message" tools live here. |
| `memory_repository` | AI memory and conversation history.                                                                                           |
| `plugin_registrar`  | Service discovery and registration for plugins.                                                                               |

Binaries (under `binary/`):

| Binary       | Role                                                                                                                                                      |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `server`     | The service. Has multiple worker modes (selected by subcommand) that group the modules above; intended to be run multiple times with different roles.     |
| `admin-tool` | CLI for the deployment owner. Talks directly to Redis, RabbitMQ, and PostgreSQL — bypassing the running services — so it stays useful when things break. |

User-facing reference frontends (shipped as defaults, can be swapped or
removed entirely):

| Path                          | Role                                                                                                                  |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `webui/`                      | Default end-user chat UI. Talks to the `interface` module over gRPC.                                                  |
| `dashboard/`                  | Default admin/ops UI. Talks to most modules over gRPC (this is why every module exposes an `rpc` submodule).          |
| `user_interface/telegram_bot` | First-party Telegram adapter. As a trusted cluster node it talks to the cluster over RabbitMQ, signing each message as a cluster message. Other chat platforms (Discord, Slack, …) are added the same way. |

These frontends are *not* plugins — they don't provide skills or tools.
"Send a message" tooling is part of the `interface` module itself.

Shared libraries (under `libs/`):

| Library    | Role                                                                                                            |
| ---------- | --------------------------------------------------------------------------------------------------------------- |
| `wakuwaku` | Internal infrastructure crate: connection pooling, AMQP/SQL/Redis glue, interval jobs, shared error types.     |

### Plugins

Plugins extend Isla without being part of the core cluster. They communicate
with the cluster as JSON messages over AMQP and are discovered through
`plugin_registrar`.

Each plugin has:

- **Namespace** *(required)* — a non-empty string like `office.gmail` or
  `life.accuweather`. Used for tool-use addressing.
- **Metadata** *(required)*.
- **Structured memory index** *(optional)*.
- **Tools** *(optional)*.
- **Skill files** *(optional)*.
- **Dependency data** *(optional)* — a plugin may call another plugin.

Each plugin must handle these events:

- Tool-use calls, as declared in its tools.
- Service-discovery signals.
- Memory-query calls, as declared.

Each plugin can emit these events:

- Service-discovery responses.
- Memory-query calls, as declared in its dependencies.
- Tool-use callbacks.
- **Wake-up signals** — when the user asks Isla to do *A when B*, after *B*
  occurs the plugin sends a wake-up signal carrying a pointer to the relevant
  conversation.

A reference plugin lives at `plugin/gmail`.

## Repository layout

```
binary/
  server/         # main service binary, multi-mode worker
  admin-tool/     # operator CLI, talks directly to Redis/RabbitMQ/Postgres
modules/
  auth/                # auth provider
  administration/      # administration layer
  shield/              # WAF-like protection
  vault/               # E2E encrypted secret store
  ai_caller/           # upstream LLM API + tool-use dispatch
  interface/           # unified gRPC bot/channel abstraction
  memory_repository/   # AI memory & conversation history
  plugin_registrar/    # plugin service discovery
libs/
  wakuwaku/       # shared infra: pools, AMQP, SQL, Redis, interval jobs
user_interface/
  telegram_bot/   # first-party Telegram adapter
plugin/
  gmail/          # reference plugin (namespace: office.gmail)
webui/            # default end-user web chat UI (swappable, may be removed)
dashboard/        # default admin/ops dashboard (swappable, may be removed)
```

## License

GPL-3.0. See [`LICENSE`](./LICENSE).
