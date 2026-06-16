# Isla — Agent Guide

## Project overview

Isla is a self-hosted, self-owned, plugin-driven personal AI assistant, written
in Rust. It is built as a cluster of focused microservices that cooperate
through shared contracts, messaging patterns, and observability tooling, rather
than a single megabinary.

> **Status: pre-alpha.** APIs, module boundaries, and on-the-wire formats are
> unstable and will change without warning.

### Core principles

- **You own it** — runs on your own machine/VPS/SBC, your data stays in your
  database, no telemetry or cloud tenancy.
- **Serious engineering, not magic** — old-school microservices architecture,
  defensive design optimized for stability, Rust everywhere in the core with
  `#![forbid(unsafe_code)]` and
  `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]` in every
  crate.

### Transport summary

| Traffic                                          | Transport       | Payload               |
| ------------------------------------------------ | --------------- | --------------------- |
| Intra-cluster module-to-module                   | RabbitMQ (AMQP) | JSON                  |
| Channel adapter (e.g. `telegram_bot`) ↔ cluster  | RabbitMQ (AMQP) | JSON (cluster-signed) |
| `webui` ↔ `interface` module                     | gRPC            | Protobuf              |
| `dashboard` ↔ most modules                       | gRPC            | Protobuf              |
| Plugin ↔ cluster                                 | RabbitMQ (AMQP) | JSON                  |
| Persistent state                                 | PostgreSQL      | —                     |
| Caches / ephemeral state                         | Redis           | —                     |

gRPC is reserved for user-facing clients that cannot join the message bus
directly: the first-party `webui` reaches the cluster through the `interface`
module's gRPC API, and `dashboard` talks to most modules over gRPC.

Every other user-facing channel adapter (such as `user_interface/telegram_bot`)
is a trusted first-party cluster node, so it talks to the cluster directly over
RabbitMQ and signs each message as a cluster message — Ed25519 over the SHA-256
digest, carried in the `X-Cluster-Signature` header (see
`libs/dynamic_message_extension`, `ClusterMessage`). The `webui` is the
exception: as a browser client it cannot hold the cluster signing key, so it
talks to the `interface` module over gRPC instead.

Plugins also reach the cluster over RabbitMQ, but as untrusted senders
authenticated with per-plugin JWTs rather than the cluster key, so a plugin can
be written in any language and hosted anywhere the broker is reachable.

## Repository layout

```
binary/
  server/         # main service binary, multi-mode worker (modules grouped by subcommand)
  admin-tool/     # operator CLI, talks directly to Redis/RabbitMQ/Postgres
modules/
  auth/                # auth provider for admin and WebUI users
  administration/      # administration layer to manage the deployment
  shield/              # WAF-like protection (anti-XSS, CAPTCHA, ...)
  vault/               # E2E encrypted secret store (tokens, passwords, API keys)
  ai_caller/           # upstream LLM API calls + tool-use dispatch
  interface/           # unified gRPC abstraction over user-facing channels
  memory_repository/   # AI memory & conversation history
  plugin_registrar/    # plugin service discovery & registration
libs/
  wakuwaku/                    # shared infra: pools, AMQP/SQL/Redis glue, interval jobs, errors
  dynamic_message_extension/   # helpers for dynamic messages
user_interface/
  telegram_bot/   # first-party Telegram adapter
plugin/
  gmail/          # reference plugin (namespace: office.gmail)
webui/            # default end-user web chat UI (swappable, may be removed)
dashboard/        # default admin/ops dashboard (swappable, may be removed)
migrations/       # database migrations
```

## Conventions and standards

This repository ships a detailed [`GUIDELINES.md`](../GUIDELINES.md). Always
follow it. Key points:

- **Layering**: `entities` (all DB queries) → `services` (business logic) →
  `rpc` (gRPC endpoints that only call services, never the DB directly).
- **Naming**: `XxxEntity` (tables, in `entities`), `XxxService` (in `services`),
  `XxxGrpcService` (in `rpc`), `XxxRequest` (DTOs), `XxxHook`/`XxxExecutor`
  (hooks).
- **Processor pattern**: implement `kanau::processor::Processor<Input>` with
  native async fn in trait (no `#[async_trait]`); define `type Output` and
  `type Error`; annotate every `process` with
  `#[instrument(skip_all, name = "...", err)]`.
- **Errors**: bubble up with `?`; never `unwrap`/`expect`/`panic`.
- **Safety**: every module uses `#![forbid(unsafe_code)]`.
- **Docs**: use `///` to document how to use a component, not how it is
  implemented.
- **Raw SQL**: lives only in entity processors, never in services or hooks.
- **Observability**: use `tracing`.

## Building and testing

This is a Cargo workspace. Use standard commands:

- Build: `cargo build`
- Test: `cargo test`
- Lint: `cargo clippy` (lint configuration in `clippy.toml`)

Isolate pure algorithms from IO so they can be tested, and focus tests on logic
complex enough to be wrong.

**Tests may only exercise pure functions — never the database.** No live DB, no
`#[sqlx::test]`, no test containers. When a processor that touches
`DatabaseProcessor` holds non-trivial logic, extract that logic into a pure
function (inputs in, value out, no IO) and test that; leave the thin DB plumbing
untested. For example, the summary read-path saga keeps `select_context` and
`resolve_context` pure and tested, while `SummaryService::process` just wires the
database around them.
