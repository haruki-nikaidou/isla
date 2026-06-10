# Isla Project Guidelines

## Naming Conventions

Strictly follow these naming rules to ensure consistency. You can assume all code follows these roles, so you don't need to guess where a type is defined.

### Entities
- **Location**: `modules/xxx/src/entities`
- **Tables**: All table representations must be named `XxxEntity`, except for many-to-many relationship tables.
- **Placement**: `XxxEntity` is only allowed to be in `entities`.

### Services
- **Location**: `modules/xxx/src/services`
- **Services**: All services must be named `XxxService`. 
- **Placement**: All `XxxService` are always in `services`, except for `XxxGrpcService`.
- **Requests**: All requests must be named `XxxRequest`.
- **Responses**: Responses do not need to be named `XxxResponse`.
- **Placement**: `XxxRequest` only appears in `services` and protobuf codegen.

### gRPC Services (RPC)
- **Location**: `modules/xxx/src/rpc`
- **Services**: All gRPC services must be named `XxxGrpcService`.
- **Placement**: All `XxxGrpcService` must be in `rpc`.
- **Conflict Handling**: In `rpc`, when the same `XxxRequest` name appears in both `services` and protobuf codegen, use an alias for the protobuf module:
  ```rust
  use proto::foo::bar::baz as pbaz;
  // Then use pbaz::XxxRequest
  ```

### Hooks
- **Location**: `modules/xxx/src/hooks`
- **Naming**: 
  - All hooks must be named `XxxHook`.
  - **Exception**: Schedule job hooks must be named `XxxExecutor`.

## Processor Pattern

As described in the `add-processor-function` skill, use the `Processor` pattern for async behavior across layers.

- **Trait**: Use `kanau::processor::Processor<Input>`.
- **Async Implementation**: Use native async fn in trait (RPITIT). Do **NOT** use `#[async_trait]`.
- **Associated Types**: Define `type Output` and `type Error`.
- **Instrumentation**: Every `process` function must have `#[instrument(skip_all, name = "...", err)]`.
  - **Entity Spans**: Named `SQL:XxxRequest` or `SQL-Transaction:XxxRequest`.
  - **Service/Hook Spans**: Named `XxxRequest` (the DTO name).

## Architecture & Layer Boundaries

Maintain clear boundaries between each layer:

- **Entities**: All database queries must be in `entities`.
- **Services**: Business logic and composition of entity calls.
- **RPC**: gRPC endpoints must simply call `services`. They are **not allowed** to perform any operations on the database directly.

## Coding Standards

### Documentation
- Use document comments (`///`) to document **how to use** a component.
- **Never** comment on **how it is implemented** unless the implementation restricts usage (e.g., "the implementation assumes a sorted array").

### Testing
- **Isolation**: Isolate pure algorithms from IO to make algorithms testable.
- **Focus**: Only test complex enough algorithms that may be wrong.

### Observability
- Use `tracing` for observability.

### Dynamic Messages
- For dynamic messages, use `libs/dynamic_message_extension`.

## Other Important Rules
- **Safety**: All modules should use `#![forbid(unsafe_code)]`.
- **Error Handling**: Use `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]`. Bubble up errors using the `?` operator.
- **DTOs**: Processor input DTOs should derive at minimum `Debug, Clone`.
- **Raw SQL**: Never put raw SQL in services or hooks; it belongs in entity processors.
- **Single Responsibility**: One DTO per operation. Avoid adding bespoke `pub async fn` methods to services.
