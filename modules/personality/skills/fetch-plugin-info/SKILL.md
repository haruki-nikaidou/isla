---
name: fetch-plugin-info
description: Discover which plugin handles a namespace, list a plugin's advertised tools, or look up a registered plugin and how to address it over AMQP. Use when the agent or a cluster module needs to resolve a dotted namespace (e.g. "office.gmail") to its plugin, enumerate the tool catalog the LLM may call, or find the exchange / routing key to dispatch a tool-use call.
---

# Fetch Plugin Info

The `plugin_registrar` crate is the cluster's plugin directory. Discovery goes through the **service-layer `Processor` impls** on `PluginRegistrarService` (`Error = wakuwaku::Error`), invoked in-process with `service.process(Dto).await?`. Direct primary-key/name lookups live one layer down as entity processors on `DatabaseProcessor`.

## Discovery API — `PluginRegistrarService`

| DTO | Fields | Output |
|---|---|---|
| `FindPluginByNamespaceRequest` | `namespace: String` | `Option<PluginEntity>` |
| `ListPluginsRequest` | `limit: i64`, `offset: i64` | `Vec<PluginEntity>` |
| `ListToolCatalogRequest` | _(unit)_ | `Vec<PluginToolEntity>` |

`namespace` is the dotted identifier a capability is published under, e.g. `office.gmail` or `life.accuweather`. In the database namespaces are stored as the segment array (`{"office","gmail"}`) so the registrar can match by prefix; you pass the dotted string. `ListToolCatalogRequest` returns every advertised tool across all plugins — the catalog the LLM is shown.

## Entities

`PluginEntity` — a registered plugin:

| Field | Meaning |
|---|---|
| `id: Uuid` | cluster-issued identity; AMQP correlation principal and FK target |
| `name: String` | stable machine name (e.g. `gmail`), unique cluster-wide |
| `scope_range: Vec<String>` | authorization scopes the plugin may act under |
| `description: String` | human-readable description for operator surfaces |
| `message_exchange: String` | **the AMQP exchange the cluster publishes to** to address this plugin |

`PluginToolEntity` — one tool a plugin advertises:

| Field | Meaning |
|---|---|
| `id: i64` / `plugin_id: Uuid` | row id / owning plugin |
| `name: String` | tool name as the LLM sees it; unique per plugin |
| `display_name: String` | human-readable name for operator surfaces |
| `description: String` | description shown to the LLM |
| `parameters_schema: serde_json::Value` | JSON Schema for the tool's input parameters |
| `routing_key: String` | **AMQP routing key** the cluster uses to dispatch a tool-use call |

## Dispatching a tool-use call

A tool call is published over AMQP to the owning plugin's `message_exchange`, using the tool's `routing_key`. So the typical flow is: resolve the namespace → plugin (`FindPluginByNamespaceRequest`) to get `message_exchange`, find the chosen tool's `routing_key` in the catalog, then publish the call onto `(message_exchange, routing_key)`.

```rust
use plugin_registrar::services::{
    PluginRegistrarService, FindPluginByNamespaceRequest, ListToolCatalogRequest,
};

let plugin = registrar
    .process(FindPluginByNamespaceRequest { namespace: "office.gmail".to_string() })
    .await?;
let Some(plugin) = plugin else { return Err(wakuwaku::Error::NotFound) };

let tools = registrar.process(ListToolCatalogRequest).await?;
let tool = tools
    .iter()
    .find(|t| t.plugin_id == plugin.id && t.name == "send_email")
    .ok_or(wakuwaku::Error::NotFound)?;

// dispatch the tool-use call over AMQP:
//   exchange = plugin.message_exchange, routing_key = tool.routing_key
```

## Direct lookups (entity layer)

For a known id or machine name, the entity-layer finders on `DatabaseProcessor` skip the service hop:

- `FindPluginById { id: Uuid }` -> `Option<PluginEntity>`
- `FindPluginByName { name: String }` -> `Option<PluginEntity>`

Use these only for direct identity resolution; namespace routing and the tool catalog go through the service.

## Definition of done

- [ ] Namespace → plugin resolved via `FindPluginByNamespaceRequest`; `None` handled as not-found.
- [ ] Tool chosen from `ListToolCatalogRequest`; `parameters_schema` honored when building arguments.
- [ ] Dispatch addressed to `plugin.message_exchange` with the tool's `routing_key`.
- [ ] `?`-propagated `wakuwaku::Error`; no `unwrap`/`expect`/`panic`.
