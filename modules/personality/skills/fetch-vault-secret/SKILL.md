---
name: fetch-vault-secret
description: Load a module's persisted configuration or an upstream credential (e.g. an API key) from the vault. Use when a cluster module needs its typed config (defaulting when unset), needs to update that config, or must read a stored secret record and reason about which callers are allowed to access it.
---

# Fetch a Vault Secret or Config

The `vault` crate holds module configuration and encrypted secrets. Access is through **entity-layer `Processor` impls on `wakuwaku::sqlx::DatabaseProcessor`** (`Error = sqlx::Error`), invoked with `db.process(Dto).await?`. Secret **decryption happens inside the vault module**; callers never decrypt ciphertext themselves — they obtain plaintext only through vault's own access path.

## Module configuration

A module declares its config type by implementing `vault::module_config::ModuleConfig`:

```rust
pub trait ModuleConfig:
    serde::de::DeserializeOwned + serde::Serialize + Default
{
    const SCOPE: Scope;
    const CONFIG_NAME: &'static str;
}
```

Two generic processors operate on any `T: ModuleConfig`:

| DTO | Fields | Output |
|---|---|---|
| `FindConfig<T>` | _(unit; `FindConfig::<T>::new()` / `default()`)_ | `T` |
| `UpdateConfig<T>` | `new_value: T` | `Option<ModuleConfigEntity>` |

`FindConfig<T>` returns the typed config, falling back to `T::default()` when no row exists — so it never returns `None`. `UpdateConfig<T>` writes the new value and returns the updated row (`None` if no config row was present to update).

Example — `ai_caller` keeps its upstream-LLM settings as `OpenRouterConfig` under `AI_CALLER_SCOPE` with config_name `"ai_caller_config"`:

```rust
impl vault::module_config::ModuleConfig for OpenRouterConfig {
    const SCOPE: vault::scopes::Scope = vault::scopes::AI_CALLER_SCOPE;
    const CONFIG_NAME: &'static str = "ai_caller_config";
}

use vault::entities::db::config::{FindConfig, UpdateConfig};

// Read (defaults when unset):
let cfg: OpenRouterConfig = db.process(FindConfig::<OpenRouterConfig>::new()).await?;

// Persist a change:
db.process(UpdateConfig { new_value: cfg.clone() }).await?;
```

## Secrets

| DTO | Fields | Output |
|---|---|---|
| `FindSecretById` | `id: i64` | `Option<SecretEntity>` |

`SecretEntity` is the **encrypted** record, not plaintext:

| Field | Meaning |
|---|---|
| `id: i64` | bigserial primary key |
| `platform: String` / `name: String` | service the secret belongs to / human name |
| `allowed_scopes: Vec<String>` | scope patterns permitted to read this secret |
| `content: Vec<u8>` | ciphertext |
| `signature: Vec<u8>` | HMAC for integrity |
| `key: Uuid` | id of the rolling key used to encrypt it |
| `created_at` / `updated_at: i64`, `version: i32` | timestamps + optimistic-concurrency version |

```rust
use vault::entities::db::secret::FindSecretById;

let secret = db.process(FindSecretById { id }).await?
    .ok_or(wakuwaku::Error::NotFound)?;
// secret.content is ciphertext — decryption is performed inside the vault
// module's access path, gated by the scope model below.
```

## Scope model

Access is gated by hierarchical, dot-separated scopes. System scopes are prefixed `@isla` (e.g. `@isla.ai_caller`).

- `Scope(&'static [&'static str])` — a concrete permission, e.g. `AI_CALLER_SCOPE = Scope(&["@isla", "ai_caller"])`. `Display` renders it dotted.
- `ScopeRange` — a glob pattern matched with `ScopeRange::matches(scope) -> bool`:
  - literal segment → matches exactly
  - `*` → exactly one segment
  - `**` → zero or more segments
  - `+` → one or more segments
  - parsed from dotted strings via `FromStr` (e.g. `"@isla.**.read"`).

A secret's `allowed_scopes` are the patterns that gate which callers may read it: a caller's scope must match one of them before vault releases plaintext. `SYSTEM_SCOPE_RANGE` matches everything under `@isla`.

```rust
use std::str::FromStr;
use vault::scopes::{ScopeRange, AI_CALLER_SCOPE};

let pattern = ScopeRange::from_str("@isla.**")?;
assert!(pattern.matches(AI_CALLER_SCOPE)); // @isla.ai_caller is in range
```

## Definition of done

- [ ] Config read via `FindConfig<T>` (relying on the `T::default()` fallback) and written via `UpdateConfig<T>`.
- [ ] Secrets fetched with `FindSecretById`; `None` handled as not-found.
- [ ] Ciphertext (`content`) never decrypted by the caller; scope gating reasoned about via `ScopeRange::matches`.
- [ ] `?`-propagated errors; no `unwrap`/`expect`/`panic`.
