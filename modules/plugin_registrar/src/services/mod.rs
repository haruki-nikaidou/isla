//! Service-layer processors.
//!
//! Cross-entity orchestration: registering a plugin (atomically inserting
//! the plugin row plus its tools, skills, memory queries, namespaces, and
//! dependencies), de-registering, dependency-resolution checks, and routing
//! lookups used by the rest of the cluster.
//!
//! Registration, de-registration, namespace routing, and tool-catalog lookups
//! are exposed through [`registrar`].

pub mod registrar;
