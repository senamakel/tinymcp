//! The storage resources TinyMCP asks its host to provide.
//!
//! These declarations describe module needs; host policy decides what to
//! grant. Keeping collection schemas, indexes, streams, and secret selectors
//! here gives module loaders the same contract the implementation prepares
//! and uses. Secret environment values use a prefix, while the cache key is an
//! exact-name need and never broadens to a prefix.

use std::sync::LazyLock;

use tinystoragedrivers_bus::{
    CollectionSpec, IndexSpec, StorageNeeds, StorageNeedsWithExactSecrets,
};

/// Documents for installed MCP servers.
pub const SERVERS: &str = "mcp_servers";
/// Atomic claims on installed server names.
pub const SERVER_NAMES: &str = "mcp_server_names";
/// Cached upstream registry browse responses.
pub const REGISTRY_CACHE: &str = "mcp_registry_cache";
/// Cached tool declarations by server.
pub const TOOL_CACHE: &str = "mcp_tool_cache";
/// The local index of the official registry.
pub const REGISTRY_INDEX: &str = "mcp_registry_index";
/// The sync cursor for each registry index.
pub const REGISTRY_INDEX_STATE: &str = "mcp_registry_index_state";

/// The prefix of the append-only tool-write audit stream.
pub const AUDIT_STREAM: &str = "mcp_audit";
/// Prefix for each server's environment and OAuth bundle secret.
pub const ENV_SECRET_PREFIX: &str = "mcp_client_env/";
/// Exact secret name for the tool-cache fingerprint key.
pub const CACHE_KEY_SECRET: &str = "mcp_cache_fingerprint_key";

const STREAM_PREFIXES: &[&str] = &[AUDIT_STREAM];
const SECRET_PREFIXES: &[&str] = &[ENV_SECRET_PREFIX];
const EXACT_SECRET_NAMES: &[&str] = &[CACHE_KEY_SECRET];

static COLLECTIONS: LazyLock<Vec<CollectionSpec>> = LazyLock::new(|| {
    vec![
        collection(
            SERVERS,
            &[
                ("qualified_name", "qualified_name"),
                ("installed_at", "installed_at"),
            ],
        ),
        collection(SERVER_NAMES, &[]),
        collection(REGISTRY_CACHE, &[("cache_key", "cache_key")]),
        collection(TOOL_CACHE, &[]),
        collection(REGISTRY_INDEX, &[("source", "source")]),
        collection(REGISTRY_INDEX_STATE, &[]),
    ]
});

/// Returns TinyMCP's complete canonical storage declaration.
///
/// The returned borrowed references point to process-static metadata. It
/// requests its cache-key secret by exact name; only the environment bundle
/// is requested by prefix.
#[must_use]
pub fn storage_needs() -> StorageNeedsWithExactSecrets<'static> {
    StorageNeeds {
        collections: COLLECTIONS.as_slice(),
        blob_prefixes: &[],
        stream_prefixes: STREAM_PREFIXES,
        secret_prefixes: SECRET_PREFIXES,
    }
    .with_exact_secret_names(EXACT_SECRET_NAMES)
}

fn collection(name: &str, indexes: &[(&str, &str)]) -> CollectionSpec {
    CollectionSpec {
        name: name.to_owned(),
        indexes: indexes
            .iter()
            .map(|(name, field)| IndexSpec {
                name: (*name).to_owned(),
                fields: vec![(*field).to_owned()],
                unique: false,
            })
            .collect(),
        ttl_field: None,
        search: None,
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
