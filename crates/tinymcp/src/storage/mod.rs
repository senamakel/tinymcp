//! Where the module keeps its state: the storage ports, never a database.
//!
//! Everything this crate persists goes through three ports from
//! `tinystoragedrivers`:
//!
//! | Port | Holds |
//! | --- | --- |
//! | `DocumentStore` | installed servers, the browse cache, the tool cache, the registry index and its sync state |
//! | `StreamStore` | the append-only audit log of tool writes |
//! | `SecretStore` | each server's environment values and OAuth bundle, and the key that keys the tool-cache fingerprints |
//!
//! The crate links no database client and picks no backend. A host hands over
//! [`StoragePorts`] (in process, the handles of its installed backend; across
//! the bus, the same port traits implemented by the storage bus client), or
//! installs a [`StorageProvider`] that does so per data directory. Without
//! one, a process-wide in-memory provider answers, which is the right shape
//! for tests and for a host that wants only its statically declared servers.
//!
//! # Declared needs
//!
//! The bus contract's [`storage_needs`] declaration lists the collections and
//! indexes, the audit stream, and the environment and cache-key secret
//! selectors. These are requests, not authority: the host applies its own
//! policy and binds its resulting grant to the caller. The implementation uses
//! the same collection schemas in [`StoragePorts::prepare`].
//!
//! # Scope
//!
//! The module never names a tenant. The scope belongs to the handles it is
//! given, so the same code serves a desktop user and one tenant of a cloud
//! host.

#[cfg(feature = "module")]
mod bus;
#[cfg(test)]
mod failing;
mod ports;
mod provider;

#[cfg(feature = "module")]
pub use bus::BusProvider;
pub use tinymcp_bus::storage::{
    AUDIT_STREAM, CACHE_KEY_SECRET, ENV_SECRET_PREFIX, REGISTRY_CACHE, REGISTRY_INDEX,
    REGISTRY_INDEX_STATE, SERVER_NAMES, SERVERS, TOOL_CACHE, storage_needs,
};
pub use ports::StoragePorts;
pub use provider::{
    MemoryProvider, StorageProvider, install_provider, installed_provider, provider_installed,
};

#[cfg(all(test, feature = "module"))]
pub(crate) mod test_host;

#[cfg(all(test, feature = "module"))]
#[path = "bus_tests.rs"]
mod bus_tests;

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
