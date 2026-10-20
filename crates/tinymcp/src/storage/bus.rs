//! Storage over the host's `ai.tinyhumans.tinystorage.Storage` bus objects.
//!
//! A loaded module cannot reach the host's storage handles, so it asks the
//! host for them over its own `TinyBus` connection. The host binds the caller
//! to one scope it chooses from the broker-authenticated sender; the module
//! never names a tenant, a collection outside its storage needs or a
//! backend. What comes back implements the same port traits as an in-process
//! backend, so the stores above this file are identical in both cases.

use std::path::Path;
use std::sync::Arc;

use tinybus::Connection;
use tinystoragedrivers_busproxy::{
    BusTransport, StorageDescriptor, connect_storage, module_scope_label,
};
use tinystoragedrivers_client::BusStorage;
use tinystoragedrivers_core::StorageBackend;
use tinystoragedrivers_secrets::SecretStore;

use super::{StoragePorts, StorageProvider};
use crate::error::{Error, Result};

/// A provider whose ports are the host's, reached over the bus.
///
/// One bus connection serves one scope, so every data directory answers the
/// same ports: the host, not the path, decides whose state this is.
#[derive(Debug, Clone)]
pub struct BusProvider {
    ports: StoragePorts,
}

impl BusProvider {
    /// Connects to the host's storage objects and binds the granted scope.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Store`] when the host serves no storage, refuses the
    /// capability calls, or the bus fails.
    pub async fn connect(connection: Connection) -> Result<Self> {
        Self::connect_with(connection, None).await
    }

    /// Connects to the host-issued current-only storage lease on this module's
    /// existing `TinyBus` connection.
    ///
    /// The descriptor selects a unique service peer and route. The TSD host
    /// still authenticates this module peer and checks that the lease is live
    /// for every request.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Store`] when the descriptor is invalid or revoked,
    /// the host refuses the module, or the bus fails.
    pub async fn connect_bound(
        connection: Connection,
        descriptor: StorageDescriptor,
    ) -> Result<Self> {
        let scope = module_scope_label();
        let transport = BusTransport::connect_bound(connection, descriptor)
            .await
            .map_err(|source| {
                Error::store(
                    "connecting to the host storage lease",
                    tinystoragedrivers_core::StorageError::unavailable(source.to_string()),
                )
            })?;
        let storage = BusStorage::connect(Arc::new(transport), scope.clone())
            .await
            .map_err(|source| Error::store("opening the host-bound scope", source))?;
        let scoped = storage
            .for_scope(&scope)
            .map_err(|source| Error::store("opening the host-bound scope", source))?;
        let secrets = storage.secrets();
        Ok(Self {
            ports: StoragePorts::from_scoped(&scoped, secrets),
        })
    }

    /// [`connect`](Self::connect), with `secrets` standing in for the host's
    /// secret store. Secrets travel only on the confidential route, which a
    /// broker grants to attested artifacts alone, so a test over an in-process
    /// broker supplies its own.
    pub(crate) async fn connect_with(
        connection: Connection,
        secrets: Option<Arc<dyn SecretStore>>,
    ) -> Result<Self> {
        let storage = connect_storage(connection)
            .await
            .map_err(|source| Error::store("connecting to the host storage", source))?;
        let scoped = storage
            .for_scope(&module_scope_label())
            .map_err(|source| Error::store("opening the host-bound scope", source))?;
        let secrets = secrets.unwrap_or_else(|| storage.secrets());
        Ok(Self {
            ports: StoragePorts::from_scoped(&scoped, secrets),
        })
    }

    /// A provider over already-connected `ports`.
    #[must_use]
    pub fn from_ports(ports: StoragePorts) -> Self {
        Self { ports }
    }
}

impl StorageProvider for BusProvider {
    fn ports_for(&self, _data_dir: &Path) -> Result<StoragePorts> {
        Ok(self.ports.clone())
    }
}
