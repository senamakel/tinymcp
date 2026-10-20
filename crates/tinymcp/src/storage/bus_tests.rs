//! The bus provider over a real in-process broker: one host connection serving
//! the storage objects, one module connection calling them.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use tinybus::{Connection, broker::Broker, transport::memory::MemoryBus};
use tinystoragedrivers_core::MemoryStorage;
use tinystoragedrivers_secrets::{MemorySecrets, SecretStore};

use super::test_host::{TestHost, serve_storage};
use super::*;
use crate::Store;

async fn host(backend: &MemoryStorage, scope: &str) -> (MemoryBus, TestHost) {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let host = serve_storage(&bus, backend, scope).await;
    (bus, host)
}

fn a_server(id: &str) -> tinymcp_bus::InstalledServer {
    tinymcp_bus::InstalledServer {
        server_id: id.to_string(),
        qualified_name: "@test/server".to_string(),
        display_name: "Test Server".to_string(),
        description: None,
        icon_url: None,
        command_kind: tinymcp_bus::CommandKind::Node,
        command: "npx".to_string(),
        args: vec!["-y".to_string()],
        env_keys: vec!["TOKEN".to_string()],
        config: None,
        installed_at: 1_700_000_000_000,
        last_connected_at: None,
        transport: tinymcp_bus::Transport::Stdio,
        enabled: true,
    }
}

/// A module connection. Secrets are held in `secrets`, shared across
/// connections the way the host's one store is.
async fn module(bus: &MemoryBus, secrets: &Arc<MemorySecrets>) -> BusProvider {
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    BusProvider::connect_with(connection, Some(secrets.clone() as Arc<dyn SecretStore>))
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn installed_servers_and_their_secrets_survive_reopening_over_the_bus() {
    let backend = MemoryStorage::new();
    let (bus, _host) = host(&backend, "module-tinymcp").await;
    let dir = std::env::temp_dir().join("tinymcp-bus-test");

    let secrets = Arc::new(MemorySecrets::new());
    let first = module(&bus, &secrets).await.ports_for(&dir).unwrap();
    first.prepare().unwrap();
    let store = Store::open(first).unwrap();
    assert!(!store.ports().has_state().unwrap());
    store.insert_server(&a_server("srv-1")).unwrap();
    store
        .set_env_values(
            "srv-1",
            &std::collections::BTreeMap::from([("TOKEN".to_owned(), "s3cret".to_owned())]),
        )
        .unwrap();

    // A second module connection (a restart) sees the same state.
    let second = Store::open(module(&bus, &secrets).await.ports_for(&dir).unwrap()).unwrap();
    assert_eq!(second.list_servers().unwrap().len(), 1);
    assert_eq!(
        second
            .load_env_values("srv-1")
            .unwrap()
            .get("TOKEN")
            .map(String::as_str),
        Some("s3cret")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn every_directory_answers_the_one_host_bound_scope() {
    let backend = MemoryStorage::new();
    let (bus, _host) = host(&backend, "module-tinymcp").await;
    let secrets = Arc::new(MemorySecrets::new());
    let provider = module(&bus, &secrets).await;
    let a = provider.ports_for(&std::env::temp_dir().join("a")).unwrap();
    a.prepare().unwrap();
    Store::open(a)
        .unwrap()
        .insert_server(&a_server("srv-1"))
        .unwrap();
    let b = provider.ports_for(&std::env::temp_dir().join("b")).unwrap();
    assert!(b.has_state().unwrap());
}

#[tokio::test(flavor = "multi_thread")]
async fn connecting_without_a_host_is_an_error_not_a_silent_memory_store() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let lonely = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    assert!(BusProvider::connect(lonely).await.is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_connect_routes_secrets_to_the_attested_host_store_only() {
    let backend = MemoryStorage::new();
    let (bus, _host) = host(&backend, "module-tinymcp").await;
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let provider = BusProvider::connect(connection).await.unwrap();
    let ports = provider.ports_for(&std::env::temp_dir().join("c")).unwrap();
    ports.prepare().unwrap();
    // An in-process broker cannot attest the caller, so the confidential
    // secret route is refused rather than silently served.
    let refused = Store::open(ports).unwrap_err();
    assert!(refused.to_string().contains("NotAttested"), "{refused}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_provider_over_ready_ports_hands_out_clones_of_them() {
    let backend = MemoryStorage::new();
    let (bus, _host) = host(&backend, "module-tinymcp").await;
    let secrets = Arc::new(MemorySecrets::new());
    let ports = module(&bus, &secrets)
        .await
        .ports_for(&std::env::temp_dir().join("d"))
        .unwrap();
    let provider = BusProvider::from_ports(ports);
    let again = provider.ports_for(&std::env::temp_dir().join("e")).unwrap();
    again.prepare().unwrap();
    assert!(!again.has_state().unwrap());
    assert!(format!("{provider:?}").contains("BusProvider"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bound_provider_rejects_a_descriptor_with_an_invalid_lease_root() {
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let connection = Connection::connect(bus.connect().await.unwrap())
        .await
        .unwrap();
    let descriptor = tinystoragedrivers_busproxy::StorageDescriptor {
        service_peer: ":1.42".to_owned(),
        registration_id: 1,
        object_root: "/org/tinystoragedrivers/Storage".to_owned(),
    };

    let error = BusProvider::connect_bound(connection, descriptor)
        .await
        .expect_err("a fixed storage root is not a lease route");
    assert!(error.to_string().contains("connecting to the host storage lease"));
}
