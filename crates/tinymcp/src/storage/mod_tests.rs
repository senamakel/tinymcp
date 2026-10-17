//! Tests for the storage ports, declared needs, and providers.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::ports::to_core_collection_spec;
use super::*;
use std::path::{Path, PathBuf};

/// An absolute directory path on every platform (`/data/a` is not absolute on
/// Windows); the memory provider never touches the disk.
fn dir(name: &str) -> PathBuf {
    std::env::temp_dir().join("tinymcp-test").join(name)
}
use tinystoragedrivers_core::{Filter, MemoryStorage};

use crate::{AuditStore, Store};
use tinymcp_bus::NewMcpWriteRecord;

fn a_write() -> NewMcpWriteRecord {
    NewMcpWriteRecord {
        timestamp_ms: 1,
        client_info: "client".into(),
        tool_name: "tool".into(),
        args_summary: serde_json::json!({}),
        resulting_chunk_id: None,
        success: true,
        error_message: None,
    }
}

#[test]
fn every_declared_collection_is_valid_and_none_needs_full_text_search() {
    let needs = storage_needs();
    for spec in needs.needs.collections {
        to_core_collection_spec(spec)
            .validate()
            .expect("a valid collection declaration");
    }
    let index = needs
        .needs
        .collections
        .into_iter()
        .find(|spec| spec.name == REGISTRY_INDEX)
        .expect("the index is declared");
    // Index search filters rows by substring, so the backend need not offer
    // full-text indexing.
    assert!(index.search.is_none());
}

#[test]
fn preparing_twice_is_harmless() {
    let ports = StoragePorts::memory().unwrap();
    ports.prepare().unwrap();
    ports.prepare().unwrap();
}

#[test]
fn a_fresh_backend_holds_no_state_until_something_is_written() {
    let ports = StoragePorts::memory().unwrap();
    assert!(!ports.has_state().unwrap());
    let audit = AuditStore::open(ports.clone()).unwrap();
    assert!(!ports.has_state().unwrap());
    audit.record(&a_write()).unwrap();
    assert!(ports.has_state().unwrap());
}

#[test]
fn installed_servers_count_as_stored_state() {
    let ports = StoragePorts::memory().unwrap();
    let store = Store::open(ports.clone()).unwrap();
    assert!(!ports.has_state().unwrap());
    let server = tinymcp_bus::InstalledServer {
        server_id: "one".into(),
        qualified_name: "@test/one".into(),
        display_name: "One".into(),
        description: None,
        icon_url: None,
        command_kind: tinymcp_bus::CommandKind::Node,
        command: "npx".into(),
        args: Vec::new(),
        env_keys: Vec::new(),
        config: None,
        installed_at: 1,
        last_connected_at: None,
        transport: tinymcp_bus::Transport::Stdio,
        enabled: true,
    };
    store.insert_server(&server).unwrap();
    assert!(ports.has_state().unwrap());
}

#[test]
fn the_memory_provider_answers_the_same_state_for_one_directory() {
    let provider = MemoryProvider::new();
    let first = provider.ports_for(&dir("a")).unwrap();
    AuditStore::open(first).unwrap().record(&a_write()).unwrap();
    let again = provider.ports_for(&dir("a")).unwrap();
    assert!(again.has_state().unwrap());
}

#[test]
fn the_memory_provider_keeps_directories_apart() {
    let provider = MemoryProvider::new();
    AuditStore::open(provider.ports_for(&dir("a")).unwrap())
        .unwrap()
        .record(&a_write())
        .unwrap();
    assert!(!provider.ports_for(&dir("b")).unwrap().has_state().unwrap());
}

#[test]
fn a_shared_backend_gives_each_scope_its_own_records() {
    let backend = MemoryStorage::new();
    let alice = StoragePorts::memory_scoped(&backend, "alice").unwrap();
    let bob = StoragePorts::memory_scoped(&backend, "bob").unwrap();
    AuditStore::open(alice.clone())
        .unwrap()
        .record(&a_write())
        .unwrap();
    assert!(alice.has_state().unwrap());
    assert!(!bob.has_state().unwrap());
}

#[test]
fn an_invalid_scope_is_refused() {
    let error = StoragePorts::memory_scoped(&MemoryStorage::new(), "").unwrap_err();
    assert!(matches!(error, crate::Error::Store { .. }));
}

#[test]
fn an_installed_provider_replaces_the_default() {
    // A private slot, so this test cannot reroute the services other tests
    // build through the process-wide one.
    let slot = super::provider::ProviderSlot::new();
    let first = slot.current();
    let replacement: std::sync::Arc<dyn StorageProvider> =
        std::sync::Arc::new(MemoryProvider::new());
    slot.install(std::sync::Arc::clone(&replacement));
    assert!(std::sync::Arc::ptr_eq(&slot.current(), &replacement));
    assert!(!std::sync::Arc::ptr_eq(&slot.current(), &first));
}

#[test]
fn the_process_wide_provider_answers_for_a_directory() {
    let ports = installed_provider()
        .ports_for(&dir("process-wide"))
        .unwrap();
    assert!(!ports.has_state().unwrap());
}

#[test]
fn the_counted_filter_default_selects_everything() {
    let ports = StoragePorts::memory().unwrap();
    ports.prepare().unwrap();
    let counted = ports
        .run("counting", |ports| async move {
            ports.documents.count(SERVERS, &Filter::default()).await
        })
        .unwrap();
    assert_eq!(counted, 0);
}

#[test]
fn the_memory_provider_refuses_a_relative_directory() {
    let provider = MemoryProvider::new();
    assert!(matches!(
        provider.ports_for(Path::new("relative/dir")),
        Err(crate::Error::InvalidArgument { .. })
    ));
    assert!(provider.ports_for(Path::new("")).is_err());
}

#[tokio::test]
async fn failing_documents_refuse_every_call_except_declaring_collections() {
    use super::failing::FailingDocuments;
    use tinystoragedrivers_core::{DocumentStore, Precondition, Query};

    let documents = FailingDocuments::default();
    assert!(
        !documents
            .capabilities()
            .contains(tinystoragedrivers_core::Capability::Transactions)
    );
    for spec in storage_needs().needs.collections {
        let spec = to_core_collection_spec(spec);
        documents.ensure_collection(&spec).await.unwrap();
    }
    let filter = Filter::eq("a", "b");
    assert!(documents.get("c", "1").await.is_err());
    assert!(
        documents
            .put("c", "1", serde_json::json!({}), Precondition::None)
            .await
            .is_err()
    );
    assert!(
        documents
            .delete("c", "1", Precondition::None)
            .await
            .is_err()
    );
    assert!(documents.query("c", &Query::default()).await.is_err());
    assert!(documents.count("c", &filter).await.is_err());
    assert!(documents.delete_where("c", &filter).await.is_err());
    assert!(
        documents
            .claim("c", &filter, &[], &serde_json::json!({}))
            .await
            .is_err()
    );
    assert!(documents.search("c", "text", 1).await.is_err());
    assert!(documents.drop_collection("c").await.is_err());
}

#[test]
fn a_failing_documents_port_is_an_error_not_an_empty_directory() {
    let ports = StoragePorts::with_failing_documents();
    assert!(matches!(ports.has_state(), Err(crate::Error::Store { .. })));
}

#[test]
fn an_undeclared_collection_counts_as_no_state() {
    let ports = StoragePorts::with_undeclared_collections();
    assert!(!ports.has_state().unwrap());
}

#[test]
fn directories_whose_names_differ_only_in_bytes_keep_separate_state() {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        let provider = MemoryProvider::new();
        let mut a = b"/data/".to_vec();
        a.push(0xff);
        let mut b = b"/data/".to_vec();
        b.push(0xfe);
        let a = PathBuf::from(std::ffi::OsString::from_vec(a));
        let b = PathBuf::from(std::ffi::OsString::from_vec(b));
        AuditStore::open(provider.ports_for(&a).unwrap())
            .unwrap()
            .record(&a_write())
            .unwrap();
        assert!(!provider.ports_for(&b).unwrap().has_state().unwrap());
    }
}
